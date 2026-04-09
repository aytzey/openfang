//! Config hot-reload planning for the sales-only daemon.

use pulsivo_salesman_types::config::{KernelConfig, ReloadMode};
use tracing::{info, warn};

/// Runtime-safe actions that can be applied without a full restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HotAction {
    ReloadChannels,
    ReloadWebConfig,
}

/// Diff result for applying configuration changes.
#[derive(Debug, Clone)]
pub struct ReloadPlan {
    pub restart_required: bool,
    pub restart_reasons: Vec<String>,
    pub hot_actions: Vec<HotAction>,
    pub noop_changes: Vec<String>,
}

impl ReloadPlan {
    pub fn has_changes(&self) -> bool {
        self.restart_required || !self.hot_actions.is_empty() || !self.noop_changes.is_empty()
    }

    pub fn is_hot_reloadable(&self) -> bool {
        !self.restart_required
    }

    pub fn log_summary(&self) {
        if !self.has_changes() {
            info!("config reload: no changes detected");
            return;
        }
        if self.restart_required {
            warn!(
                "config reload: restart required — {}",
                self.restart_reasons.join("; ")
            );
        }
        for action in &self.hot_actions {
            info!("config reload: hot-reload action queued — {action:?}");
        }
        for noop in &self.noop_changes {
            info!("config reload: no-op change — {noop}");
        }
    }
}

fn field_changed<T: serde::Serialize>(old: &T, new: &T) -> bool {
    let old_json = serde_json::to_string(old).ok();
    let new_json = serde_json::to_string(new).ok();
    old_json != new_json
}

/// Compare two configs and categorize the delta.
pub fn build_reload_plan(old: &KernelConfig, new: &KernelConfig) -> ReloadPlan {
    let mut plan = ReloadPlan {
        restart_required: false,
        restart_reasons: Vec::new(),
        hot_actions: Vec::new(),
        noop_changes: Vec::new(),
    };

    if old.api_listen != new.api_listen {
        plan.restart_required = true;
        plan.restart_reasons.push(format!(
            "api_listen changed: {} -> {}",
            old.api_listen, new.api_listen
        ));
    }

    if old.api_key != new.api_key {
        plan.restart_required = true;
        plan.restart_reasons.push("api_key changed".to_string());
    }

    if field_changed(&old.memory, &new.memory) {
        plan.restart_required = true;
        plan.restart_reasons
            .push("memory config changed".to_string());
    }

    if field_changed(&old.default_model, &new.default_model) {
        plan.restart_required = true;
        plan.restart_reasons
            .push("default_model changed".to_string());
    }

    if old.home_dir != new.home_dir {
        plan.restart_required = true;
        plan.restart_reasons.push(format!(
            "home_dir changed: {:?} -> {:?}",
            old.home_dir, new.home_dir
        ));
    }

    if old.data_dir != new.data_dir {
        plan.restart_required = true;
        plan.restart_reasons.push(format!(
            "data_dir changed: {:?} -> {:?}",
            old.data_dir, new.data_dir
        ));
    }

    if field_changed(&old.channels, &new.channels) {
        plan.hot_actions.push(HotAction::ReloadChannels);
    }

    if field_changed(&old.web, &new.web) {
        plan.hot_actions.push(HotAction::ReloadWebConfig);
    }

    if old.log_level != new.log_level {
        plan.noop_changes
            .push(format!("log_level: {} -> {}", old.log_level, new.log_level));
    }

    if old.language != new.language {
        plan.noop_changes
            .push(format!("language: {} -> {}", old.language, new.language));
    }

    if old.mode != new.mode {
        plan.noop_changes
            .push(format!("mode: {:?} -> {:?}", old.mode, new.mode));
    }

    plan
}

/// Validate a new config before hot-swapping it in place.
pub fn validate_config_for_reload(config: &KernelConfig) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();

    if config.api_listen.trim().is_empty() {
        errors.push("api_listen cannot be empty".to_string());
    }

    if let Some(email) = &config.channels.email {
        if email.smtp_host.trim().is_empty() {
            errors.push("email.smtp_host cannot be empty when email is configured".to_string());
        }
        if email.username.trim().is_empty() {
            errors.push("email.username cannot be empty when email is configured".to_string());
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Decide whether hot actions should be applied for the configured reload mode.
pub fn should_apply_hot(mode: ReloadMode, plan: &ReloadPlan) -> bool {
    match mode {
        ReloadMode::Off | ReloadMode::Restart => false,
        ReloadMode::Hot | ReloadMode::Hybrid => !plan.hot_actions.is_empty(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsivo_salesman_types::config::{EmailConfig, KernelConfig, KernelMode, ReloadMode};

    fn default_cfg() -> KernelConfig {
        KernelConfig::default()
    }

    #[test]
    fn test_no_changes_detected() {
        let a = default_cfg();
        let b = default_cfg();
        let plan = build_reload_plan(&a, &b);
        assert!(!plan.has_changes());
        assert!(!plan.restart_required);
        assert!(plan.hot_actions.is_empty());
        assert!(plan.noop_changes.is_empty());
    }

    #[test]
    fn test_api_listen_requires_restart() {
        let a = default_cfg();
        let mut b = default_cfg();
        b.api_listen = "0.0.0.0:8080".to_string();
        let plan = build_reload_plan(&a, &b);
        assert!(plan.restart_required);
        assert!(plan.restart_reasons.iter().any(|r| r.contains("api_listen")));
    }

    #[test]
    fn test_api_key_requires_restart() {
        let a = default_cfg();
        let mut b = default_cfg();
        b.api_key = "super-secret-key".to_string();
        let plan = build_reload_plan(&a, &b);
        assert!(plan.restart_required);
        assert!(plan.restart_reasons.iter().any(|r| r.contains("api_key")));
    }

    #[test]
    fn test_memory_config_requires_restart() {
        let a = default_cfg();
        let mut b = default_cfg();
        b.memory.consolidation_threshold = 99_999;
        let plan = build_reload_plan(&a, &b);
        assert!(plan.restart_required);
        assert!(plan
            .restart_reasons
            .iter()
            .any(|r| r.contains("memory config")));
    }

    #[test]
    fn test_default_model_requires_restart() {
        let a = default_cfg();
        let mut b = default_cfg();
        b.default_model.model = "gpt-4".to_string();
        let plan = build_reload_plan(&a, &b);
        assert!(plan.restart_required);
        assert!(plan
            .restart_reasons
            .iter()
            .any(|r| r.contains("default_model")));
    }

    #[test]
    fn test_email_channel_hot_reload() {
        let a = default_cfg();
        let mut b = default_cfg();
        b.channels.email = Some(EmailConfig {
            smtp_host: "smtp.example.com".to_string(),
            smtp_port: 587,
            username: "ops@example.com".to_string(),
            password_env: "EMAIL_PASSWORD".to_string(),
        });
        let plan = build_reload_plan(&a, &b);
        assert!(!plan.restart_required);
        assert!(plan.hot_actions.contains(&HotAction::ReloadChannels));
    }

    #[test]
    fn test_web_hot_reload() {
        let a = default_cfg();
        let mut b = default_cfg();
        b.web.fetch.timeout_secs = 10;
        let plan = build_reload_plan(&a, &b);
        assert!(!plan.restart_required);
        assert!(plan.hot_actions.contains(&HotAction::ReloadWebConfig));
    }

    #[test]
    fn test_mixed_changes() {
        let a = default_cfg();
        let mut b = default_cfg();
        b.api_listen = "0.0.0.0:9999".to_string();
        b.web.fetch.timeout_secs = 10;
        b.log_level = "debug".to_string();

        let plan = build_reload_plan(&a, &b);
        assert!(plan.restart_required);
        assert!(plan.hot_actions.contains(&HotAction::ReloadWebConfig));
        assert!(plan.noop_changes.iter().any(|c| c.contains("log_level")));
    }

    #[test]
    fn test_noop_changes() {
        let a = default_cfg();
        let mut b = default_cfg();
        b.log_level = "debug".to_string();
        b.language = "tr".to_string();
        b.mode = KernelMode::Dev;

        let plan = build_reload_plan(&a, &b);
        assert!(!plan.restart_required);
        assert!(plan.hot_actions.is_empty());
        assert_eq!(plan.noop_changes.len(), 3);
    }

    #[test]
    fn test_validate_config_for_reload_valid() {
        let config = default_cfg();
        assert!(validate_config_for_reload(&config).is_ok());
    }

    #[test]
    fn test_validate_config_for_reload_invalid() {
        let mut config = default_cfg();
        config.api_listen = String::new();
        let err = validate_config_for_reload(&config).unwrap_err();
        assert!(err.iter().any(|e| e.contains("api_listen")));

        let mut config = default_cfg();
        config.channels.email = Some(EmailConfig::default());
        let err = validate_config_for_reload(&config).unwrap_err();
        assert!(err.iter().any(|e| e.contains("smtp_host")));
        assert!(err.iter().any(|e| e.contains("username")));
    }

    #[test]
    fn test_should_apply_hot() {
        let plan = ReloadPlan {
            restart_required: false,
            restart_reasons: vec![],
            hot_actions: vec![HotAction::ReloadChannels],
            noop_changes: vec![],
        };
        assert!(!should_apply_hot(ReloadMode::Off, &plan));
        assert!(!should_apply_hot(ReloadMode::Restart, &plan));
        assert!(should_apply_hot(ReloadMode::Hot, &plan));
        assert!(should_apply_hot(ReloadMode::Hybrid, &plan));
    }
}
