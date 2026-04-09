//! Minimal configuration types for the sales-only PulsivoSalesman daemon.

use crate::agent::ReasoningEffort;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Kernel operating mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KernelMode {
    Stable,
    #[default]
    Default,
    Dev,
}

/// Web search provider selection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchProvider {
    Brave,
    Tavily,
    Perplexity,
    DuckDuckGo,
    #[default]
    Auto,
}

/// Web tools configuration (search + fetch).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WebConfig {
    /// Which search provider to use.
    pub search_provider: SearchProvider,
    /// Cache TTL in minutes (0 = disabled).
    pub cache_ttl_minutes: u64,
    /// Brave Search configuration.
    pub brave: BraveSearchConfig,
    /// Tavily Search configuration.
    pub tavily: TavilySearchConfig,
    /// Perplexity Search configuration.
    pub perplexity: PerplexitySearchConfig,
    /// Web fetch configuration.
    pub fetch: WebFetchConfig,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            search_provider: SearchProvider::default(),
            cache_ttl_minutes: 15,
            brave: BraveSearchConfig::default(),
            tavily: TavilySearchConfig::default(),
            perplexity: PerplexitySearchConfig::default(),
            fetch: WebFetchConfig::default(),
        }
    }
}

/// Brave Search API configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BraveSearchConfig {
    pub api_key_env: String,
    pub max_results: usize,
    pub country: String,
    pub search_lang: String,
    pub freshness: String,
}

impl Default for BraveSearchConfig {
    fn default() -> Self {
        Self {
            api_key_env: "BRAVE_API_KEY".to_string(),
            max_results: 5,
            country: String::new(),
            search_lang: String::new(),
            freshness: String::new(),
        }
    }
}

/// Tavily Search API configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TavilySearchConfig {
    pub api_key_env: String,
    pub search_depth: String,
    pub max_results: usize,
    pub include_answer: bool,
}

impl Default for TavilySearchConfig {
    fn default() -> Self {
        Self {
            api_key_env: "TAVILY_API_KEY".to_string(),
            search_depth: "basic".to_string(),
            max_results: 5,
            include_answer: true,
        }
    }
}

/// Perplexity Search API configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PerplexitySearchConfig {
    pub api_key_env: String,
    pub model: String,
}

impl Default for PerplexitySearchConfig {
    fn default() -> Self {
        Self {
            api_key_env: "PERPLEXITY_API_KEY".to_string(),
            model: "sonar".to_string(),
        }
    }
}

/// Web fetch configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WebFetchConfig {
    pub max_chars: usize,
    pub max_response_bytes: usize,
    pub timeout_secs: u64,
    pub readability: bool,
}

impl Default for WebFetchConfig {
    fn default() -> Self {
        Self {
            max_chars: 50_000,
            max_response_bytes: 10 * 1024 * 1024,
            timeout_secs: 30,
            readability: true,
        }
    }
}

/// Config hot-reload mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReloadMode {
    Off,
    Restart,
    Hot,
    #[default]
    Hybrid,
}

/// Configuration for config file watching and hot-reload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReloadConfig {
    pub mode: ReloadMode,
    pub debounce_ms: u64,
}

impl Default for ReloadConfig {
    fn default() -> Self {
        Self {
            mode: ReloadMode::default(),
            debounce_ms: 500,
        }
    }
}

/// Extended thinking configuration for models that support it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ThinkingConfig {
    pub budget_tokens: u32,
    pub stream_thinking: bool,
}

impl Default for ThinkingConfig {
    fn default() -> Self {
        Self {
            budget_tokens: 10_000,
            stream_thinking: false,
        }
    }
}

/// Default LLM model configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DefaultModelConfig {
    pub provider: String,
    pub model: String,
    pub api_key_env: String,
    pub base_url: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<ReasoningEffort>,
}

impl Default for DefaultModelConfig {
    fn default() -> Self {
        Self {
            provider: "openai-codex".to_string(),
            model: "gpt-5.3-codex".to_string(),
            api_key_env: "OPENAI_CODEX_ACCESS_TOKEN".to_string(),
            base_url: None,
            reasoning_effort: Some(ReasoningEffort::High),
        }
    }
}

/// Memory substrate configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MemoryConfig {
    pub sqlite_path: Option<PathBuf>,
    pub embedding_model: String,
    pub consolidation_threshold: u64,
    pub decay_rate: f32,
    #[serde(default)]
    pub embedding_provider: Option<String>,
    #[serde(default)]
    pub embedding_api_key_env: Option<String>,
    #[serde(default = "default_consolidation_interval")]
    pub consolidation_interval_hours: u64,
}

fn default_consolidation_interval() -> u64 {
    24
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            sqlite_path: None,
            embedding_model: "all-MiniLM-L6-v2".to_string(),
            consolidation_threshold: 10_000,
            decay_rate: 0.1,
            embedding_provider: None,
            embedding_api_key_env: None,
            consolidation_interval_hours: default_consolidation_interval(),
        }
    }
}

/// Outbound email channel configuration used by the sales engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EmailConfig {
    pub smtp_host: String,
    pub smtp_port: u16,
    pub username: String,
    pub password_env: String,
}

impl Default for EmailConfig {
    fn default() -> Self {
        Self {
            smtp_host: String::new(),
            smtp_port: 587,
            username: String::new(),
            password_env: "EMAIL_PASSWORD".to_string(),
        }
    }
}

/// Sales daemon channel configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ChannelsConfig {
    pub email: Option<EmailConfig>,
}

/// Top-level kernel configuration for the sales daemon.
#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KernelConfig {
    /// PulsivoSalesman home directory (default: ~/.pulsivo-salesman).
    pub home_dir: PathBuf,
    /// Data directory for databases (default: ~/.pulsivo-salesman/data).
    pub data_dir: PathBuf,
    /// Log level (trace, debug, info, warn, error).
    pub log_level: String,
    /// API listen address.
    #[serde(alias = "listen_addr")]
    pub api_listen: String,
    /// API authentication key.
    pub api_key: String,
    /// Default model configuration.
    pub default_model: DefaultModelConfig,
    /// Memory substrate configuration.
    pub memory: MemoryConfig,
    /// Web search/fetch configuration.
    #[serde(default)]
    pub web: WebConfig,
    /// Sales channel configuration.
    #[serde(default)]
    pub channels: ChannelsConfig,
    /// Config hot-reload settings.
    #[serde(default)]
    pub reload: ReloadConfig,
    /// Informational operating mode.
    #[serde(default)]
    pub mode: KernelMode,
    /// Informational language tag.
    #[serde(default = "default_language")]
    pub language: String,
    /// Config include files loaded before the root file.
    #[serde(default)]
    pub include: Vec<String>,
}

fn default_language() -> String {
    "en".to_string()
}

impl Default for KernelConfig {
    fn default() -> Self {
        let home_dir = dirs_home().join(".pulsivo-salesman");
        Self {
            home_dir: home_dir.clone(),
            data_dir: home_dir.join("data"),
            log_level: "info".to_string(),
            api_listen: "127.0.0.1:4200".to_string(),
            api_key: String::new(),
            default_model: DefaultModelConfig::default(),
            memory: MemoryConfig::default(),
            web: WebConfig::default(),
            channels: ChannelsConfig::default(),
            reload: ReloadConfig::default(),
            mode: KernelMode::default(),
            language: default_language(),
            include: Vec::new(),
        }
    }
}

impl KernelConfig {
    /// Validate config references and return human-readable warnings.
    pub fn validate(&self) -> Vec<String> {
        let mut warnings = Vec::new();

        if let Some(ref email) = self.channels.email {
            if email.smtp_host.trim().is_empty() {
                warnings.push("Email channel configured but smtp_host is empty".to_string());
            }
            if email.username.trim().is_empty() {
                warnings.push("Email channel configured but username is empty".to_string());
            }
            if std::env::var(&email.password_env)
                .unwrap_or_default()
                .is_empty()
            {
                warnings.push(format!(
                    "Email channel configured but {} is not set",
                    email.password_env
                ));
            }
        }

        match self.web.search_provider {
            SearchProvider::Brave => {
                if std::env::var(&self.web.brave.api_key_env)
                    .unwrap_or_default()
                    .is_empty()
                {
                    warnings.push(format!(
                        "Brave search selected but {} is not set",
                        self.web.brave.api_key_env
                    ));
                }
            }
            SearchProvider::Tavily => {
                if std::env::var(&self.web.tavily.api_key_env)
                    .unwrap_or_default()
                    .is_empty()
                {
                    warnings.push(format!(
                        "Tavily search selected but {} is not set",
                        self.web.tavily.api_key_env
                    ));
                }
            }
            SearchProvider::Perplexity => {
                if std::env::var(&self.web.perplexity.api_key_env)
                    .unwrap_or_default()
                    .is_empty()
                {
                    warnings.push(format!(
                        "Perplexity search selected but {} is not set",
                        self.web.perplexity.api_key_env
                    ));
                }
            }
            SearchProvider::DuckDuckGo | SearchProvider::Auto => {}
        }

        warnings
    }

    /// Clamp risky zero or extreme values to production-safe bounds.
    pub fn clamp_bounds(&mut self) {
        if self.web.fetch.max_chars == 0 {
            self.web.fetch.max_chars = 50_000;
        }

        if self.web.fetch.max_response_bytes == 0 {
            self.web.fetch.max_response_bytes = 5_000_000;
        } else if self.web.fetch.max_response_bytes > 50_000_000 {
            self.web.fetch.max_response_bytes = 50_000_000;
        }

        if self.web.fetch.timeout_secs == 0 {
            self.web.fetch.timeout_secs = 30;
        } else if self.web.fetch.timeout_secs > 120 {
            self.web.fetch.timeout_secs = 120;
        }
    }
}

impl std::fmt::Debug for KernelConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KernelConfig")
            .field("home_dir", &self.home_dir)
            .field("data_dir", &self.data_dir)
            .field("log_level", &self.log_level)
            .field("api_listen", &self.api_listen)
            .field(
                "api_key",
                &if self.api_key.is_empty() {
                    "<empty>"
                } else {
                    "<redacted>"
                },
            )
            .field("default_model", &self.default_model)
            .field("memory", &self.memory)
            .field("web", &self.web)
            .field("channels", &self.channels)
            .field("reload", &self.reload)
            .field("mode", &self.mode)
            .field("language", &self.language)
            .field("include", &format!("{} file(s)", self.include.len()))
            .finish()
    }
}

fn dirs_home() -> PathBuf {
    dirs::home_dir().unwrap_or_else(std::env::temp_dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = KernelConfig::default();
        assert_eq!(config.log_level, "info");
        assert_eq!(config.api_listen, "127.0.0.1:4200");
        assert!(config.channels.email.is_none());
    }

    #[test]
    fn test_config_serialization() {
        let config = KernelConfig::default();
        let toml_str = toml::to_string_pretty(&config).unwrap();
        assert!(toml_str.contains("log_level"));
        assert!(toml_str.contains("default_model"));
    }

    #[test]
    fn test_validate_no_channels() {
        let config = KernelConfig::default();
        let warnings = config.validate();
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_validate_missing_email_password_env() {
        let mut config = KernelConfig::default();
        config.channels.email = Some(EmailConfig {
            smtp_host: "smtp.example.com".to_string(),
            smtp_port: 587,
            username: "ops@example.com".to_string(),
            password_env: "PULSIVO_SALESMAN_TEST_NONEXISTENT_EMAIL".to_string(),
        });
        let warnings = config.validate();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("PULSIVO_SALESMAN_TEST_NONEXISTENT_EMAIL"));
    }

    #[test]
    fn test_email_config_defaults() {
        let config = EmailConfig::default();
        assert_eq!(config.smtp_port, 587);
        assert_eq!(config.password_env, "EMAIL_PASSWORD");
    }

    #[test]
    fn test_kernel_mode_serde() {
        let mode = KernelMode::Stable;
        let json = serde_json::to_string(&mode).unwrap();
        assert_eq!(json, "\"stable\"");
        let back: KernelMode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, KernelMode::Stable);
    }

    #[test]
    fn test_clamp_bounds_zero_fetch_bytes() {
        let mut config = KernelConfig::default();
        config.web.fetch.max_response_bytes = 0;
        config.clamp_bounds();
        assert_eq!(config.web.fetch.max_response_bytes, 5_000_000);
    }

    #[test]
    fn test_clamp_bounds_zero_fetch_timeout() {
        let mut config = KernelConfig::default();
        config.web.fetch.timeout_secs = 0;
        config.clamp_bounds();
        assert_eq!(config.web.fetch.timeout_secs, 30);
    }

    #[test]
    fn test_clamp_bounds_defaults_unchanged() {
        let mut config = KernelConfig::default();
        let max_chars = config.web.fetch.max_chars;
        let fetch_bytes = config.web.fetch.max_response_bytes;
        let fetch_timeout = config.web.fetch.timeout_secs;
        config.clamp_bounds();
        assert_eq!(config.web.fetch.max_chars, max_chars);
        assert_eq!(config.web.fetch.max_response_bytes, fetch_bytes);
        assert_eq!(config.web.fetch.timeout_secs, fetch_timeout);
    }
}
