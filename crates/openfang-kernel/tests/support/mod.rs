#![allow(dead_code)]

use openfang_kernel::OpenFangKernel;
use openfang_types::agent::AgentManifest;
use openfang_types::config::{DefaultModelConfig, KernelConfig};
use std::sync::Arc;

#[derive(Clone, Copy)]
pub struct TestModelConfig {
    pub provider: &'static str,
    pub model: &'static str,
    pub api_key_env: &'static str,
}

pub const OLLAMA_TEST_MODEL: TestModelConfig = TestModelConfig {
    provider: "ollama",
    model: "test-model",
    api_key_env: "OLLAMA_API_KEY",
};

pub const GROQ_TEST_MODEL: TestModelConfig = TestModelConfig {
    provider: "groq",
    model: "llama-3.3-70b-versatile",
    api_key_env: "GROQ_API_KEY",
};

pub struct TestKernelHarness {
    pub kernel: Arc<OpenFangKernel>,
    _tmp: tempfile::TempDir,
}

impl TestKernelHarness {
    pub fn boot(model: TestModelConfig) -> Self {
        let tmp = tempfile::tempdir().expect("Failed to create temp dir for kernel test");
        let config = KernelConfig {
            home_dir: tmp.path().to_path_buf(),
            data_dir: tmp.path().join("data"),
            default_model: DefaultModelConfig {
                provider: model.provider.to_string(),
                model: model.model.to_string(),
                api_key_env: model.api_key_env.to_string(),
                base_url: None,
                reasoning_effort: None,
            },
            ..KernelConfig::default()
        };

        let kernel = Arc::new(
            OpenFangKernel::boot_with_config(config).expect("Kernel should boot for test"),
        );

        Self { kernel, _tmp: tmp }
    }

    pub fn with_self_handle(self) -> Self {
        self.kernel.set_self_handle();
        self
    }
}

impl Drop for TestKernelHarness {
    fn drop(&mut self) {
        self.kernel.shutdown();
    }
}

pub fn parse_manifest(toml_str: &str) -> AgentManifest {
    toml::from_str(toml_str).expect("Should parse manifest")
}

pub fn skip_if_env_missing(env_var: &str, label: &str) -> bool {
    if std::env::var(env_var).is_ok() {
        return false;
    }

    eprintln!("{env_var} not set, skipping {label}");
    true
}
