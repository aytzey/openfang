//! Email channel adapter (IMAP + SMTP).
//!
//! Polls IMAP for new emails and sends responses via SMTP.
//! Uses the subject line for agent routing (e.g., "\[coder\] Fix this bug").

use crate::types::{ChannelAdapter, ChannelContent, ChannelMessage, ChannelType, ChannelUser};
use async_trait::async_trait;
use futures::Stream;
use lettre::message::{Mailbox, Message};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};
use zeroize::Zeroizing;

/// Email channel adapter using IMAP for receiving and SMTP for sending.
pub struct EmailAdapter {
    /// IMAP server host.
    imap_host: String,
    /// IMAP port (993 for TLS).
    imap_port: u16,
    /// SMTP server host.
    smtp_host: String,
    /// SMTP port (587 for STARTTLS).
    smtp_port: u16,
    /// Email address (used for both IMAP and SMTP).
    username: String,
    /// SECURITY: Password is zeroized on drop.
    password: Zeroizing<String>,
    /// How often to check for new emails.
    poll_interval: Duration,
    /// Which IMAP folders to monitor.
    folders: Vec<String>,
    /// Only process emails from these senders (empty = all).
    allowed_senders: Vec<String>,
    /// Shutdown signal.
    shutdown_tx: Arc<watch::Sender<bool>>,
    shutdown_rx: watch::Receiver<bool>,
}

impl EmailAdapter {
    /// Create a new email adapter.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        imap_host: String,
        imap_port: u16,
        smtp_host: String,
        smtp_port: u16,
        username: String,
        password: String,
        poll_interval_secs: u64,
        folders: Vec<String>,
        allowed_senders: Vec<String>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            imap_host,
            imap_port,
            smtp_host,
            smtp_port,
            username,
            password: Zeroizing::new(password),
            poll_interval: Duration::from_secs(poll_interval_secs),
            folders: if folders.is_empty() {
                vec!["INBOX".to_string()]
            } else {
                folders
            },
            allowed_senders,
            shutdown_tx: Arc::new(shutdown_tx),
            shutdown_rx,
        }
    }

    #[allow(dead_code)]
    fn is_allowed_sender(&self, sender: &str) -> bool {
        self.allowed_senders.is_empty() || self.allowed_senders.iter().any(|s| sender.contains(s))
    }

    /// Extract agent name from subject line brackets, e.g., "[coder] Fix the bug" -> Some("coder")
    #[allow(dead_code)]
    fn extract_agent_from_subject(subject: &str) -> Option<String> {
        let subject = subject.trim();
        if subject.starts_with('[') {
            if let Some(end) = subject.find(']') {
                let agent = &subject[1..end];
                if !agent.is_empty() {
                    return Some(agent.to_string());
                }
            }
        }
        None
    }

    /// Strip the agent tag from a subject line.
    #[allow(dead_code)]
    fn strip_agent_tag(subject: &str) -> String {
        let subject = subject.trim();
        if subject.starts_with('[') {
            if let Some(end) = subject.find(']') {
                return subject[end + 1..].trim().to_string();
            }
        }
        subject.to_string()
    }
}

#[async_trait]
impl ChannelAdapter for EmailAdapter {
    fn name(&self) -> &str {
        "email"
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::Email
    }

    async fn start(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = ChannelMessage> + Send>>, Box<dyn std::error::Error>>
    {
        let (_tx, rx) = mpsc::channel::<ChannelMessage>(256);
        let poll_interval = self.poll_interval;
        let _allowed_senders = self.allowed_senders.clone();
        let imap_host = self.imap_host.clone();
        let imap_port = self.imap_port;
        let _username = self.username.clone();
        let _password = self.password.clone();
        let _folders = self.folders.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();

        info!(
            "Starting email adapter (IMAP: {}:{}, polling every {:?})",
            imap_host, imap_port, poll_interval
        );

        tokio::spawn(async move {
            // Email polling is blocking I/O, so we'll use spawn_blocking
            // For now, implement as a polling loop with placeholder
            // Full IMAP implementation requires the `imap` crate
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("Email adapter shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(poll_interval) => {}
                }

                // Placeholder: In a full implementation, this would:
                // 1. Connect to IMAP server via TLS
                // 2. Select each folder
                // 3. Search for UNSEEN messages
                // 4. Fetch and parse each message (From, Subject, Body)
                // 5. Convert to ChannelMessage
                // 6. Mark as seen
                debug!("Email poll cycle (IMAP {}:{})", imap_host, imap_port);
            }
        });

        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn send(
        &self,
        user: &ChannelUser,
        content: ChannelContent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let text = match content {
            ChannelContent::Text(text) => text,
            _ => {
                warn!("Unsupported email content type for {}", user.platform_id);
                return Ok(());
            }
        };

        let from: Mailbox = self
            .username
            .parse()
            .map_err(|e| format!("Invalid sender email '{}': {e}", self.username))?;
        let to: Mailbox = user
            .platform_id
            .parse()
            .map_err(|e| format!("Invalid recipient email '{}': {e}", user.platform_id))?;

        let message = Message::builder()
            .from(from)
            .to(to)
            .subject("OpenFang Message")
            .body(text)?;

        let creds = Credentials::new(self.username.clone(), self.password.to_string());

        let mailer = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&self.smtp_host)?
            .port(self.smtp_port)
            .credentials(creds)
            .build();

        mailer.send(message).await?;
        info!("Sent email to {}", user.platform_id);
        debug!("SMTP delivered via {}:{}", self.smtp_host, self.smtp_port);
        Ok(())
    }

    async fn stop(&self) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self.shutdown_tx.send(true);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_email_adapter_creation() {
        let adapter = EmailAdapter::new(
            "imap.gmail.com".to_string(),
            993,
            "smtp.gmail.com".to_string(),
            587,
            "user@gmail.com".to_string(),
            "password".to_string(),
            30,
            vec![],
            vec![],
        );
        assert_eq!(adapter.name(), "email");
        assert_eq!(adapter.folders, vec!["INBOX".to_string()]);
    }

    #[test]
    fn test_allowed_senders() {
        let adapter = EmailAdapter::new(
            "imap.example.com".to_string(),
            993,
            "smtp.example.com".to_string(),
            587,
            "bot@example.com".to_string(),
            "pass".to_string(),
            30,
            vec![],
            vec!["boss@company.com".to_string()],
        );
        assert!(adapter.is_allowed_sender("boss@company.com"));
        assert!(!adapter.is_allowed_sender("random@other.com"));

        let open = EmailAdapter::new(
            "imap.example.com".to_string(),
            993,
            "smtp.example.com".to_string(),
            587,
            "bot@example.com".to_string(),
            "pass".to_string(),
            30,
            vec![],
            vec![],
        );
        assert!(open.is_allowed_sender("anyone@anywhere.com"));
    }

    #[test]
    fn test_extract_agent_from_subject() {
        assert_eq!(
            EmailAdapter::extract_agent_from_subject("[coder] Fix the bug"),
            Some("coder".to_string())
        );
        assert_eq!(
            EmailAdapter::extract_agent_from_subject("[researcher] Find papers on AI"),
            Some("researcher".to_string())
        );
        assert_eq!(
            EmailAdapter::extract_agent_from_subject("No brackets here"),
            None
        );
        assert_eq!(
            EmailAdapter::extract_agent_from_subject("[] Empty brackets"),
            None
        );
    }

    #[test]
    fn test_strip_agent_tag() {
        assert_eq!(
            EmailAdapter::strip_agent_tag("[coder] Fix the bug"),
            "Fix the bug"
        );
        assert_eq!(EmailAdapter::strip_agent_tag("No brackets"), "No brackets");
    }
}
