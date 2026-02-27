//! OpenAI Codex OAuth driver (ChatGPT Codex Responses backend).
//!
//! Uses OAuth access tokens against:
//! `https://chatgpt.com/backend-api/codex/responses`

use crate::llm_driver::{CompletionRequest, CompletionResponse, LlmDriver, LlmError, StreamEvent};
use async_trait::async_trait;
use base64::Engine;
use futures::StreamExt;
use openfang_types::message::{ContentBlock, MessageContent, Role, StopReason, TokenUsage};
use openfang_types::tool::{ToolCall, ToolDefinition};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use tracing::debug;
use zeroize::Zeroizing;

/// OpenAI Codex (OAuth) driver.
pub struct CodexDriver {
    access_token: Zeroizing<String>,
    account_id: Option<String>,
    base_url: String,
    client: reqwest::Client,
}

impl CodexDriver {
    /// Create a new Codex driver.
    pub fn new(access_token: String, base_url: String, account_id: Option<String>) -> Self {
        let account_id = account_id
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        Self {
            access_token: Zeroizing::new(access_token),
            account_id,
            base_url,
            client: reqwest::Client::new(),
        }
    }

    fn endpoint_url(&self) -> String {
        let trimmed = self.base_url.trim_end_matches('/');
        if trimmed.ends_with("/responses") {
            trimmed.to_string()
        } else {
            format!("{trimmed}/responses")
        }
    }

    fn parse_jwt_payload(jwt: &str) -> Option<Value> {
        let parts: Vec<&str> = jwt.split('.').collect();
        if parts.len() < 2 {
            return None;
        }
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[1])
            .ok()
            .or_else(|| {
                base64::engine::general_purpose::URL_SAFE
                    .decode(parts[1])
                    .ok()
            })?;
        serde_json::from_slice::<Value>(&payload).ok()
    }

    fn account_id_from_access_token(token: &str) -> Option<String> {
        let payload = Self::parse_jwt_payload(token)?;
        payload
            .get("https://api.openai.com/auth.chatgpt_account_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .or_else(|| {
                payload
                    .get("chatgpt_account_id")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToString::to_string)
            })
            .or_else(|| {
                payload
                    .get("https://api.openai.com/auth")
                    .and_then(Value::as_object)
                    .and_then(|auth| auth.get("chatgpt_account_id"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToString::to_string)
            })
    }

    fn resolve_auth_context(&self) -> Result<(String, String), LlmError> {
        let env_token = std::env::var("OPENAI_CODEX_ACCESS_TOKEN")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let fallback_token = self.access_token.trim();
        let access_token = env_token.unwrap_or_else(|| fallback_token.to_string());
        if access_token.is_empty() {
            return Err(LlmError::MissingApiKey(
                "Set OPENAI_CODEX_ACCESS_TOKEN environment variable for openai-codex".to_string(),
            ));
        }

        let env_account_id = std::env::var("OPENAI_CODEX_ACCOUNT_ID")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let fallback_account_id = self
            .account_id
            .as_ref()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let account_id = env_account_id
            .or(fallback_account_id)
            .or_else(|| Self::account_id_from_access_token(&access_token))
            .ok_or_else(|| LlmError::Api {
                status: 401,
                message: "OAuth token is missing organization context (chatgpt_account_id). Reconnect from Sales > Connect OAuth.".to_string(),
            })?;

        Ok((access_token, account_id))
    }

    fn instructions_for(request: &CompletionRequest) -> String {
        request
            .system
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .unwrap_or("You are a helpful assistant.")
            .to_string()
    }

    fn build_tools(tools: &[ToolDefinition]) -> Vec<Value> {
        tools
            .iter()
            .map(|t| {
                let mut schema =
                    openfang_types::tool::normalize_schema_for_provider(&t.input_schema, "openai");
                Self::enforce_strict_object_schema(&mut schema);
                serde_json::json!({
                    "type": "function",
                    "name": t.name,
                    "description": t.description,
                    "parameters": schema
                })
            })
            .collect()
    }

    fn enforce_strict_object_schema(schema: &mut Value) {
        let Some(obj) = schema.as_object_mut() else {
            return;
        };

        let is_object = obj
            .get("type")
            .and_then(Value::as_str)
            .map(|t| t == "object")
            .unwrap_or(false)
            || obj.contains_key("properties");
        if is_object {
            obj.insert("additionalProperties".to_string(), Value::Bool(false));
            let required_keys = obj
                .get("properties")
                .and_then(Value::as_object)
                .map(|props| {
                    props
                        .keys()
                        .map(|k| Value::String(k.clone()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            obj.insert("required".to_string(), Value::Array(required_keys));
        }

        if let Some(props) = obj.get_mut("properties").and_then(Value::as_object_mut) {
            for child in props.values_mut() {
                Self::enforce_strict_object_schema(child);
            }
        }

        if let Some(items) = obj.get_mut("items") {
            if items.is_object() {
                Self::enforce_strict_object_schema(items);
            } else if let Some(arr) = items.as_array_mut() {
                for child in arr {
                    Self::enforce_strict_object_schema(child);
                }
            }
        }

        for key in ["anyOf", "oneOf", "allOf"] {
            if let Some(arr) = obj.get_mut(key).and_then(Value::as_array_mut) {
                for child in arr {
                    Self::enforce_strict_object_schema(child);
                }
            }
        }
    }

    fn reasoning_effort_label(request: &CompletionRequest) -> Option<&'static str> {
        request
            .reasoning_effort
            .as_ref()
            .map(|effort| match effort {
                openfang_types::agent::ReasoningEffort::Low => "low",
                openfang_types::agent::ReasoningEffort::Medium => "medium",
                openfang_types::agent::ReasoningEffort::High => "high",
            })
    }

    fn push_user_message(
        input_items: &mut Vec<Value>,
        text_parts: Vec<String>,
        image_parts: Vec<Value>,
    ) {
        let mut content: Vec<Value> = text_parts
            .into_iter()
            .filter(|t| !t.is_empty())
            .map(|text| serde_json::json!({"type": "input_text", "text": text}))
            .collect();
        content.extend(image_parts);
        if content.is_empty() {
            return;
        }
        input_items.push(serde_json::json!({
            "type": "message",
            "role": "user",
            "content": content
        }));
    }

    fn build_input_items(request: &CompletionRequest) -> Vec<Value> {
        let mut input_items: Vec<Value> = Vec::new();
        let mut seen_call_ids: HashSet<String> = HashSet::new();

        for msg in &request.messages {
            match (&msg.role, &msg.content) {
                (Role::System, MessageContent::Text(_)) => {
                    // We pass system guidance via top-level `instructions`.
                }
                (Role::User, MessageContent::Text(text)) => {
                    let text = text.trim().to_string();
                    if text.is_empty() {
                        continue;
                    }
                    input_items.push(serde_json::json!({
                        "type": "message",
                        "role": "user",
                        "content": [{"type":"input_text","text": text}]
                    }));
                }
                (Role::Assistant, MessageContent::Text(text)) => {
                    let text = text.trim().to_string();
                    if text.is_empty() {
                        continue;
                    }
                    input_items.push(serde_json::json!({
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type":"output_text","text": text}]
                    }));
                }
                (Role::User, MessageContent::Blocks(blocks)) => {
                    let mut text_parts: Vec<String> = Vec::new();
                    let mut image_parts: Vec<Value> = Vec::new();
                    for block in blocks {
                        match block {
                            ContentBlock::Text { text } => {
                                if !text.is_empty() {
                                    text_parts.push(text.clone());
                                }
                            }
                            ContentBlock::Image { media_type, data } => {
                                image_parts.push(serde_json::json!({
                                    "type": "input_image",
                                    "detail": "auto",
                                    "image_url": format!("data:{media_type};base64,{data}")
                                }));
                            }
                            ContentBlock::ToolResult {
                                tool_use_id,
                                content,
                                ..
                            } => {
                                if !seen_call_ids.contains(tool_use_id) {
                                    // Skip orphaned outputs; backend rejects unmatched call_id.
                                    continue;
                                }
                                input_items.push(serde_json::json!({
                                    "type": "function_call_output",
                                    "call_id": tool_use_id,
                                    "output": content
                                }));
                            }
                            ContentBlock::Thinking { .. } => {}
                            ContentBlock::ToolUse { .. } => {}
                            ContentBlock::Unknown => {}
                        }
                    }
                    Self::push_user_message(&mut input_items, text_parts, image_parts);
                }
                (Role::Assistant, MessageContent::Blocks(blocks)) => {
                    let mut text_parts: Vec<String> = Vec::new();
                    for block in blocks {
                        match block {
                            ContentBlock::Text { text } => {
                                if !text.is_empty() {
                                    text_parts.push(text.clone());
                                }
                            }
                            ContentBlock::ToolUse { id, name, input } => {
                                seen_call_ids.insert(id.clone());
                                input_items.push(serde_json::json!({
                                    "type": "function_call",
                                    "call_id": id,
                                    "name": name,
                                    "arguments": serde_json::to_string(input).unwrap_or_else(|_| "{}".to_string())
                                }));
                            }
                            ContentBlock::Thinking { .. } => {}
                            ContentBlock::Image { .. } => {}
                            ContentBlock::ToolResult { .. } => {}
                            ContentBlock::Unknown => {}
                        }
                    }
                    if !text_parts.is_empty() {
                        input_items.push(serde_json::json!({
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type":"output_text","text": text_parts.join("")}]
                        }));
                    }
                }
                (Role::System, MessageContent::Blocks(_)) => {}
            }
        }

        if input_items.is_empty() {
            input_items.push(serde_json::json!({
                "type": "message",
                "role": "user",
                "content": [{"type":"input_text","text":"Continue."}]
            }));
        }

        input_items
    }

    async fn maybe_send(tx: &Option<tokio::sync::mpsc::Sender<StreamEvent>>, event: StreamEvent) {
        if let Some(tx) = tx.as_ref() {
            let _ = tx.send(event).await;
        }
    }

    fn usage_from_response(response: &Value) -> TokenUsage {
        let usage = response.get("usage").and_then(Value::as_object);
        TokenUsage {
            input_tokens: usage
                .and_then(|u| u.get("input_tokens"))
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            output_tokens: usage
                .and_then(|u| u.get("output_tokens"))
                .and_then(Value::as_u64)
                .unwrap_or_default(),
        }
    }

    fn build_completion_from_response(
        response: Option<&Value>,
        fallback_text: String,
        fallback_tool_calls: Vec<ToolCall>,
        fallback_usage: TokenUsage,
    ) -> CompletionResponse {
        let mut text = String::new();
        let mut content: Vec<ContentBlock> = Vec::new();
        let mut tool_calls: Vec<ToolCall> = Vec::new();
        let mut usage = fallback_usage;
        let mut stop_reason = StopReason::EndTurn;

        if let Some(resp) = response {
            usage = Self::usage_from_response(resp);

            if let Some(status) = resp.get("status").and_then(Value::as_str) {
                if status.eq_ignore_ascii_case("incomplete") {
                    stop_reason = StopReason::MaxTokens;
                }
            }

            if let Some(output_items) = resp.get("output").and_then(Value::as_array) {
                for item in output_items {
                    let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
                    match item_type {
                        "message" => {
                            if let Some(parts) = item.get("content").and_then(Value::as_array) {
                                for part in parts {
                                    let part_type = part
                                        .get("type")
                                        .and_then(Value::as_str)
                                        .unwrap_or_default();
                                    if (part_type == "output_text" || part_type == "refusal")
                                        && part.get("text").and_then(Value::as_str).is_some()
                                    {
                                        let t = part
                                            .get("text")
                                            .and_then(Value::as_str)
                                            .unwrap_or_default();
                                        text.push_str(t);
                                    }
                                    if part_type == "refusal" {
                                        let refusal = part
                                            .get("refusal")
                                            .and_then(Value::as_str)
                                            .unwrap_or_default();
                                        text.push_str(refusal);
                                    }
                                }
                            }
                        }
                        "function_call" => {
                            let call_id = item
                                .get("call_id")
                                .and_then(Value::as_str)
                                .or_else(|| item.get("id").and_then(Value::as_str))
                                .unwrap_or_default()
                                .to_string();
                            let name = item
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string();
                            if call_id.is_empty() || name.is_empty() {
                                continue;
                            }
                            let arguments = item
                                .get("arguments")
                                .and_then(Value::as_str)
                                .unwrap_or("{}");
                            let input: Value = serde_json::from_str(arguments)
                                .unwrap_or_else(|_| serde_json::json!({}));
                            tool_calls.push(ToolCall {
                                id: call_id.clone(),
                                name: name.clone(),
                                input: input.clone(),
                            });
                            content.push(ContentBlock::ToolUse {
                                id: call_id,
                                name,
                                input,
                            });
                        }
                        _ => {}
                    }
                }
            }
        }

        if text.is_empty() {
            text = fallback_text;
        }
        if !text.is_empty() {
            content.insert(0, ContentBlock::Text { text });
        }

        if tool_calls.is_empty() && !fallback_tool_calls.is_empty() {
            for call in &fallback_tool_calls {
                content.push(ContentBlock::ToolUse {
                    id: call.id.clone(),
                    name: call.name.clone(),
                    input: call.input.clone(),
                });
            }
            tool_calls = fallback_tool_calls;
        }

        if !tool_calls.is_empty() {
            stop_reason = StopReason::ToolUse;
        }

        CompletionResponse {
            content,
            stop_reason,
            tool_calls,
            usage,
        }
    }

    async fn run_completion(
        &self,
        request: CompletionRequest,
        tx: Option<tokio::sync::mpsc::Sender<StreamEvent>>,
    ) -> Result<CompletionResponse, LlmError> {
        let (access_token, account_id) = self.resolve_auth_context()?;

        let url = self.endpoint_url();
        let input_items = Self::build_input_items(&request);
        let tools = Self::build_tools(&request.tools);
        let instructions = Self::instructions_for(&request);

        let mut body = serde_json::json!({
            "model": request.model,
            "stream": true,
            "store": false,
            "instructions": instructions,
            "input": input_items,
        });
        if !tools.is_empty() {
            body["tools"] = Value::Array(tools);
            body["tool_choice"] = serde_json::json!("auto");
        }
        if let Some(effort) = Self::reasoning_effort_label(&request) {
            body["reasoning"] = serde_json::json!({ "effort": effort });
        }

        debug!(url = %url, "Sending Codex responses request");
        let mut req = self
            .client
            .post(&url)
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .header("authorization", format!("Bearer {access_token}"))
            .header("openai-beta", "responses=experimental")
            .header("originator", "pi")
            .json(&body);
        req = req.header("chatgpt-account-id", account_id);

        let resp = req
            .send()
            .await
            .map_err(|e| LlmError::Http(e.to_string()))?;
        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(LlmError::Api {
                status,
                message: body,
            });
        }

        let mut buffer = String::new();
        let mut current_event: Option<String> = None;
        let mut current_data = String::new();

        let mut text_accum = String::new();
        let mut tool_meta: HashMap<String, (String, String)> = HashMap::new();
        let mut tool_args: HashMap<String, String> = HashMap::new();
        let mut started_item_ids: HashSet<String> = HashSet::new();
        let mut ended_call_ids: HashSet<String> = HashSet::new();
        let mut fallback_tool_calls: Vec<ToolCall> = Vec::new();
        let mut fallback_usage = TokenUsage::default();
        let mut completed_response: Option<Value> = None;

        let mut byte_stream = resp.bytes_stream();
        while let Some(chunk_result) = byte_stream.next().await {
            let chunk = chunk_result.map_err(|e| LlmError::Http(e.to_string()))?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(pos) = buffer.find('\n') {
                let mut line = buffer[..pos].to_string();
                if line.ends_with('\r') {
                    line.pop();
                }
                buffer = buffer[pos + 1..].to_string();

                if line.is_empty() {
                    if let Some(event_name) = current_event.take() {
                        let data = current_data.trim().to_string();
                        current_data.clear();
                        if data.is_empty() || data == "[DONE]" {
                            continue;
                        }

                        let json: Value = match serde_json::from_str(&data) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };

                        match event_name.as_str() {
                            "response.output_text.delta" => {
                                if let Some(delta) = json.get("delta").and_then(Value::as_str) {
                                    if !delta.is_empty() {
                                        text_accum.push_str(delta);
                                        Self::maybe_send(
                                            &tx,
                                            StreamEvent::TextDelta {
                                                text: delta.to_string(),
                                            },
                                        )
                                        .await;
                                    }
                                }
                            }
                            "response.output_item.added" => {
                                let item = json.get("item").and_then(Value::as_object);
                                if let Some(item) = item {
                                    let item_type = item
                                        .get("type")
                                        .and_then(Value::as_str)
                                        .unwrap_or_default();
                                    if item_type == "function_call" {
                                        let item_id = item
                                            .get("id")
                                            .and_then(Value::as_str)
                                            .unwrap_or_default()
                                            .to_string();
                                        let call_id = item
                                            .get("call_id")
                                            .and_then(Value::as_str)
                                            .or_else(|| item.get("id").and_then(Value::as_str))
                                            .unwrap_or_default()
                                            .to_string();
                                        let name = item
                                            .get("name")
                                            .and_then(Value::as_str)
                                            .unwrap_or_default()
                                            .to_string();
                                        if !item_id.is_empty() {
                                            tool_meta.insert(
                                                item_id.clone(),
                                                (call_id.clone(), name.clone()),
                                            );
                                            if started_item_ids.insert(item_id) {
                                                Self::maybe_send(
                                                    &tx,
                                                    StreamEvent::ToolUseStart { id: call_id, name },
                                                )
                                                .await;
                                            }
                                        }
                                    }
                                }
                            }
                            "response.function_call_arguments.delta" => {
                                let item_id = json
                                    .get("item_id")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_string();
                                let delta = json
                                    .get("delta")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_string();
                                if !item_id.is_empty() {
                                    tool_args
                                        .entry(item_id)
                                        .and_modify(|s| s.push_str(&delta))
                                        .or_insert(delta.clone());
                                }
                                if !delta.is_empty() {
                                    Self::maybe_send(
                                        &tx,
                                        StreamEvent::ToolInputDelta { text: delta },
                                    )
                                    .await;
                                }
                            }
                            "response.function_call_arguments.done" => {
                                let item_id = json
                                    .get("item_id")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_string();
                                let args_str = json
                                    .get("arguments")
                                    .and_then(Value::as_str)
                                    .unwrap_or("{}")
                                    .to_string();
                                if !item_id.is_empty() {
                                    tool_args.insert(item_id.clone(), args_str.clone());
                                    if let Some((call_id, name)) = tool_meta.get(&item_id) {
                                        if ended_call_ids.insert(call_id.clone()) {
                                            let input: Value = serde_json::from_str(&args_str)
                                                .unwrap_or_else(|_| serde_json::json!({}));
                                            Self::maybe_send(
                                                &tx,
                                                StreamEvent::ToolUseEnd {
                                                    id: call_id.clone(),
                                                    name: name.clone(),
                                                    input,
                                                },
                                            )
                                            .await;
                                        }
                                    }
                                }
                            }
                            "response.output_item.done" => {
                                let item = json.get("item").and_then(Value::as_object);
                                if let Some(item) = item {
                                    let item_type = item
                                        .get("type")
                                        .and_then(Value::as_str)
                                        .unwrap_or_default();
                                    if item_type == "function_call" {
                                        let call_id = item
                                            .get("call_id")
                                            .and_then(Value::as_str)
                                            .or_else(|| item.get("id").and_then(Value::as_str))
                                            .unwrap_or_default()
                                            .to_string();
                                        let name = item
                                            .get("name")
                                            .and_then(Value::as_str)
                                            .unwrap_or_default()
                                            .to_string();
                                        let args = item
                                            .get("arguments")
                                            .and_then(Value::as_str)
                                            .unwrap_or("{}");
                                        let input: Value = serde_json::from_str(args)
                                            .unwrap_or_else(|_| serde_json::json!({}));
                                        if !call_id.is_empty() && !name.is_empty() {
                                            fallback_tool_calls.push(ToolCall {
                                                id: call_id.clone(),
                                                name: name.clone(),
                                                input: input.clone(),
                                            });
                                            if ended_call_ids.insert(call_id.clone()) {
                                                Self::maybe_send(
                                                    &tx,
                                                    StreamEvent::ToolUseEnd {
                                                        id: call_id,
                                                        name,
                                                        input,
                                                    },
                                                )
                                                .await;
                                            }
                                        }
                                    }
                                }
                            }
                            "response.completed" => {
                                if let Some(response) = json.get("response") {
                                    fallback_usage = Self::usage_from_response(response);
                                    completed_response = Some(response.clone());
                                }
                            }
                            _ => {}
                        }
                    }
                    continue;
                }

                if line.starts_with(':') {
                    continue;
                }
                if let Some(v) = line.strip_prefix("event:") {
                    current_event = Some(v.trim().to_string());
                    continue;
                }
                if let Some(v) = line.strip_prefix("data:") {
                    if !current_data.is_empty() {
                        current_data.push('\n');
                    }
                    current_data.push_str(v.trim_start());
                }
            }
        }

        let mut response = Self::build_completion_from_response(
            completed_response.as_ref(),
            text_accum,
            fallback_tool_calls,
            fallback_usage,
        );

        // If streaming got argument deltas but response output omitted function_call items,
        // reconstruct from accumulated tool metadata.
        if response.tool_calls.is_empty() && !tool_meta.is_empty() {
            for (item_id, (call_id, name)) in tool_meta {
                let args = tool_args
                    .get(&item_id)
                    .cloned()
                    .unwrap_or_else(|| "{}".to_string());
                let input: Value =
                    serde_json::from_str(&args).unwrap_or_else(|_| serde_json::json!({}));
                response.tool_calls.push(ToolCall {
                    id: call_id.clone(),
                    name: name.clone(),
                    input: input.clone(),
                });
                response.content.push(ContentBlock::ToolUse {
                    id: call_id,
                    name,
                    input,
                });
            }
            response.stop_reason = StopReason::ToolUse;
        }

        Self::maybe_send(
            &tx,
            StreamEvent::ContentComplete {
                stop_reason: response.stop_reason,
                usage: response.usage,
            },
        )
        .await;

        Ok(response)
    }
}

#[async_trait]
impl LlmDriver for CodexDriver {
    async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.run_completion(request, None).await
    }

    async fn stream(
        &self,
        request: CompletionRequest,
        tx: tokio::sync::mpsc::Sender<StreamEvent>,
    ) -> Result<CompletionResponse, LlmError> {
        self.run_completion(request, Some(tx)).await
    }
}
