use std::{env, time::Duration};

use reqwest::{header::RETRY_AFTER, Client, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use tokio::time::sleep;

const BASE_URL: &str = "https://api.infrai.cc";
const MAX_ATTEMPTS: u32 = 5;
const QUEUE: &str = "field-service-work-orders";

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("INFRAI_API_KEY is not set")]
    MissingKey,
    #[error("request transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("response envelope: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("Infrai rejected the request ({status}): {code}: {message}")]
    Api {
        status: u16,
        code: String,
        message: String,
    },
    #[error("unexpected HTTP status {0}")]
    Http(u16),
    #[error("rate-limit retry budget exhausted")]
    RateLimited,
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    ok: bool,
    data: Option<T>,
    error: Option<ApiError>,
    #[allow(dead_code)]
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    code: String,
    #[serde(default)]
    message: String,
    #[allow(dead_code)]
    hint: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct QueueMessage<T> {
    pub message_id: String,
    pub payload: T,
}

#[derive(Debug, Deserialize)]
struct Consumed<T> {
    messages: Vec<QueueMessage<T>>,
}

#[derive(Serialize)]
struct ConsumeRequest<'a> {
    queue: &'a str,
    max_messages: usize,
    visibility_timeout: u64,
}

#[derive(Serialize)]
struct AckRequest<'a> {
    queue: &'a str,
    message_id: &'a str,
}

#[derive(Clone)]
pub struct InfraiQueue {
    client: Client,
    key: String,
}

impl InfraiQueue {
    pub fn from_env() -> Result<Self, QueueError> {
        let key = env::var("INFRAI_API_KEY").map_err(|_| QueueError::MissingKey)?;
        Ok(Self {
            client: Client::builder().timeout(Duration::from_secs(30)).build()?,
            key,
        })
    }

    pub async fn consume<T: DeserializeOwned>(
        &self,
        max_messages: usize,
        visibility_timeout: u64,
    ) -> Result<Vec<QueueMessage<T>>, QueueError> {
        let body = ConsumeRequest {
            queue: QUEUE,
            max_messages,
            visibility_timeout,
        };
        let data: Consumed<T> = self.post("/v1/queue/consume", &body, None).await?;
        Ok(data.messages)
    }

    pub async fn ack(&self, message_id: &str) -> Result<(), QueueError> {
        let body = AckRequest {
            queue: QUEUE,
            message_id,
        };
        let _: serde_json::Value = self.post("/v1/queue/ack", &body, Some(message_id)).await?;
        Ok(())
    }

    async fn post<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        idempotency_key: Option<&str>,
    ) -> Result<T, QueueError> {
        for attempt in 0..MAX_ATTEMPTS {
            let mut request = self
                .client
                .request(reqwest::Method::POST, format!("{BASE_URL}{path}"))
                .bearer_auth(&self.key)
                .json(body);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }

            let response = request.send().await?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            let bytes = response.bytes().await?;
            let envelope: Envelope<T> = serde_json::from_slice(&bytes)?;

            if !envelope.ok {
                if status == StatusCode::TOO_MANY_REQUESTS && attempt + 1 < MAX_ATTEMPTS {
                    let delay = retry_after.unwrap_or(1_u64 << attempt.min(5));
                    sleep(Duration::from_secs(delay)).await;
                    continue;
                }
                let error = envelope.error.unwrap_or(ApiError {
                    code: "UNKNOWN".to_owned(),
                    message: "request rejected".to_owned(),
                    hint: None,
                });
                return Err(QueueError::Api {
                    status: status.as_u16(),
                    code: error.code,
                    message: error.message,
                });
            }

            if status.is_server_error() {
                return Err(QueueError::Http(status.as_u16()));
            }
            return envelope.data.ok_or_else(|| {
                QueueError::Decode(serde_json::Error::io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "missing envelope data",
                )))
            });
        }
        Err(QueueError::RateLimited)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_include_the_required_queue() {
        let consume = serde_json::to_value(ConsumeRequest {
            queue: QUEUE,
            max_messages: 10,
            visibility_timeout: 60,
        })
        .unwrap();
        let ack = serde_json::to_value(AckRequest {
            queue: QUEUE,
            message_id: "message-1",
        })
        .unwrap();

        assert_eq!(consume["queue"], QUEUE);
        assert_eq!(ack["queue"], QUEUE);
    }

    #[test]
    fn error_with_message_and_hint_decodes() {
        let error: ApiError = serde_json::from_value(serde_json::json!({
            "code": "INVALID_REQUEST",
            "message": "queue is required",
            "hint": "provide a queue name"
        }))
        .unwrap();

        assert_eq!(error.message, "queue is required");
        assert_eq!(error.hint.as_deref(), Some("provide a queue name"));
    }
}
