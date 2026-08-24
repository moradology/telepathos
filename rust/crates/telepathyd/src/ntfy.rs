//! Push bridge: new deliveries ring the phone's doorbell via ntfy.
//!
//! Decoupled from the relay internals on purpose — a watcher polls the
//! delivery sequence and POSTs each new item to the configured ntfy topic.
//! The phone's ntfy app turns that into an Android notification instantly
//! (ntfy.sh rides FCM; self-hosted ntfy uses the app's own connection).
//!
//! Config:
//!   TELEPATHY_NTFY_URL    e.g. https://ntfy.sh/telepathy-<secret-suffix>
//!   TELEPATHY_NTFY_TOKEN  optional access token (self-hosted auth)

use reqwest::Client;

pub struct NtfyPush {
    client: Client,
    url: String,
    token: Option<String>,
    last_seq: u64,
}

impl NtfyPush {
    pub fn new(url: String, token: Option<String>, last_seq: u64) -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_else(|_| Client::new()),
            url,
            token,
            last_seq,
        }
    }

    async fn post(&self, lane_name: &str, content: &str) {
        let mut req = self
            .client
            .post(&self.url)
            .header("Title", format!("Telepathy · {lane_name}"))
            .header("Tags", "ear")
            .body(content.to_string());
        if let Some(tok) = &self.token {
            req = req.bearer_auth(tok);
        }
        if let Err(e) = req.send().await {
            println!("ntfy push failed: {e}");
        }
    }

    /// Announce deliveries newer than the last announced seq.
    /// Fire-and-forget: a failed push is swallowed — the durable pending
    /// queue still has the item, and the next pinch reads it aloud.
    pub async fn announce_new(
        &mut self,
        relay: &crate::relay::RelayState,
        lane_names: &dyn Fn(&str) -> String,
    ) {
        let latest = *relay.next_seq.lock().unwrap();
        if latest <= self.last_seq {
            return;
        }
        let items = match relay.deliveries_after(self.last_seq, false, None, None, None) {
            Ok((items, _)) => items,
            Err(_) => return,
        };
        for d in &items {
            if d.seq > self.last_seq {
                let lane = lane_names(&d.chat_id);
                self.post(&lane, &d.content).await;
                self.last_seq = d.seq;
            }
        }
    }
}
