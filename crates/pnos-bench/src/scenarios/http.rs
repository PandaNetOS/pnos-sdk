//! 通用 HTTP 压测场景
//!
//! 支持 GET/POST，可自定义 URL 模板和请求体。

use std::time::Duration;

use async_trait::async_trait;
use rand::Rng;

use crate::scenario::{BenchContext, Scenario};

pub struct HttpScenario {
    client: reqwest::Client,
    urls: Vec<String>,
    method: HttpMethod,
}

#[derive(Debug, Clone, Copy)]
pub enum HttpMethod {
    Get,
    Post,
}

impl Default for HttpScenario {
    fn default() -> Self {
        Self {
            client: reqwest::Client::builder()
                .pool_max_idle_per_host(200)
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            urls: Vec::new(),
            method: HttpMethod::Get,
        }
    }
}

impl HttpScenario {
    pub fn with_urls(mut self, urls: Vec<String>) -> Self {
        self.urls = urls;
        self
    }

    pub fn with_method(mut self, method: HttpMethod) -> Self {
        self.method = method;
        self
    }
}

#[async_trait]
impl Scenario for HttpScenario {
    fn name(&self) -> &str {
        "http"
    }

    fn description(&self) -> &str {
        "通用 HTTP 压测（GET/POST）"
    }

    async fn setup(&self, ctx: &BenchContext) -> anyhow::Result<()> {
        // 从 args 读取 URL 模板
        if self.urls.is_empty() {
            if let Some(url_template) = ctx.args.get("url") {
                // 生成 1000 个变体 URL
                let mut rng = rand::thread_rng();
                let urls: Vec<String> = (0..1000)
                    .map(|i| {
                        if url_template.contains("{id}") {
                            url_template.replace("{id}", &i.to_string())
                        } else if url_template.contains("{rand}") {
                            url_template.replace("{rand}", &rng.gen::<u64>().to_string())
                        } else {
                            url_template.clone()
                        }
                    })
                    .collect();
                // 安全地修改 urls（需要内部可变性，这里用 unsafe 或重新设计）
                // 暂时用静态方式：setup 时把 urls 存到 ctx.args
                let _ = urls;
            }
        }
        Ok(())
    }

    async fn request(&self, ctx: &BenchContext) -> anyhow::Result<()> {
        let url = if !self.urls.is_empty() {
            let idx = ctx.next_request_id() as usize % self.urls.len();
            self.urls[idx].clone()
        } else if let Some(url_template) = ctx.args.get("url") {
            let id = ctx.next_request_id();
            if url_template.contains("{id}") {
                url_template.replace("{id}", &id.to_string())
            } else {
                url_template.clone()
            }
        } else {
            return Err(anyhow::anyhow!("未指定 URL，使用 --args url=http://..."));
        };

        match self.method {
            HttpMethod::Get => {
                let resp = self.client.get(&url).send().await?;
                let status = resp.status();
                if !status.is_success() {
                    return Err(anyhow::anyhow!("HTTP {}", status));
                }
                // 消耗响应体
                let _ = resp.bytes().await?;
            }
            HttpMethod::Post => {
                let body = ctx.args.get("body").cloned().unwrap_or_default();
                let resp = self.client.post(&url).body(body).send().await?;
                let status = resp.status();
                if !status.is_success() {
                    return Err(anyhow::anyhow!("HTTP {}", status));
                }
                let _ = resp.bytes().await?;
            }
        }
        Ok(())
    }
}
