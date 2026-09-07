//! 健康检查构建器
//!
//! 应用可以用它快速构建符合 pnos 标准的健康检查响应。

use pnos::health::{HealthResponse, HealthStatus};

/// 健康检查构建器
pub struct HealthBuilder {
    version: String,
    started_at: std::time::Instant,
    dependencies: Vec<(String, HealthStatus, Option<String>)>,
}

impl HealthBuilder {
    /// 创建健康检查构建器
    pub fn new(version: impl Into<String>) -> Self {
        Self {
            version: version.into(),
            started_at: std::time::Instant::now(),
            dependencies: Vec::new(),
        }
    }

    /// 添加依赖项（健康）
    pub fn dependency_ok(mut self, name: impl Into<String>) -> Self {
        self.dependencies
            .push((name.into(), HealthStatus::Ok, None));
        self
    }

    /// 添加依赖项（降级）
    pub fn dependency_degraded(mut self, name: impl Into<String>, msg: impl Into<String>) -> Self {
        self.dependencies
            .push((name.into(), HealthStatus::Degraded, Some(msg.into())));
        self
    }

    /// 添加依赖项（不可用）
    pub fn dependency_down(mut self, name: impl Into<String>, msg: impl Into<String>) -> Self {
        self.dependencies
            .push((name.into(), HealthStatus::Down, Some(msg.into())));
        self
    }

    /// 构建健康检查响应
    pub fn build(&self) -> HealthResponse {
        let overall = if self
            .dependencies
            .iter()
            .any(|(_, s, _)| matches!(s, HealthStatus::Down))
        {
            HealthStatus::Down
        } else if self
            .dependencies
            .iter()
            .any(|(_, s, _)| matches!(s, HealthStatus::Degraded))
        {
            HealthStatus::Degraded
        } else {
            HealthStatus::Ok
        };

        HealthResponse {
            status: overall,
            version: self.version.clone(),
            uptime: self.started_at.elapsed().as_secs(),
            dependencies: self
                .dependencies
                .iter()
                .map(|(name, status, msg)| pnos::health::DependencyHealth {
                    name: name.clone(),
                    status: *status,
                    message: msg.clone(),
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_all_ok() {
        let health = HealthBuilder::new("1.0.0")
            .dependency_ok("pk")
            .dependency_ok("spde")
            .build();
        assert_eq!(health.status, HealthStatus::Ok);
        assert_eq!(health.version, "1.0.0");
        assert_eq!(health.dependencies.len(), 2);
    }

    #[test]
    fn test_health_degraded() {
        let health = HealthBuilder::new("1.0.0")
            .dependency_ok("pk")
            .dependency_degraded("spde", "队列积压")
            .build();
        assert_eq!(health.status, HealthStatus::Degraded);
    }

    #[test]
    fn test_health_down() {
        let health = HealthBuilder::new("1.0.0")
            .dependency_ok("pk")
            .dependency_down("spde", "连接失败")
            .build();
        assert_eq!(health.status, HealthStatus::Down);
    }

    #[test]
    fn test_health_serialization() {
        let health = HealthBuilder::new("1.0.0").build();
        let json = serde_json::to_string(&health).unwrap();
        assert!(json.contains("\"status\":\"ok\""));
        assert!(json.contains("\"version\":\"1.0.0\""));
    }
}
