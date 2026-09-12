//! 内置压测场景

pub mod http;
pub mod udp_tracker;

use std::sync::Arc;

use crate::scenario::ScenarioRegistry;

/// 注册所有内置场景
pub fn register_all(registry: &mut ScenarioRegistry) {
    registry.register(Arc::new(http::HttpScenario::default()));
    registry.register(Arc::new(udp_tracker::UdpTrackerScenario::default()));
}
