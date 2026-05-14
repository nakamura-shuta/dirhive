//! Phase 1 placeholder。Phase 3 で実装する。
//!
//! Phase 3 で DaemonState (= allowlist + ticket cache 等の共有 state) を定義する。
//! Phase 1 では空 struct を置いて、`lib.rs::run_health_check` の signature が型として
//! 成立するようにしておく。

/// daemon 全体で共有する state。Phase 3 で field を追加する。
pub struct DaemonState {}
