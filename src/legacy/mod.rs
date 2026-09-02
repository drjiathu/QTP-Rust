//! Compatibility rules, normalization, and replay for legacy QTP records.

mod normalization;
mod references;
mod replay;
mod rules;

pub use normalization::{LegacyContext, NormalizeError, normalize};
pub use references::OrderReferenceIndex;
pub use replay::{LegacyReplay, ReplayError, ReplayErrorKind, ReplaySource};
pub use rules::LegacyQtpRules;
