pub mod api;
pub mod adjust;
pub mod config;
pub mod convert;
pub mod domain;
pub mod error;
pub mod manager;
pub mod merge;
pub mod transport;

pub use api::{NopNri, NriApi, NriContainerEvent, NriDomain, NriPodEvent};
pub use adjust::apply_annotation_adjustments;
pub use config::NriManagerConfig;
pub use convert::external_annotations;
pub use domain::RuntimeSnapshot;
pub use error::{NriError, Result};
pub use manager::NriManager;
pub use merge::merge_annotation_adjustments;
pub use transport::{PluginTtrpcClient, RuntimeTtrpcServer};
