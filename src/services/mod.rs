pub mod diagnostics;
pub mod event;
pub mod health;
pub mod introspection;
pub mod local;

pub use diagnostics::{DiagnosticsServiceImpl, DiagnosticsState};
pub use event::{EventService, InternalEvent, InternalEventSeverity, LedgerInternalEventSink};
pub use health::{
    HealthCondition, HealthService, InternalHealthCondition, RecoveryLedgerHealthSummary,
};
pub use introspection::IntrospectionService;
pub use local::LocalServiceImpl;

#[derive(Debug, Clone)]
pub struct InternalServices {
    pub events: EventService,
    pub health: HealthService,
    pub introspection: IntrospectionService,
}

impl InternalServices {
    pub fn new(events: EventService) -> Self {
        Self {
            events,
            health: HealthService,
            introspection: IntrospectionService,
        }
    }
}
