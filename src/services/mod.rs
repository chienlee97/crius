pub mod event;
pub mod health;
pub mod introspection;

pub use event::{EventService, InternalEvent, InternalEventSeverity};
pub use health::{HealthCondition, HealthService, InternalHealthCondition};
pub use introspection::IntrospectionService;

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
