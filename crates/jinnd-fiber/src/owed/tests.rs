//! The derivation's own suite, split at the seam it was always written
//! across: [`properties`] carries the PROOF — laws quantified over the
//! whole input space — and [`cases`] says what each corner MEANS. The
//! fixtures both halves build inputs from live here, so the two halves
//! can never disagree about what a fiber looks like.

use std::any::TypeId;

use jinnd_api::{
    DependencySnapshot, Epoch, FiberId, Generation, Realm, ServiceType, TransitionCause,
};

use crate::plan::{Aim, Desired};

mod cases;
mod properties;

fn epoch(generation: u64) -> Epoch {
    Epoch {
        dependencies: vec![DependencySnapshot {
            service: ServiceType {
                type_id: TypeId::of::<()>(),
                name: "jinn.test/dependency",
            },
            provider: FiberId(1),
            generation: Generation(generation),
            realm: Realm::Root,
        }],
    }
}

fn aim(generation: u64, revision: u64) -> Aim {
    Aim {
        epoch: Some(epoch(generation)),
        revision,
    }
}

/// The aim of a fiber whose dependency has been withdrawn: no epoch, so
/// the planner cannot load for it.
fn unsatisfied() -> Aim {
    Aim {
        epoch: None,
        revision: 0,
    }
}

fn desired(aim: Aim) -> Desired {
    Desired {
        aim,
        cause: TransitionCause::InitialLoad,
        disposing: false,
        suspending: false,
        faulted: false,
    }
}
