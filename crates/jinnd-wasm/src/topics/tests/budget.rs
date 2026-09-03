//! Red-first witness for M2-K25(b): a delivery's deterministic fuel bound
//! belongs to its registration and survives the registry snapshot.

use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

use jinnd_api::{DispatchMode, KernelFuture};

use crate::selector::{NoRealms, Selector};
use crate::topics::{EventTarget, LocalTopics};

#[derive(Default)]
struct BudgetProbe(Mutex<Vec<Option<NonZeroU64>>>);

impl EventTarget for BudgetProbe {
    fn deliver(
        &self,
        _: u64,
        _: &str,
        _: Vec<u8>,
        budget: Option<NonZeroU64>,
    ) -> KernelFuture<'static, Vec<u8>> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(budget);
        Box::pin(async { Ok(Vec::new()) })
    }
}

#[tokio::test]
async fn each_delivery_carries_its_registration_fuel_budget() {
    let topics = LocalTopics::default();
    let probe = Arc::new(BudgetProbe::default());
    let budget = NonZeroU64::new(12_345);
    topics.listen("t", 1, 7, None, budget, probe.clone());

    topics
        .emit(
            1,
            "t",
            DispatchMode::Emit,
            &Selector::All,
            Vec::new(),
            None,
            &NoRealms,
        )
        .await;

    assert_eq!(
        *probe.0.lock().unwrap_or_else(|poison| poison.into_inner()),
        vec![budget]
    );
}
