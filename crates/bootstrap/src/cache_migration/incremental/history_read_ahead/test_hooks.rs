use super::*;

// The calling thread snapshots these hooks before starting private readers.
#[derive(Clone, Default)]
pub(super) struct Hooks(
    pub(super) Option<Arc<dyn Fn(HistoryReadAheadEvent) -> HistoryReadAheadAction + Send + Sync>>,
);

thread_local! {
    static TEST_HOOK: std::cell::RefCell<Hooks> = const { std::cell::RefCell::new(Hooks(None)) };
}

#[derive(Clone, Copy, Debug)]
pub enum HistoryReadAheadEvent {
    Point { worker: usize, point: Point },
    Acquire { worker: usize, resource: Resource },
    Release { worker: usize, resource: Resource },
}

pub enum HistoryReadAheadAction {
    Continue,
    Cancel,
    Fail(rusqlite::Error),
}

/// Scenario-only hook; scoped to the calling thread and inherited by its readers.
#[cfg(feature = "scenario")]
pub fn set_history_read_ahead_hook_for_test(
    hook: Option<Arc<dyn Fn(HistoryReadAheadEvent) -> HistoryReadAheadAction + Send + Sync>>,
) {
    TEST_HOOK.with(|current| *current.borrow_mut() = Hooks(hook));
}

impl Hooks {
    pub(super) fn current() -> Self {
        TEST_HOOK.with(|current| current.borrow().clone())
    }

    pub(super) fn point(
        &self,
        worker: usize,
        point: Point,
        stop: &AtomicBool,
    ) -> rusqlite::Result<()> {
        if let Some(hook) = &self.0 {
            match hook(HistoryReadAheadEvent::Point { worker, point }) {
                HistoryReadAheadAction::Continue => {}
                HistoryReadAheadAction::Cancel => stop.store(true, Ordering::Relaxed),
                HistoryReadAheadAction::Fail(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub(super) fn resource(&self, worker: usize, resource: Resource) -> ResourceGuard {
        if let Some(hook) = &self.0 {
            hook(HistoryReadAheadEvent::Acquire { worker, resource });
        }
        ResourceGuard {
            hooks: self.clone(),
            worker,
            resource,
        }
    }
}

pub(super) struct ResourceGuard {
    hooks: Hooks,
    worker: usize,
    resource: Resource,
}

impl Drop for ResourceGuard {
    fn drop(&mut self) {
        if let Some(hook) = &self.hooks.0 {
            hook(HistoryReadAheadEvent::Release {
                worker: self.worker,
                resource: self.resource,
            });
        }
    }
}
