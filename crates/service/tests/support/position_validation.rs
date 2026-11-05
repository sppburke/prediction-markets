#![allow(dead_code)]
use futures::StreamExt;
use pe_core_types::WalletAddress;
use pe_paper_state::PaperStateDb;
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::position_seeder::{
    AnchorInstall, BRACKET_CONCURRENCY, CausalPositionError, CausalPositionValidator, FailureClass,
    ValidationPurpose,
};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::time::Instant;

pub struct ValidationOutcomes {
    pub started_prefix: usize,
    pub completed_at: HashMap<WalletAddress, Instant>,
    pub accepted: Vec<AnchorInstall>,
    pub deferred: Vec<(WalletAddress, CausalPositionError)>,
    pub shared: Option<CausalPositionError>,
}

pub trait ControlValidationExt {
    fn validate_via_control(
        &self,
        wallets: &[WalletAddress],
        control_tx: &mpsc::Sender<OrchestratorControl>,
        paper_state: &PaperStateDb,
        purpose: ValidationPurpose,
        deadline: Option<Instant>,
    ) -> impl std::future::Future<Output = ValidationOutcomes> + Send;
}
impl ControlValidationExt for CausalPositionValidator {
    async fn validate_via_control(
        &self,
        wallets: &[WalletAddress],
        control_tx: &mpsc::Sender<OrchestratorControl>,
        paper_state: &PaperStateDb,
        purpose: ValidationPurpose,
        deadline: Option<Instant>,
    ) -> ValidationOutcomes {
        let mut running = futures::stream::FuturesUnordered::new();
        let mut started_prefix = 0;
        let mut completed = Vec::new();
        loop {
            while running.len() < BRACKET_CONCURRENCY
                && started_prefix < wallets.len()
                && deadline.is_none_or(|end| Instant::now() < end)
            {
                let index = started_prefix;
                let wallet = wallets[index];
                started_prefix += 1;
                running.push(async move {
                    let result = self
                        .validate_logged(wallet, control_tx, paper_state, purpose, deadline)
                        .await;
                    (index, result, Instant::now())
                });
            }
            let Some(result) = running.next().await else {
                break;
            };
            completed.push(result);
        }
        completed.sort_by_key(|(index, _, _)| *index);
        let mut outcomes = ValidationOutcomes {
            started_prefix,
            completed_at: HashMap::new(),
            accepted: Vec::with_capacity(completed.len()),
            deferred: Vec::new(),
            shared: None,
        };
        for (index, result, completed_at) in completed {
            outcomes.completed_at.insert(wallets[index], completed_at);
            match result {
                Ok(install) => outcomes.accepted.push(install),
                Err(error) if error.class() == FailureClass::Shared => {
                    if outcomes.shared.is_none() {
                        outcomes.shared = Some(error);
                    }
                }
                Err(error) => outcomes.deferred.push((wallets[index], error)),
            }
        }
        outcomes
    }
}
