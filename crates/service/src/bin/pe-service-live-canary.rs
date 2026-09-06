#![forbid(unsafe_code)]

use std::env;
use std::fs;
use std::io::Write as _;
use std::os::unix::fs::{FileTypeExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, Result, bail, ensure};
use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    BasisPoints, CollateralAmount, PolymarketConditionId, PolymarketTokenId, Price, Probability,
    RawArtifactObservation, RawEvidence, RawHttpResponse,
};
use pe_execution_core::{
    AttemptAttribution, AttemptOrigin, CampaignAuthorization, CampaignStage, CanaryActor,
    CanaryActorHandle, CanaryAdmission, CanaryCampaignState, CanaryJournal, CanaryQuote,
    CommandReceipt, OrganicStageAuthorization, ProbeAuthorization, organic_evidence_bundle_hash,
    probe_authority_hash, raw_evidence_hash, response_evidence_hash,
};
use pe_resolver_card::{MarketFamily, validate_install_expected};
use pe_risk_engine::{CanaryRiskSnapshot, exposure_bps_ceil};
use pe_service::live_canary::LiveCanaryIo;
use pe_service::organic_canary::{OrganicCandidateSource, OrganicObservation};
use pe_source_polymarket_public::canary::{parse_strict_market, verify_geopolitics_tag};
use pe_source_polymarket_public::{GAMMA_BATCH_LIMIT_PARAM, HttpRequestContext, ReqwestFetcher};
use pe_strategy_winner_follow::{
    OrganicCanaryOrder, OrganicCanaryPolicy, OrganicDecisionProof, organic_decision_proof_hash,
};
use pe_venue_polymarket::{
    CLOB_V2_HOST, CanaryBookSnapshot, CanaryV2Client, CanaryV2Credentials, ClobMarketEvidence,
    ExecutableLadder, SDK_ARCHIVE_SHA256, V2BuyRequest, executable_ladder, parse_book,
    parse_market_evidence,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Semaphore;

const GAMMA_HOST: &str = "https://gamma-api.polymarket.com";
const RECONCILIATION_INTERVAL_SECS: u64 = 30;
const SHUTDOWN_DEADLINE_SECS: u64 = 45;
const SHUTDOWN_WORK_DEADLINE_SECS: u64 = 40;
const SDK_EFFECTIVE_VENDOR_TREE_SHA256: &str =
    "46386f697c128d0245406aa90c00e7cd00e4172d3e4d3f4d9a0836a70fc0ddbb";

fn minimum_fill_price() -> Price {
    Price(Decimal::new(15, 2))
}

fn maximum_fill_price() -> Price {
    Price(Decimal::new(85, 2))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum WireCommand {
    Status,
    ArmProbes {
        command_id: String,
        authorization: CampaignAuthorization,
    },
    Reconcile {
        command_id: String,
    },
    ProbeBuy {
        command_id: String,
        authorization: ProbeAuthorization,
    },
    ReviewProbe {
        command_id: String,
        campaign_id: String,
        ordinal: u8,
    },
    AdvanceOrganic {
        command_id: String,
        authorization: OrganicStageAuthorization,
    },
    Kill {
        command_id: String,
    },
}

impl WireCommand {
    fn command_id(&self) -> Option<&str> {
        match self {
            Self::Status => None,
            Self::ArmProbes { command_id, .. }
            | Self::Reconcile { command_id }
            | Self::ProbeBuy { command_id, .. }
            | Self::ReviewProbe { command_id, .. }
            | Self::AdvanceOrganic { command_id, .. }
            | Self::Kill { command_id } => Some(command_id),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireResponse {
    ok: bool,
    error: Option<String>,
    state: Option<CanaryCampaignState>,
}

#[derive(Clone)]
struct DaemonContext {
    actor: CanaryActorHandle,
    public: Arc<ReqwestFetcher>,
    resolver_dir: PathBuf,
    status_path: PathBuf,
    boot: Arc<CanaryBootIdentity>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanaryBootConfig {
    schema_version: u16,
    implementation_commit: String,
    jurisdiction: String,
    jurisdiction_attestation_hash: String,
    account_attestation_hash: String,
}

impl CanaryBootConfig {
    fn validate(&self) -> Result<()> {
        let jurisdiction = self.jurisdiction.as_bytes();
        ensure!(
            self.schema_version == 1
                && !self.implementation_commit.trim().is_empty()
                && jurisdiction.len() == 2
                && jurisdiction.iter().all(u8::is_ascii_uppercase)
                && !self.jurisdiction_attestation_hash.trim().is_empty()
                && !self.account_attestation_hash.trim().is_empty(),
            "invalid canary boot config"
        );
        Ok(())
    }
}

#[derive(Debug)]
struct CanaryBootIdentity {
    config: CanaryBootConfig,
    config_hash: String,
    binary_hash: String,
    wallet: String,
    owner_signer: String,
    spender: String,
    resolver_dir: PathBuf,
}

#[derive(Serialize)]
struct ArtifactIdentityV1 {
    schema_version: u16,
    implementation_commit: String,
    binary_blake3: String,
    boot_config_blake3: String,
    resolver_inventory_blake3: String,
    sdk_archive_sha256: &'static str,
    sdk_effective_vendor_tree_sha256: &'static str,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        Some("daemon") if args.len() == 1 => run_daemon().await,
        Some("status") if args.len() == 1 => send(WireCommand::Status).await,
        Some("arm-probes") if args.len() == 2 => {
            let authorization = read_json::<CampaignAuthorization>(&args[1])?;
            send(WireCommand::ArmProbes {
                command_id: command_id(),
                authorization,
            })
            .await
        }
        Some("reconcile") if args.len() == 1 => {
            send(WireCommand::Reconcile {
                command_id: command_id(),
            })
            .await
        }
        Some("probe-buy") if args.len() == 2 => {
            let authorization = read_json::<ProbeAuthorization>(&args[1])?;
            send(WireCommand::ProbeBuy {
                command_id: command_id(),
                authorization,
            })
            .await
        }
        Some("review-probe") if args.len() == 3 => {
            let ordinal = args[2].parse().context("review-probe ordinal")?;
            send(WireCommand::ReviewProbe {
                command_id: command_id(),
                campaign_id: args[1].clone(),
                ordinal,
            })
            .await
        }
        Some("advance-organic") if args.len() == 2 => {
            let authorization = read_json::<OrganicStageAuthorization>(&args[1])?;
            send(WireCommand::AdvanceOrganic {
                command_id: command_id(),
                authorization,
            })
            .await
        }
        Some("kill") if args.len() == 1 => {
            send(WireCommand::Kill {
                command_id: command_id(),
            })
            .await
        }
        Some("artifact-identity") if args.len() == 3 => {
            print_artifact_identity(Path::new(&args[1]), Path::new(&args[2]))
        }
        Some("authority-hash") if args.len() == 3 => {
            println!("{}", authority_hash(&args[1], &args[2])?);
            Ok(())
        }
        Some("resolver-card")
            if args.get(1).map(String::as_str) == Some("validate-install") && args.len() == 4 =>
        {
            validate_resolver_install(Path::new(&args[2]), Path::new(&args[3]))
        }
        _ => bail!(
            "usage: pe-service-live-canary daemon|status|artifact-identity BOOT_CONFIG RESOLVER_DIR|authority-hash probe PROBE_AUTHORIZATION.json|authority-hash reviewed-probes REVIEWED_PROBE_HASHES.json|arm-probes FILE|reconcile|probe-buy FILE|review-probe CAMPAIGN ORDINAL|advance-organic FILE|kill|resolver-card validate-install INPUT OUTPUT"
        ),
    }
}

async fn run_daemon() -> Result<()> {
    let state_dir = environment_path("STATE_DIRECTORY", "/var/lib/pe-service-live-canary");
    let runtime_dir = environment_path("RUNTIME_DIRECTORY", "/run/pe-service-live-canary");
    ensure_private_directory(&state_dir)?;
    ensure_private_directory(&runtime_dir)?;
    let credentials_directory = credentials_directory()?;
    let boot_config_bytes = read_credential_bytes(&credentials_directory, "canary-boot-config")?;
    let boot_config: CanaryBootConfig =
        serde_json::from_slice(&boot_config_bytes).context("parse canary boot config")?;
    boot_config.validate()?;
    let credentials = load_credentials()?;
    let supabase_url = read_secret(&credentials_directory, "supabase-url")?;
    let supabase_anon_key = read_secret(&credentials_directory, "supabase-anon-key")?;
    let client = CanaryV2Client::new(credentials, CLOB_V2_HOST)
        .await
        .context("construct pinned Polymarket V2 client")?;
    let resolver_dir = state_dir.join("resolvers");
    let boot = Arc::new(CanaryBootIdentity {
        config: boot_config,
        config_hash: blake3::hash(&boot_config_bytes).to_hex().to_string(),
        binary_hash: blake3::hash(&fs::read(env::current_exe()?)?)
            .to_hex()
            .to_string(),
        wallet: client.deposit_wallet(),
        owner_signer: client.owner_signer(),
        spender: CanaryV2Client::standard_spender().map_err(|error| anyhow::anyhow!(error))?,
        resolver_dir: resolver_dir.clone(),
    });
    let io =
        LiveCanaryIo::new(client, boot.config.jurisdiction.clone()).map_err(anyhow::Error::msg)?;
    let journal_path = state_dir.join("canary.log");
    let state = if journal_path.exists() {
        CanaryJournal::rebuild(&journal_path).context("rebuild canary journal")?
    } else {
        CanaryCampaignState::default()
    };
    ensure_running_campaign_binding(&state, &boot)?;
    let journal = CanaryJournal::open(&journal_path).context("open canary journal")?;
    let (actor, handle) = CanaryActor::new(state, journal, io);
    let actor_task = tokio::spawn(actor.run());
    let context = DaemonContext {
        actor: handle.clone(),
        public: Arc::new(ReqwestFetcher::new(reqwest::Client::new()).with_max_retries(0)),
        resolver_dir,
        status_path: runtime_dir.join("status.json"),
        boot,
    };
    fs::create_dir_all(&context.resolver_dir).context("create resolver directory")?;
    fs::set_permissions(&context.resolver_dir, fs::Permissions::from_mode(0o700))?;
    publish_status(&context.status_path, &handle.status().await?)?;

    let socket_path = runtime_dir.join("control.sock");
    remove_stale_socket(&socket_path)?;
    let listener = UnixListener::bind(&socket_path).context("bind canary control socket")?;
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
    let server = tokio::spawn(serve(listener, context.clone()));
    let organic = tokio::spawn(run_organic_loop(
        context.clone(),
        supabase_url,
        supabase_anon_key,
    ));
    let periodic_handle = handle.clone();
    let periodic_status = context.status_path.clone();
    let periodic = tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(RECONCILIATION_INTERVAL_SECS));
        interval.tick().await;
        loop {
            interval.tick().await;
            let Ok(state) = periodic_handle.status().await else {
                break;
            };
            if matches!(
                state.stage,
                CampaignStage::ProbesArmed
                    | CampaignStage::OrganicReady
                    | CampaignStage::OrganicArmed
                    | CampaignStage::ClosedObserving
            ) && let Ok(state) = periodic_handle.reconcile(None, "periodic".to_owned()).await
            {
                let _ = publish_status(&periodic_status, &state);
            }
        }
    });

    shutdown_signal().await?;
    server.abort();
    periodic.abort();
    organic.abort();
    let work_deadline =
        tokio::time::Instant::now() + std::time::Duration::from_secs(SHUTDOWN_WORK_DEADLINE_SECS);
    let shutdown_deadline = work_deadline
        + std::time::Duration::from_secs(
            SHUTDOWN_DEADLINE_SECS.saturating_sub(SHUTDOWN_WORK_DEADLINE_SECS),
        );
    let shutdown = tokio::time::timeout(
        std::time::Duration::from_secs(SHUTDOWN_DEADLINE_SECS),
        async {
            let observed_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
            handle
                .kill_before(
                    CommandReceipt {
                        command_id: format!("system:shutdown:{observed_at}"),
                        command_hash: blake3::hash(b"signal shutdown").to_hex().to_string(),
                    },
                    work_deadline,
                )
                .await?;
            handle.shutdown_before(work_deadline).await
        },
    )
    .await
    .context("canary shutdown exceeded the 45-second application deadline")??;
    ensure!(
        !shutdown.reconciliation_failed && shutdown.pending.is_none(),
        "canary shutdown requires authenticated recovery"
    );
    publish_status(&context.status_path, &shutdown)?;
    ensure!(
        tokio::time::Instant::now() < shutdown_deadline,
        "terminal status publication exceeded the 45-second application deadline"
    );
    tokio::time::timeout(
        shutdown_deadline.saturating_duration_since(tokio::time::Instant::now()),
        actor_task,
    )
    .await
    .context("canary actor did not exit before the 45-second application deadline")?
    .context("canary actor task failed")?;
    remove_stale_socket(&socket_path)?;
    Ok(())
}

async fn shutdown_signal() -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("install SIGTERM handler")?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.context("install SIGINT handler"),
        signal = terminate.recv() => {
            ensure!(signal.is_some(), "SIGTERM handler closed unexpectedly");
            Ok(())
        }
    }
}

async fn run_organic_loop(context: DaemonContext, supabase_url: String, supabase_anon_key: String) {
    let mut source =
        OrganicCandidateSource::new(context.public.clone(), supabase_url, supabase_anon_key);
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
    loop {
        interval.tick().await;
        let Ok(state) = context.actor.status().await else {
            return;
        };
        if state.stage != CampaignStage::OrganicArmed || state.pending.is_some() {
            continue;
        }
        let observation = match source.next().await {
            Ok(Some(observation)) => observation,
            Ok(None) => {
                let evidence = source.take_idle_evidence();
                let observed_at = OffsetDateTime::now_utc();
                let identity = format!("organic-idle:{}", observed_at.unix_timestamp_nanos());
                let reason = "organic poll produced no candidate".to_owned();
                let receipt = CommandReceipt {
                    command_id: identity.clone(),
                    command_hash: blake3::hash(reason.as_bytes()).to_hex().to_string(),
                };
                if context
                    .actor
                    .record_skip(receipt, identity, reason, evidence)
                    .await
                    .is_err()
                {
                    return;
                }
                continue;
            }
            Err(error) => {
                let observed_at = OffsetDateTime::now_utc();
                let identity = format!("organic-source:{}", observed_at.unix_timestamp_nanos());
                let receipt = CommandReceipt {
                    command_id: identity.clone(),
                    command_hash: blake3::hash(error.reason.as_bytes()).to_hex().to_string(),
                };
                let _ = context
                    .actor
                    .record_skip(receipt, identity, error.to_string(), error.evidence)
                    .await;
                continue;
            }
        };
        let Ok(receipt) = organic_receipt(&observation) else {
            continue;
        };
        let result = if let Some(reason) = &observation.skip_reason {
            context
                .actor
                .record_skip(
                    receipt,
                    observation.identity.clone(),
                    reason.clone(),
                    observation.evidence.clone(),
                )
                .await
        } else {
            let mut evidence = observation.evidence.clone();
            match build_organic(&context, &observation, &mut evidence).await {
                Ok((admission, request)) => {
                    context
                        .actor
                        .dispatch(receipt, admission, request, evidence)
                        .await
                }
                Err(error) => {
                    context
                        .actor
                        .record_skip(
                            receipt,
                            observation.identity.clone(),
                            error.to_string(),
                            evidence,
                        )
                        .await
                }
            }
        };
        if result.is_ok() {
            source.acknowledge(&observation);
        }
    }
}

fn organic_receipt(observation: &OrganicObservation) -> Result<CommandReceipt> {
    let payload = serde_json::to_vec(&(
        &observation.identity,
        &observation.trade,
        &observation.signal,
        &observation.skip_reason,
    ))?;
    let command_hash = blake3::hash(&payload).to_hex().to_string();
    Ok(CommandReceipt {
        command_id: format!("organic:{}", observation.identity),
        command_hash,
    })
}

async fn serve(listener: UnixListener, context: DaemonContext) -> Result<()> {
    let ordinary_command_gate = Arc::new(Semaphore::new(1));
    loop {
        let (stream, _) = listener.accept().await?;
        let context = context.clone();
        let ordinary_command_gate = ordinary_command_gate.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, context, ordinary_command_gate).await {
                tracing::warn!(error = %error, "canary control connection failed");
            }
        });
    }
}

async fn handle_connection(
    stream: UnixStream,
    context: DaemonContext,
    ordinary_command_gate: Arc<Semaphore>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut line = String::new();
    BufReader::new(reader).read_line(&mut line).await?;
    let command: WireCommand = match serde_json::from_str(&line) {
        Ok(command) => command,
        Err(error) => {
            writer
                .write_all(
                    format!(
                        "{{\"ok\":false,\"error\":{}}}\n",
                        serde_json::to_string(&error.to_string())?
                    )
                    .as_bytes(),
                )
                .await?;
            return Ok(());
        }
    };
    let _ordinary_permit = if matches!(command, WireCommand::Kill { .. }) {
        None
    } else {
        Some(
            ordinary_command_gate
                .acquire_owned()
                .await
                .context("ordinary command gate closed")?,
        )
    };
    let response = execute_command(&context, &command).await;
    if let Some(state) = &response.state {
        let _ = publish_status(&context.status_path, state);
    }
    writer.write_all(&serde_json::to_vec(&response)?).await?;
    writer.write_all(b"\n").await?;
    Ok(())
}

async fn execute_command(context: &DaemonContext, command: &WireCommand) -> WireResponse {
    let receipt = match command_receipt(command) {
        Ok(receipt) => receipt,
        Err(error) => {
            return WireResponse {
                ok: false,
                error: Some(error.to_string()),
                state: None,
            };
        }
    };
    if matches!(command, WireCommand::Kill { .. }) {
        let Some(receipt) = receipt else {
            return WireResponse {
                ok: false,
                error: Some("kill command is missing its receipt".to_owned()),
                state: None,
            };
        };
        return match context.actor.kill(receipt).await {
            Ok(()) => WireResponse {
                ok: true,
                error: None,
                state: context.actor.status().await.ok(),
            },
            Err(error) => WireResponse {
                ok: false,
                error: Some(error.to_string()),
                state: None,
            },
        };
    }
    if let Some(receipt) = receipt.as_ref() {
        match context.actor.status().await {
            Ok(state) => match state.command_receipt(&receipt.command_id) {
                Some(existing) if existing.command_hash == receipt.command_hash => {
                    return WireResponse {
                        ok: true,
                        error: None,
                        state: Some(state),
                    };
                }
                Some(_) => {
                    return WireResponse {
                        ok: false,
                        error: Some(
                            "command ID was already used for a different command".to_owned(),
                        ),
                        state: None,
                    };
                }
                None => {}
            },
            Err(error) => {
                return WireResponse {
                    ok: false,
                    error: Some(error.to_string()),
                    state: None,
                };
            }
        }
    }
    if matches!(command, WireCommand::Status) {
        return match context.actor.status().await {
            Ok(state) => WireResponse {
                ok: true,
                error: None,
                state: Some(state),
            },
            Err(error) => WireResponse {
                ok: false,
                error: Some(error.to_string()),
                state: None,
            },
        };
    }
    let Some(receipt) = receipt else {
        return WireResponse {
            ok: false,
            error: Some("state-changing command is missing its receipt".to_owned()),
            state: None,
        };
    };
    let result = match command {
        WireCommand::Status => context.actor.status().await,
        WireCommand::ArmProbes { authorization, .. } => {
            match validate_campaign_environment(authorization, &context.boot) {
                Ok(()) => context.actor.arm(receipt, authorization.clone()).await,
                Err(error) => Err(pe_execution_core::CanaryActorError::Post(error.to_string())),
            }
        }
        WireCommand::Reconcile { .. } => {
            context
                .actor
                .reconcile(Some(receipt), "operator".to_owned())
                .await
        }
        WireCommand::ProbeBuy { authorization, .. } => {
            probe_buy(context, receipt, authorization.clone()).await
        }
        WireCommand::ReviewProbe {
            campaign_id,
            ordinal,
            ..
        } => {
            context
                .actor
                .review_probe(receipt, campaign_id.clone(), *ordinal)
                .await
        }
        WireCommand::AdvanceOrganic { authorization, .. } => match context.actor.status().await {
            Ok(state) => match ensure_running_campaign_binding(&state, &context.boot) {
                Ok(()) => {
                    context
                        .actor
                        .arm_organic(receipt, authorization.clone())
                        .await
                }
                Err(error) => Err(pe_execution_core::CanaryActorError::Post(error.to_string())),
            },
            Err(error) => Err(error),
        },
        WireCommand::Kill { .. } => match context.actor.kill(receipt).await {
            Ok(()) => context.actor.status().await,
            Err(error) => Err(error),
        },
    };
    match result {
        Ok(state) => WireResponse {
            ok: true,
            error: None,
            state: Some(state),
        },
        Err(error) => WireResponse {
            ok: false,
            error: Some(error.to_string()),
            state: None,
        },
    }
}

async fn probe_buy(
    context: &DaemonContext,
    receipt: CommandReceipt,
    authorization: ProbeAuthorization,
) -> std::result::Result<CanaryCampaignState, pe_execution_core::CanaryActorError> {
    let state = context.actor.status().await?;
    let identity = format!(
        "{}:probe:{}",
        authorization.campaign_id, authorization.probe_ordinal
    );
    let mut evidence = Vec::new();
    match build_probe(context, &state, authorization, &mut evidence).await {
        Ok((admission, request)) => {
            context
                .actor
                .dispatch(receipt, admission, request, evidence)
                .await
        }
        Err(error) => {
            context
                .actor
                .record_skip(receipt, identity, error.to_string(), evidence)
                .await
        }
    }
}

enum OriginTerms {
    Probe {
        authorization: ProbeAuthorization,
        family: MarketFamily,
    },
    Organic {
        identity: String,
        signal: LeaderSignal,
        probability: Probability,
        sized: OrganicCanaryOrder,
        token_id: PolymarketTokenId,
        resolver_hash: String,
        family: MarketFamily,
    },
}

struct QuoteInputs {
    market: ClobMarketEvidence,
    book: CanaryBookSnapshot,
    ladder: ExecutableLadder,
    book_evidence_hash: String,
}

async fn build_probe(
    context: &DaemonContext,
    state: &CanaryCampaignState,
    authorization: ProbeAuthorization,
    evidence: &mut Vec<RawEvidence>,
) -> Result<(CanaryAdmission, V2BuyRequest)> {
    ensure!(
        state.stage == CampaignStage::ProbesArmed,
        "probes are not armed"
    );
    ensure!(
        state.campaign_id.as_deref() == Some(&authorization.campaign_id),
        "campaign mismatch"
    );
    ensure!(
        authorization.authority_hash == probe_authority_hash(&authorization)?
            && authorization.maximum_collateral
                == CollateralAmount::from_decimal_exact(
                    authorization.shares.to_decimal() * authorization.worst_price.0,
                )?
            && OffsetDateTime::now_utc() < authorization.expires_at,
        "probe authority hash, debit, or expiry is invalid"
    );
    let resolver_path = context
        .resolver_dir
        .join(format!("{}.json", authorization.condition_id.0));
    let resolver_observed_at = OffsetDateTime::now_utc();
    let resolver_bytes = fs::read(&resolver_path).context("read installed resolver card")?;
    evidence.push(RawEvidence::Artifact(resolver_artifact(
        &resolver_path,
        resolver_bytes,
        resolver_observed_at,
    )));
    let artifact = evidence
        .last()
        .and_then(|evidence| match evidence {
            RawEvidence::Artifact(artifact) => Some(artifact),
            RawEvidence::HttpResponse(_) | RawEvidence::HttpTransportFailure(_) => None,
        })
        .context("resolver artifact capture changed kind")?;
    let resolver = validate_install_expected(
        &artifact.body,
        OffsetDateTime::now_utc(),
        Some(&authorization.resolver_card_hash),
    )
    .context("validate installed resolver card")?;
    ensure!(
        resolver.card.condition_id == authorization.condition_id,
        "resolver condition mismatch"
    );

    let tag_url = format!("{GAMMA_HOST}/tags/100265");
    let market_url = gamma_market_url(&authorization.condition_id);
    let long_url = format!("{CLOB_V2_HOST}/markets/{}", authorization.condition_id.0);
    let short_url = format!(
        "{CLOB_V2_HOST}/clob-markets/{}",
        authorization.condition_id.0
    );
    let tag = fetch_raw(
        &context.public,
        &tag_url,
        gamma_context("gamma-tag"),
        evidence,
    )
    .await?;
    let gamma = fetch_raw(
        &context.public,
        &market_url,
        gamma_context("gamma-market"),
        evidence,
    )
    .await?;
    verify_geopolitics_tag(&tag.body)?;
    let strict = parse_strict_market(
        &gamma.body,
        &authorization.condition_id,
        authorization.outcome_id,
    )?;
    ensure!(
        strict.token_id == authorization.token_id,
        "authority token mapping mismatch"
    );
    let long = fetch_raw(
        &context.public,
        &long_url,
        clob_context("clob-market"),
        evidence,
    )
    .await?;
    let short = fetch_raw(
        &context.public,
        &short_url,
        clob_context("clob-market-compact"),
        evidence,
    )
    .await?;
    let market = parse_market_evidence(
        &long.body,
        &short.body,
        &authorization.condition_id,
        &strict.token_ids,
    )?;
    let state = context
        .actor
        .reconcile(None, "probe_preflight".to_owned())
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let reconciliation = state
        .last_reconciliation
        .as_ref()
        .context("probe reconciliation missing")?;
    let book_url = format!("{CLOB_V2_HOST}/book?token_id={}", authorization.token_id.0);
    let book_response = fetch_raw(
        &context.public,
        &book_url,
        clob_context("clob-book"),
        evidence,
    )
    .await?;
    let book = parse_book(&book_response.body, &market, &authorization.token_id)?;
    let book_evidence_hash = response_evidence_hash(&book_response)?;
    let now = OffsetDateTime::now_utc();
    let now_ms = u64::try_from(now.unix_timestamp_nanos() / 1_000_000)?;
    let ladder = executable_ladder(
        &book,
        now_ms,
        authorization.shares,
        minimum_fill_price(),
        maximum_fill_price(),
        authorization.worst_price,
    )?;
    ensure_running_campaign_binding(&state, &context.boot)?;
    finalize_canary_order(
        &state,
        reconciliation,
        OriginTerms::Probe {
            family: resolver.card.family,
            authorization,
        },
        QuoteInputs {
            market,
            book,
            ladder,
            book_evidence_hash,
        },
        evidence,
    )
}

async fn build_organic(
    context: &DaemonContext,
    observation: &OrganicObservation,
    evidence: &mut Vec<RawEvidence>,
) -> Result<(CanaryAdmission, V2BuyRequest)> {
    let signal = observation
        .signal
        .as_ref()
        .context("organic observation has no eligible signal")?;
    let probability: Probability = observation
        .probability
        .context("organic observation has no calibrated probability")?;
    let condition_id = PolymarketConditionId(signal.market_id.0.0.clone());
    let resolver_path = context
        .resolver_dir
        .join(format!("{}.json", condition_id.0));
    let resolver_observed_at = OffsetDateTime::now_utc();
    let resolver_bytes = fs::read(&resolver_path).context("read installed resolver card")?;
    evidence.push(RawEvidence::Artifact(resolver_artifact(
        &resolver_path,
        resolver_bytes,
        resolver_observed_at,
    )));
    let artifact = evidence
        .last()
        .and_then(|evidence| match evidence {
            RawEvidence::Artifact(artifact) => Some(artifact),
            RawEvidence::HttpResponse(_) | RawEvidence::HttpTransportFailure(_) => None,
        })
        .context("resolver artifact capture changed kind")?;
    let resolver = pe_resolver_card::validate_install(&artifact.body, OffsetDateTime::now_utc())?;
    ensure!(
        resolver.card.condition_id == condition_id,
        "resolver condition mismatch"
    );
    ensure_resolution_horizon(&resolver.card.timing, OffsetDateTime::now_utc())?;

    let tag_url = format!("{GAMMA_HOST}/tags/100265");
    let market_url = gamma_market_url(&condition_id);
    let long_url = format!("{CLOB_V2_HOST}/markets/{}", condition_id.0);
    let short_url = format!("{CLOB_V2_HOST}/clob-markets/{}", condition_id.0);
    let tag = fetch_raw(
        &context.public,
        &tag_url,
        gamma_context("gamma-tag"),
        evidence,
    )
    .await?;
    let gamma = fetch_raw(
        &context.public,
        &market_url,
        gamma_context("gamma-market"),
        evidence,
    )
    .await?;
    verify_geopolitics_tag(&tag.body)?;
    let strict = parse_strict_market(&gamma.body, &condition_id, signal.outcome_id)?;
    let long = fetch_raw(
        &context.public,
        &long_url,
        clob_context("clob-market"),
        evidence,
    )
    .await?;
    let short = fetch_raw(
        &context.public,
        &short_url,
        clob_context("clob-market-compact"),
        evidence,
    )
    .await?;
    let market = parse_market_evidence(&long.body, &short.body, &condition_id, &strict.token_ids)?;
    let sized = OrganicCanaryPolicy.evaluate(
        signal,
        probability,
        context.actor.status().await?.canary_bankroll,
        market.minimum_tick_size,
    )?;
    let state = context
        .actor
        .reconcile(None, "organic_preflight".to_owned())
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    ensure!(
        state.stage == CampaignStage::OrganicArmed,
        "organic admission closed"
    );
    let reconciliation = state
        .last_reconciliation
        .as_ref()
        .context("organic reconciliation missing")?;
    ensure!(
        !reconciliation.positions.iter().any(|position| {
            position.condition_id == condition_id && position.outcome_id == signal.outcome_id
        }),
        "canary wallet already holds the target outcome"
    );
    let book_url = format!("{CLOB_V2_HOST}/book?token_id={}", strict.token_id.0);
    let book_response = fetch_raw(
        &context.public,
        &book_url,
        clob_context("clob-book"),
        evidence,
    )
    .await?;
    let book = parse_book(&book_response.body, &market, &strict.token_id)?;
    let book_evidence_hash = response_evidence_hash(&book_response)?;
    let now = OffsetDateTime::now_utc();
    let now_ms = u64::try_from(now.unix_timestamp_nanos() / 1_000_000)?;
    let ladder = executable_ladder(
        &book,
        now_ms,
        sized.shares,
        minimum_fill_price(),
        maximum_fill_price(),
        sized.kelly_cost,
    )?;
    ensure!(
        ladder.maximum_collateral <= sized.maximum_collateral,
        "executable ladder exceeds the organic Kelly/cap result"
    );
    ensure_running_campaign_binding(&state, &context.boot)?;
    finalize_canary_order(
        &state,
        reconciliation,
        OriginTerms::Organic {
            identity: observation.identity.clone(),
            signal: signal.clone(),
            probability,
            sized,
            token_id: strict.token_id,
            resolver_hash: resolver.canonical_hash.to_hex().to_string(),
            family: resolver.card.family,
        },
        QuoteInputs {
            market,
            book,
            ladder,
            book_evidence_hash,
        },
        evidence,
    )
}

fn finalize_canary_order(
    state: &CanaryCampaignState,
    reconciliation: &pe_execution_core::CanaryReconciliation,
    terms: OriginTerms,
    inputs: QuoteInputs,
    evidence: &[RawEvidence],
) -> Result<(CanaryAdmission, V2BuyRequest)> {
    let QuoteInputs {
        market,
        book,
        ladder,
        book_evidence_hash,
    } = inputs;
    let (token_id, outcome_id, resolver_hash, family) = match &terms {
        OriginTerms::Probe {
            authorization,
            family,
        } => (
            authorization.token_id.clone(),
            authorization.outcome_id,
            authorization.resolver_card_hash.clone(),
            *family,
        ),
        OriginTerms::Organic {
            signal,
            token_id,
            resolver_hash,
            family,
            ..
        } => (
            token_id.clone(),
            signal.outcome_id,
            resolver_hash.clone(),
            *family,
        ),
    };
    ensure!(
        market.token_ids.get(usize::from(outcome_id.0)) == Some(&token_id)
            && book.token_id == token_id,
        "final quote token mapping is inconsistent"
    );
    let metadata_hashes = evidence
        .iter()
        .map(raw_evidence_hash)
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let (
        identity,
        origin,
        neutral_request_hash,
        origin_price_ceiling,
        kelly_cost,
        leader,
        probe_authorization,
        organic_decision_proof,
    ) = match terms {
        OriginTerms::Probe { authorization, .. } => {
            let identity = format!(
                "{}:probe:{}",
                authorization.campaign_id, authorization.probe_ordinal
            );
            (
                identity,
                AttemptOrigin::OperatorProbe,
                probe_authority_hash(&authorization)?,
                authorization.worst_price,
                None,
                None,
                Some(authorization),
                None,
            )
        }
        OriginTerms::Organic {
            identity,
            signal,
            probability,
            sized,
            ..
        } => {
            ensure!(
                identity == sized.intent.idempotency_key,
                "organic identity is not the canonical Winner-Follow idempotency key"
            );
            let proof = OrganicDecisionProof {
                signal: signal.clone(),
                probability,
                idempotency_key: sized.intent.idempotency_key,
                evidence_hashes: metadata_hashes.clone(),
            };
            let request_hash = organic_decision_proof_hash(&proof)?;
            (
                identity,
                AttemptOrigin::Organic,
                request_hash,
                sized.kelly_cost,
                Some(sized.kelly_cost),
                Some(signal.leader.to_string()),
                None,
                Some(proof),
            )
        }
    };
    let attribution = AttemptAttribution {
        leader,
        condition_id: market.condition_id.clone(),
        outcome_id,
        family,
    };
    let exposure = state.exposure_amounts(&attribution)?;
    let leader_exposure_bps = (origin == AttemptOrigin::Organic)
        .then(|| exposure_bps(exposure.leader, state.canary_bankroll))
        .transpose()?;
    let market_exposure_bps = exposure_bps(exposure.market, state.canary_bankroll)?;
    let family_exposure_bps = exposure_bps(exposure.family, state.canary_bankroll)?;
    let total_exposure_bps = exposure_bps(exposure.total, state.canary_bankroll)?;
    let now = OffsetDateTime::now_utc();
    let quote = CanaryQuote {
        origin,
        request_identity: identity.clone(),
        snapshot_raw_hash: book_evidence_hash,
        snapshot_observed_at_ms: book.observed_timestamp_ms,
        condition_id: attribution.condition_id.clone(),
        outcome_id: attribution.outcome_id,
        token_id: token_id.clone(),
        full_ask_ladder_hash: book.raw_hash.to_hex().to_string(),
        best_ask: ladder.best_ask,
        executable_ask: ladder.limit_price,
        origin_price_ceiling,
        kelly_cost,
        minimum_fill_price: minimum_fill_price(),
        maximum_fill_price_exclusive: maximum_fill_price(),
        minimum_tick_size: market.minimum_tick_size,
        minimum_order_size: market.minimum_order_size,
        shares: ladder.shares,
        maximum_collateral: ladder.maximum_collateral,
        metadata_hashes: metadata_hashes.clone(),
        expires_at: now + Duration::seconds(2),
    };
    let admission = CanaryAdmission {
        identity,
        campaign_id: state.campaign_id.clone().context("campaign ID missing")?,
        origin,
        neutral_request_hash,
        resolver_card_hash: resolver_hash,
        quote,
        risk: CanaryRiskSnapshot {
            origin,
            proposed_worst_case_debit: ladder.maximum_collateral,
            canary_bankroll: state.canary_bankroll,
            leader_exposure_bps,
            market_exposure_bps,
            family_exposure_bps,
            total_copy_exposure_bps: total_exposure_bps,
            open_exposure_bps: total_exposure_bps,
            resolver_tradable: true,
            account_state_fresh: true,
            venue_reconciliation_fresh: true,
            geoblock_fresh: true,
            geoblocked: reconciliation.geoblocked,
            closed_only_fresh: true,
            closed_only: reconciliation.closed_only,
            jurisdiction_attestation_valid: true,
            pending_reservation: false,
            allowance: reconciliation.allowance,
            standard_spender_only: reconciliation.standard_spender_only,
        },
        attribution: attribution.clone(),
        probe_authorization,
        organic_decision_proof,
    };
    Ok((
        admission,
        V2BuyRequest {
            condition_id: attribution.condition_id,
            outcome_id: attribution.outcome_id,
            token_id,
            limit_price: ladder.limit_price,
            shares: ladder.shares,
            maximum_collateral: ladder.maximum_collateral,
            tick_size: market.minimum_tick_size,
            metadata_hashes,
        },
    ))
}

fn exposure_bps(amount: CollateralAmount, bankroll: CollateralAmount) -> Result<BasisPoints> {
    exposure_bps_ceil(amount, bankroll).context("canary exposure denominator is invalid")
}

fn resolver_artifact(
    path: &Path,
    body: Vec<u8>,
    observed_at: OffsetDateTime,
) -> RawArtifactObservation {
    RawArtifactObservation {
        source_id: "resolver-card".to_owned(),
        artifact_kind: "resolver-card".to_owned(),
        path: path.display().to_string(),
        body,
        observed_at,
        received_at: OffsetDateTime::now_utc(),
        schema_version: 1,
        parser_version: 1,
        adapter_version: env!("CARGO_PKG_VERSION").to_owned(),
    }
}

fn ensure_resolution_horizon(
    timing: &pe_resolver_card::TimingRule,
    now: OffsetDateTime,
) -> Result<()> {
    let resolution = match timing {
        pe_resolver_card::TimingRule::PointInTime(timestamp)
        | pe_resolver_card::TimingRule::OnFirstPublicationAfter(timestamp) => timestamp.0,
        pe_resolver_card::TimingRule::Window(window) => window.end.0,
        pe_resolver_card::TimingRule::BusinessDayClose { .. }
        | pe_resolver_card::TimingRule::OnNthOccurrence { .. } => {
            bail!("resolver timing cannot prove the canonical resolution horizon")
        }
    };
    let seconds = (resolution - now).whole_seconds();
    ensure!(
        (60..=172_800).contains(&seconds),
        "resolver timing is outside the canonical 60-second to 48-hour horizon"
    );
    Ok(())
}

async fn fetch_raw(
    fetcher: &ReqwestFetcher,
    url: &str,
    context: HttpRequestContext,
    evidence: &mut Vec<RawEvidence>,
) -> Result<RawHttpResponse> {
    let mut attempts = Vec::new();
    let result = fetcher
        .fetch_page_observed(url, context, |attempt| {
            attempts.push(RawEvidence::from(attempt));
            Ok(())
        })
        .await;
    let mut last_response = None;
    for observation in attempts {
        if let RawEvidence::HttpResponse(response) = &observation {
            last_response = Some(response.clone());
        }
        evidence.push(observation);
    }
    result.map_err(|error| anyhow::anyhow!(error.to_string()))?;
    last_response.context("public fetch produced no HTTP response")
}

const fn gamma_context(endpoint_kind: &'static str) -> HttpRequestContext {
    HttpRequestContext {
        source_id: "polymarket-gamma",
        endpoint_kind,
    }
}

const fn clob_context(endpoint_kind: &'static str) -> HttpRequestContext {
    HttpRequestContext {
        source_id: "polymarket-clob-public",
        endpoint_kind,
    }
}

async fn send(command: WireCommand) -> Result<()> {
    let socket =
        environment_path("RUNTIME_DIRECTORY", "/run/pe-service-live-canary").join("control.sock");
    let mut stream = UnixStream::connect(&socket)
        .await
        .with_context(|| format!("connect {}", socket.display()))?;
    stream.write_all(&serde_json::to_vec(&command)?).await?;
    stream.write_all(b"\n").await?;
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).await?;
    let parsed: WireResponse = serde_json::from_str(&response)?;
    println!("{}", serde_json::to_string_pretty(&parsed)?);
    ensure!(
        parsed.ok,
        "{}",
        parsed.error.unwrap_or_else(|| "command failed".to_owned())
    );
    Ok(())
}

fn load_credentials() -> Result<CanaryV2Credentials> {
    let directory = credentials_directory()?;
    Ok(CanaryV2Credentials {
        private_key: read_secret(&directory, "polymarket-private-key")?,
        api_key: read_secret(&directory, "polymarket-api-key")?,
        api_secret: read_secret(&directory, "polymarket-api-secret")?,
        api_passphrase: read_secret(&directory, "polymarket-api-passphrase")?,
        deposit_wallet: read_secret(&directory, "polymarket-deposit-wallet")?,
    })
}

fn credentials_directory() -> Result<PathBuf> {
    env::var_os("CREDENTIALS_DIRECTORY")
        .map(PathBuf::from)
        .context(
            "CREDENTIALS_DIRECTORY is required; the inactive unit must not use environment secrets",
        )
}

fn read_secret(directory: &Path, name: &str) -> Result<String> {
    let value = String::from_utf8(read_credential_bytes(directory, name)?)?
        .trim()
        .to_owned();
    ensure!(!value.is_empty(), "credential {name} is empty");
    Ok(value)
}

fn read_credential_bytes(directory: &Path, name: &str) -> Result<Vec<u8>> {
    let path = directory.join(name);
    let metadata = fs::symlink_metadata(&path)?;
    ensure!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "credential {name} must be a regular file"
    );
    fs::read(path).map_err(Into::into)
}

fn validate_campaign_environment(
    authorization: &CampaignAuthorization,
    boot: &CanaryBootIdentity,
) -> Result<()> {
    ensure!(
        authorization.implementation_commit == boot.config.implementation_commit
            && authorization.binary_hash == boot.binary_hash
            && authorization.config_hash == boot.config_hash
            && authorization.resolver_inventory_hash
                == resolver_inventory_hash(&boot.resolver_dir)?
            && authorization.sdk_archive_sha256 == SDK_ARCHIVE_SHA256
            && authorization.sdk_effective_vendor_tree_sha256 == SDK_EFFECTIVE_VENDOR_TREE_SHA256
            && authorization.wallet.eq_ignore_ascii_case(&boot.wallet)
            && authorization
                .owner_signer
                .eq_ignore_ascii_case(&boot.owner_signer)
            && authorization.spender.eq_ignore_ascii_case(&boot.spender)
            && authorization.jurisdiction == boot.config.jurisdiction
            && authorization.jurisdiction_attestation_hash
                == boot.config.jurisdiction_attestation_hash
            && authorization.account_attestation_hash == boot.config.account_attestation_hash,
        "campaign authority does not bind the running canary artifacts"
    );
    Ok(())
}

fn ensure_running_campaign_binding(
    state: &CanaryCampaignState,
    boot: &CanaryBootIdentity,
) -> Result<()> {
    if state.campaign_id.is_none() {
        return Ok(());
    }
    let current_resolver_hash = resolver_inventory_identity_hash(&boot.resolver_dir)
        .context("recompute the journaled campaign resolver inventory identity")?;
    ensure!(
        state.implementation_commit.as_deref() == Some(&boot.config.implementation_commit)
            && state.binary_hash.as_deref() == Some(&boot.binary_hash)
            && state.config_hash.as_deref() == Some(&boot.config_hash)
            && state.resolver_inventory_hash.as_deref() == Some(&current_resolver_hash)
            && state.sdk_archive_sha256.as_deref() == Some(SDK_ARCHIVE_SHA256)
            && state.sdk_effective_vendor_tree_sha256.as_deref()
                == Some(SDK_EFFECTIVE_VENDOR_TREE_SHA256)
            && state
                .wallet
                .as_deref()
                .is_some_and(|wallet| wallet.eq_ignore_ascii_case(&boot.wallet))
            && state
                .owner_signer
                .as_deref()
                .is_some_and(|owner| owner.eq_ignore_ascii_case(&boot.owner_signer))
            && state
                .spender
                .as_deref()
                .is_some_and(|spender| spender.eq_ignore_ascii_case(&boot.spender)),
        "journaled campaign does not bind the running binary, config, credentials, spender, SDK, or resolver inventory; restore the reviewed artifacts before recovery"
    );
    Ok(())
}

fn resolver_inventory_hash(directory: &Path) -> Result<String> {
    resolver_inventory_hash_inner(directory, true)
}

fn resolver_inventory_identity_hash(directory: &Path) -> Result<String> {
    resolver_inventory_hash_inner(directory, false)
}

fn resolver_inventory_hash_inner(
    directory: &Path,
    require_current_tradability: bool,
) -> Result<String> {
    let mut entries = fs::read_dir(directory)?.collect::<std::result::Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    let mut inventory = Vec::new();
    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        ensure!(
            metadata.file_type().is_file()
                && !metadata.file_type().is_symlink()
                && path
                    .extension()
                    .is_some_and(|extension| extension == "json"),
            "resolver inventory contains an unsupported entry"
        );
        let bytes = fs::read(&path)?;
        if require_current_tradability {
            pe_resolver_card::validate_install(&bytes, OffsetDateTime::now_utc())?;
        }
        inventory.push((
            entry.file_name().to_string_lossy().into_owned(),
            blake3::hash(&bytes).to_hex().to_string(),
        ));
    }
    ensure!(!inventory.is_empty(), "resolver inventory is empty");
    Ok(blake3::hash(&serde_json::to_vec(&inventory)?)
        .to_hex()
        .to_string())
}

fn print_artifact_identity(boot_config_path: &Path, resolver_dir: &Path) -> Result<()> {
    let boot_config_bytes = fs::read(boot_config_path).context("read canary boot config")?;
    let config: CanaryBootConfig =
        serde_json::from_slice(&boot_config_bytes).context("parse canary boot config")?;
    config.validate()?;
    let identity = ArtifactIdentityV1 {
        schema_version: 1,
        implementation_commit: config.implementation_commit,
        binary_blake3: blake3::hash(&fs::read(env::current_exe()?)?)
            .to_hex()
            .to_string(),
        boot_config_blake3: blake3::hash(&boot_config_bytes).to_hex().to_string(),
        resolver_inventory_blake3: resolver_inventory_hash(resolver_dir)?,
        sdk_archive_sha256: SDK_ARCHIVE_SHA256,
        sdk_effective_vendor_tree_sha256: SDK_EFFECTIVE_VENDOR_TREE_SHA256,
    };
    println!("{}", serde_json::to_string_pretty(&identity)?);
    Ok(())
}

fn validate_resolver_install(input: &Path, output: &Path) -> Result<()> {
    let bytes = fs::read(input)?;
    let validated = pe_resolver_card::validate_install(&bytes, OffsetDateTime::now_utc())?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(output)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    println!("{}", validated.canonical_hash.to_hex());
    Ok(())
}

fn publish_status(path: &Path, state: &CanaryCampaignState) -> Result<()> {
    let temporary = path.with_extension("tmp");
    let bytes = serde_json::to_vec_pretty(state)?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn remove_stale_socket(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            fs::remove_file(path).map_err(Into::into)
        }
        Ok(_) => bail!("refusing to replace non-socket {}", path.display()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn environment_path(name: &str, default: &str) -> PathBuf {
    env::var_os(name).map_or_else(|| PathBuf::from(default), PathBuf::from)
}

fn read_json<T: serde::de::DeserializeOwned>(path: &str) -> Result<T> {
    serde_json::from_slice(&fs::read(path)?).with_context(|| format!("parse {path}"))
}

fn authority_hash(kind: &str, path: &str) -> Result<String> {
    match kind {
        "probe" => {
            probe_authority_hash(&read_json::<ProbeAuthorization>(path)?).map_err(Into::into)
        }
        "reviewed-probes" => {
            organic_evidence_bundle_hash(&read_json::<Vec<String>>(path)?).map_err(Into::into)
        }
        _ => bail!("unknown authority-hash form: {kind}"),
    }
}

fn gamma_market_url(condition_id: &PolymarketConditionId) -> String {
    format!(
        "{GAMMA_HOST}/markets?condition_ids={}&limit={GAMMA_BATCH_LIMIT_PARAM}&include_tag=true",
        condition_id.0
    )
}

fn command_id() -> String {
    if let Ok(command_id) = env::var("PE_CANARY_COMMAND_ID")
        && !command_id.trim().is_empty()
    {
        return command_id;
    }
    format!(
        "{}-{}",
        std::process::id(),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    )
}

fn command_receipt(command: &WireCommand) -> Result<Option<CommandReceipt>> {
    let Some(command_id) = command.command_id() else {
        return Ok(None);
    };
    ensure!(!command_id.trim().is_empty(), "command ID cannot be empty");
    let mut value = serde_json::to_value(command)?;
    let object = value
        .as_object_mut()
        .context("wire command must serialize as an object")?;
    object.remove("command_id");
    let command_hash = blake3::hash(&serde_json::to_vec(&value)?)
        .to_hex()
        .to_string();
    Ok(Some(CommandReceipt {
        command_id: command_id.to_owned(),
        command_hash,
    }))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use tempfile::tempdir;

    use super::*;

    fn boot_config(jurisdiction: &str) -> CanaryBootConfig {
        CanaryBootConfig {
            schema_version: 1,
            implementation_commit: "current-commit".to_owned(),
            jurisdiction: jurisdiction.to_owned(),
            jurisdiction_attestation_hash: "jurisdiction".to_owned(),
            account_attestation_hash: "account".to_owned(),
        }
    }

    #[test]
    fn boot_config_requires_an_uppercase_iso_country_code() {
        assert!(boot_config("IE").validate().is_ok());
        for jurisdiction in ["Ireland", "ie", "I", ""] {
            assert!(boot_config(jurisdiction).validate().is_err());
        }
    }

    #[test]
    fn expired_resolver_bytes_remain_available_for_recovery_identity() {
        let directory = tempdir().unwrap();
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "card_id": "00000000-0000-0000-0000-000000000000",
            "condition_id": "0x01",
            "family": "event_feed",
            "resolver_source": {"kind": "official_api", "value": "https://example.invalid/feed"},
            "upstream_sources": [],
            "output_space": {"kind": "binary"},
            "timing": {"kind": "point_in_time", "value": "2026-07-01T00:00:00Z"},
            "rounding": {"kind": "as_published"},
            "tie_rule": "source_defined",
            "finality": {"kind": "as_published"},
            "revision_policy": "accept_only_official_corrections",
            "status": "tradable",
            "valid_from": "2026-06-01T00:00:00Z",
            "valid_until": "2026-07-01T00:00:00Z"
        }))
        .unwrap();
        fs::write(directory.path().join("expired.json"), &bytes).unwrap();

        assert!(resolver_inventory_identity_hash(directory.path()).is_ok());
        assert!(resolver_inventory_hash(directory.path()).is_err());
    }

    #[test]
    fn running_binary_rejects_a_journal_bound_to_the_legacy_artifact() {
        let directory = tempdir().unwrap();
        fs::write(directory.path().join("resolver.json"), b"{}").unwrap();
        let resolver_hash = resolver_inventory_identity_hash(directory.path()).unwrap();
        let boot = CanaryBootIdentity {
            config: boot_config("US"),
            config_hash: "config".to_owned(),
            binary_hash: "current-binary".to_owned(),
            wallet: "wallet".to_owned(),
            owner_signer: "owner".to_owned(),
            spender: "spender".to_owned(),
            resolver_dir: directory.path().to_path_buf(),
        };
        let state = CanaryCampaignState {
            campaign_id: Some("legacy-campaign".to_owned()),
            implementation_commit: Some("current-commit".to_owned()),
            binary_hash: Some("legacy-binary".to_owned()),
            config_hash: Some("config".to_owned()),
            resolver_inventory_hash: Some(resolver_hash),
            sdk_archive_sha256: Some(SDK_ARCHIVE_SHA256.to_owned()),
            sdk_effective_vendor_tree_sha256: Some(SDK_EFFECTIVE_VENDOR_TREE_SHA256.to_owned()),
            wallet: Some("wallet".to_owned()),
            owner_signer: Some("owner".to_owned()),
            spender: Some("spender".to_owned()),
            ..CanaryCampaignState::default()
        };

        assert!(ensure_running_campaign_binding(&state, &boot).is_err());
    }

    #[test]
    fn gamma_market_query_requests_direct_tags() {
        let condition = PolymarketConditionId("0xcondition".to_owned());

        assert_eq!(
            gamma_market_url(&condition),
            "https://gamma-api.polymarket.com/markets?condition_ids=0xcondition&limit={GAMMA_BATCH_LIMIT_PARAM}&include_tag=true"
        );
    }
}
