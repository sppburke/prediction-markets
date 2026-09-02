//! Exact Polymarket public-activity normalization and source identity (#544).
//!
//! One parser owns the Data API `/activity` row shape for REST, websocket
//! observations, bootstrap, restart, and replay. Complete fixed-end REST
//! responses may additionally be aggregated with [`aggregate_activity_rows`].

use std::collections::HashMap;
use std::fmt;

use pe_core_types::{
    CollateralAmount, ContractQty, OutcomeId, PolymarketConditionId, PolymarketTokenId, Price,
    ReceivedAt, ShareAmount, Side, SourceId, SourceTimestamp, SourceTradeId, WalletAddress,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

/// Version of the normalized public-activity schema introduced by #544.
pub const ACTIVITY_SCHEMA_VERSION: u32 = 2;
/// Version of the source-owned public-activity parser introduced by #544.
pub const ACTIVITY_PARSER_VERSION: u32 = 2;

const ACTIVITY_GROUP_DOMAIN: &[u8] =
    b"prediction-edge/source-polymarket-public/activity-group/v2\0";
const ACTIVITY_REVISION_DOMAIN: &[u8] =
    b"prediction-edge/source-polymarket-public/activity-semantic-revision/v2\0";

/// Every activity type documented by the Polymarket public Data API, plus a
/// typed unknown variant that must fence the affected wallet (#544).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ActivityType {
    Trade,
    Split,
    Merge,
    Redeem,
    Reward,
    Conversion,
    Deposit,
    Withdrawal,
    Yield,
    MakerRebate,
    TakerRebate,
    ReferralReward,
    Unknown(String),
}

impl ActivityType {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Trade => "TRADE",
            Self::Split => "SPLIT",
            Self::Merge => "MERGE",
            Self::Redeem => "REDEEM",
            Self::Reward => "REWARD",
            Self::Conversion => "CONVERSION",
            Self::Deposit => "DEPOSIT",
            Self::Withdrawal => "WITHDRAWAL",
            Self::Yield => "YIELD",
            Self::MakerRebate => "MAKER_REBATE",
            Self::TakerRebate => "TAKER_REBATE",
            Self::ReferralReward => "REFERRAL_REWARD",
            Self::Unknown(value) => value,
        }
    }

    #[must_use]
    pub fn is_position_changing(&self) -> bool {
        matches!(
            self,
            Self::Trade | Self::Split | Self::Merge | Self::Redeem | Self::Conversion
        )
    }

    #[must_use]
    pub fn is_raw_only(&self) -> bool {
        matches!(
            self,
            Self::Reward
                | Self::Deposit
                | Self::Withdrawal
                | Self::Yield
                | Self::MakerRebate
                | Self::TakerRebate
                | Self::ReferralReward
        )
    }

    #[must_use]
    pub fn requires_wallet_fence(&self) -> bool {
        matches!(self, Self::Conversion | Self::Unknown(_))
    }
}

impl From<String> for ActivityType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "TRADE" => Self::Trade,
            "SPLIT" => Self::Split,
            "MERGE" => Self::Merge,
            "REDEEM" => Self::Redeem,
            "REWARD" => Self::Reward,
            "CONVERSION" => Self::Conversion,
            "DEPOSIT" => Self::Deposit,
            "WITHDRAWAL" => Self::Withdrawal,
            "YIELD" => Self::Yield,
            "MAKER_REBATE" => Self::MakerRebate,
            "TAKER_REBATE" => Self::TakerRebate,
            "REFERRAL_REWARD" => Self::ReferralReward,
            _ => Self::Unknown(value),
        }
    }
}

impl Serialize for ActivityType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ActivityType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Self::from)
    }
}

/// Transport provenance retained beside normalized semantics and excluded from
/// the semantic revision hash (#544).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityTransport {
    Rest,
    ActivityWebsocket,
    Replay,
}

/// Envelope-owned metadata supplied to the pure activity parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityParseContext {
    pub source_id: SourceId,
    pub observed_at: SourceTimestamp,
    pub received_at: ReceivedAt,
    pub transport: ActivityTransport,
}

/// Canonical components stored beside every version-two group key (#544).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceActivityGroupComponents {
    pub activity_type: ActivityType,
    pub wallet: WalletAddress,
    pub transaction_hash: String,
    pub condition_id: Option<PolymarketConditionId>,
    pub asset: Option<PolymarketTokenId>,
    pub outcome: Option<OutcomeId>,
    pub side: Option<Side>,
}

/// Persisted version-two activity identity. Deserialization rederives and
/// verifies `key`, so stored components cannot be altered independently.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct SourceActivityGroupId {
    key: SourceTradeId,
    components: SourceActivityGroupComponents,
}

impl SourceActivityGroupId {
    /// Derive `g2:` plus lowercase BLAKE3 from the canonical components.
    pub fn derive(
        components: SourceActivityGroupComponents,
    ) -> Result<Self, ActivityIdentityError> {
        let canonical = canonical_group_components(&components)?;
        let key = SourceTradeId(format!("g2:{}", blake3::hash(&canonical).to_hex()));
        Ok(Self { key, components })
    }

    #[must_use]
    pub fn key(&self) -> &SourceTradeId {
        &self.key
    }

    #[must_use]
    pub fn components(&self) -> &SourceActivityGroupComponents {
        &self.components
    }

    /// Rederive the key from the stored components and reject a mismatch.
    pub fn verify_components(&self) -> Result<(), ActivityIdentityError> {
        let derived = Self::derive(self.components.clone())?;
        if derived.key == self.key {
            return Ok(());
        }
        Err(ActivityIdentityError::ComponentMismatch {
            stored: self.key.0.clone(),
            derived: derived.key.0,
        })
    }
}

impl fmt::Display for SourceActivityGroupId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.key, formatter)
    }
}

impl<'de> Deserialize<'de> for SourceActivityGroupId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct StoredGroupId {
            key: SourceTradeId,
            components: SourceActivityGroupComponents,
        }

        let stored = StoredGroupId::deserialize(deserializer)?;
        let group_id = Self {
            key: stored.key,
            components: stored.components,
        };
        group_id
            .verify_components()
            .map_err(serde::de::Error::custom)?;
        Ok(group_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ActivityIdentityError {
    #[error("canonical activity component is too long")]
    ComponentTooLong,
    #[error("stored activity group key {stored} does not match components (derived {derived})")]
    ComponentMismatch { stored: String, derived: String },
}

/// One fully normalized source row. Local provenance remains linked here but
/// is deliberately excluded from semantic revision identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedActivity {
    pub activity_type: ActivityType,
    pub source_id: SourceId,
    pub wallet: WalletAddress,
    pub transaction_hash: String,
    pub condition_id: Option<PolymarketConditionId>,
    pub asset: Option<PolymarketTokenId>,
    pub outcome: Option<OutcomeId>,
    pub side: Option<Side>,
    pub price: Price,
    pub share_amount: ShareAmount,
    pub source_usdc_amount: CollateralAmount,
    pub is_combo: bool,
    pub source_time: SourceTimestamp,
    pub observed_at: SourceTimestamp,
    pub received_at: ReceivedAt,
    pub raw_row_hash: String,
    pub raw_row_json: String,
    pub parser_version: u32,
    pub schema_version: u32,
    pub transport: ActivityTransport,
}

/// Identity carried by one accepted `activity/trades` websocket observation.
///
/// The websocket payload is only a reconciliation trigger: it cannot supply a
/// semantic aggregate or mutate the ledger. Its documented envelope fixes the
/// activity type to `TRADE`, while the payload supplies the remaining version-two
/// group components and exact source second (#544).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityTradeObservation {
    pub wallet: WalletAddress,
    pub group_id: SourceActivityGroupId,
    pub source_time: SourceTimestamp,
}

impl NormalizedActivity {
    pub fn group_id(&self) -> Result<SourceActivityGroupId, ActivityIdentityError> {
        SourceActivityGroupId::derive(SourceActivityGroupComponents {
            activity_type: self.activity_type.clone(),
            wallet: self.wallet,
            transaction_hash: self.transaction_hash.clone(),
            condition_id: self.condition_id.clone(),
            asset: self.asset.clone(),
            outcome: self.outcome,
            side: self.side,
        })
    }

    /// Return the canonical bytes used as this row's multiset member in a
    /// semantic revision hash (#544).
    pub fn semantic_row_encoding(&self) -> Result<Vec<u8>, ActivityIdentityError> {
        semantic_row_encoding(self)
    }

    #[must_use]
    pub fn requires_wallet_fence(&self) -> bool {
        // A zero-share CONVERSION has an arithmetically zero effect and does
        // not fence; an Unknown type fences regardless of size — its semantics
        // cannot be trusted to make "zero" meaningful (#544 activation fix 2).
        match &self.activity_type {
            ActivityType::Conversion => self.share_amount != ShareAmount::ZERO,
            other => other.requires_wallet_fence(),
        }
    }

    /// Ordinary lookup/ranking may see only non-combo, supported mutations
    /// with a nonzero effect — zero-share rows are venue artifacts retained as
    /// raw evidence only (#544 activation fix 2).
    #[must_use]
    pub fn is_ordinary_position_change(&self) -> bool {
        self.share_amount != ShareAmount::ZERO
            && self.activity_type.is_position_changing()
            && !self.activity_type.requires_wallet_fence()
            && !self.is_combo
    }
}

/// A complete response whose every row carried the requested wallet identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedActivityWindow {
    pub requested_wallet: WalletAddress,
    pub rows: Vec<NormalizedActivity>,
}

impl NormalizedActivityWindow {
    pub fn aggregates(&self) -> Result<Vec<ActivityAggregate>, ActivityAggregationError> {
        aggregate_activity_rows(&self.rows)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ActivityWindowInvalidation {
    #[error("activity row {row_index} omitted proxyWallet")]
    MissingWallet { row_index: usize },
    #[error("activity row {row_index} has invalid proxyWallet {value:?}")]
    InvalidWallet { row_index: usize, value: String },
    #[error(
        "activity row {row_index} wallet {payload_wallet} does not match requested wallet {requested_wallet}"
    )]
    WalletMismatch {
        row_index: usize,
        requested_wallet: WalletAddress,
        payload_wallet: WalletAddress,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ActivityValidationError {
    #[error("activity/trades payload has invalid activity type {value:?}")]
    InvalidActivityType { value: String },
    #[error("activity/trades payload identity could not be derived")]
    Identity,
    #[error("activity row omitted {field}")]
    MissingField { field: &'static str },
    #[error("activity row has empty {field}")]
    EmptyField { field: &'static str },
    #[error("activity row has invalid side {value:?}")]
    InvalidSide { value: String },
    #[error("activity row has invalid price {value}: {reason}")]
    InvalidPrice { value: Decimal, reason: String },
    #[error("activity row has invalid share amount {value}: {reason}")]
    InvalidShareAmount { value: Decimal, reason: String },
    #[error("activity row has invalid collateral amount {value}: {reason}")]
    InvalidCollateralAmount { value: Decimal, reason: String },
    #[error("activity row has invalid condition/outcome mapping")]
    InvalidConditionOutcomeMapping,
    #[error("activity timestamp {0} is outside the supported range")]
    InvalidTimestamp(i64),
    #[error("activity timestamp {value:?} is not an epoch integer")]
    InvalidTimestampValue { value: String },
    #[error("legacy v1 whole-contract projection is out of range")]
    LegacyProjectionOutOfRange,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ActivityParseError {
    #[error("activity response json: {message}")]
    Json { message: String },
    #[error("activity window invalidated: {0}")]
    WindowInvalidated(ActivityWindowInvalidation),
    #[error("activity row {row_index}: {source}")]
    InvalidRow {
        row_index: usize,
        source: ActivityValidationError,
    },
}

/// Parse one complete REST response. A missing, invalid, or mismatched payload
/// wallet invalidates the whole response and yields no partial window.
pub fn parse_activity_response(
    raw: &[u8],
    requested_wallet: WalletAddress,
    context: &ActivityParseContext,
) -> Result<NormalizedActivityWindow, ActivityParseError> {
    let raw_rows: Vec<Box<RawValue>> =
        serde_json::from_slice(raw).map_err(|error| ActivityParseError::Json {
            message: error.to_string(),
        })?;
    let mut rows = Vec::with_capacity(raw_rows.len());
    for (row_index, raw_row) in raw_rows.into_iter().enumerate() {
        rows.push(parse_row(
            raw_row.get(),
            Some(requested_wallet),
            context,
            row_index,
        )?);
    }
    Ok(NormalizedActivityWindow {
        requested_wallet,
        rows,
    })
}

/// Parse one raw row. `requested_wallet = None` is for platform-wide websocket
/// observations; REST reconciliation must use [`parse_activity_response`].
pub fn parse_activity_row(
    raw: &[u8],
    requested_wallet: Option<WalletAddress>,
    context: &ActivityParseContext,
) -> Result<NormalizedActivity, ActivityParseError> {
    let raw_row: Box<RawValue> =
        serde_json::from_slice(raw).map_err(|error| ActivityParseError::Json {
            message: error.to_string(),
        })?;
    parse_row(raw_row.get(), requested_wallet, context, 0)
}

/// Parse one payload accepted under the `activity/trades` websocket envelope.
///
/// Unlike a complete REST row, the push payload does not carry `type` or
/// `usdcSize`. Those fields are not invented: the envelope supplies only the
/// `TRADE` domain, and the observation remains raw evidence plus a group trigger.
/// Exact aggregate quantities and prices come exclusively from fixed-end REST
/// reconciliation.
pub fn parse_activity_trade_observation(
    raw: &[u8],
) -> Result<ActivityTradeObservation, ActivityParseError> {
    let raw_row: Box<RawValue> =
        serde_json::from_slice(raw).map_err(|error| ActivityParseError::Json {
            message: error.to_string(),
        })?;
    let mut raw: RawActivity =
        serde_json::from_str(raw_row.get()).map_err(|error| ActivityParseError::Json {
            message: format!("row 0: {error}"),
        })?;
    let wallet = parse_wallet(raw.proxy_wallet.take(), None, 0)?;
    let observation = normalize_trade_observation(raw, wallet).map_err(|source| {
        ActivityParseError::InvalidRow {
            row_index: 0,
            source,
        }
    })?;
    Ok(observation)
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FlexibleI64 {
    Number(i64),
    String(String),
}

impl FlexibleI64 {
    fn parse(self) -> Result<i64, ActivityValidationError> {
        match self {
            Self::Number(value) => Ok(value),
            Self::String(value) => value.trim().parse::<i64>().map_err(|_| {
                ActivityValidationError::InvalidTimestampValue {
                    value: value.clone(),
                }
            }),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FlexibleU16 {
    Number(u16),
    String(String),
}

impl FlexibleU16 {
    fn parse(self) -> Result<u16, ActivityValidationError> {
        match self {
            Self::Number(value) => Ok(value),
            Self::String(value) => value
                .trim()
                .parse::<u16>()
                .map_err(|_| ActivityValidationError::InvalidConditionOutcomeMapping),
        }
    }
}

/// Exact-lexeme decimal capture (#544 review): plain serde routes fractional JSON
/// numbers through `f64`, silently rounding excess precision (for example
/// `10000000000000.000001` loses its final atomic share). Capturing the raw token
/// and parsing it with `Decimal::from_str_exact` preserves every digit; scientific
/// notation and unrepresentable magnitudes reject with a typed error instead of
/// rounding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExactDecimal(Decimal);

impl<'de> Deserialize<'de> for ExactDecimal {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw: Box<serde_json::value::RawValue> = Box::deserialize(deserializer)?;
        let lexeme = raw.get().trim();
        let unquoted = lexeme
            .strip_prefix('"')
            .and_then(|rest| rest.strip_suffix('"'))
            .unwrap_or(lexeme);
        Decimal::from_str_exact(unquoted)
            .map(ExactDecimal)
            .map_err(|error| {
                serde::de::Error::custom(format_args!(
                    "numeric token {lexeme} is not an exactly representable decimal: {error}"
                ))
            })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawActivity {
    #[serde(default, alias = "proxy_wallet")]
    proxy_wallet: Option<String>,
    #[serde(default)]
    timestamp: Option<FlexibleI64>,
    #[serde(default, rename = "type", alias = "activity_type")]
    activity_type: Option<String>,
    #[serde(default, alias = "transaction_hash")]
    transaction_hash: Option<String>,
    #[serde(default, alias = "condition_id")]
    condition_id: Option<String>,
    #[serde(default)]
    asset: Option<String>,
    #[serde(default, alias = "outcome_index")]
    outcome_index: Option<FlexibleU16>,
    #[serde(default)]
    outcome: Option<String>,
    #[serde(default)]
    side: Option<String>,
    #[serde(default)]
    price: Option<ExactDecimal>,
    #[serde(default)]
    size: Option<ExactDecimal>,
    #[serde(default, alias = "usdc_size")]
    usdc_size: Option<ExactDecimal>,
    #[serde(default, alias = "is_combo")]
    is_combo: Option<bool>,
}

fn parse_row(
    raw_row: &str,
    requested_wallet: Option<WalletAddress>,
    context: &ActivityParseContext,
    row_index: usize,
) -> Result<NormalizedActivity, ActivityParseError> {
    let mut raw: RawActivity =
        serde_json::from_str(raw_row).map_err(|error| ActivityParseError::Json {
            message: format!("row {row_index}: {error}"),
        })?;
    let wallet = parse_wallet(raw.proxy_wallet.take(), requested_wallet, row_index)?;
    let parsed = normalize_row(raw, wallet, context, raw_row)
        .map_err(|source| ActivityParseError::InvalidRow { row_index, source })?;
    Ok(parsed)
}

fn parse_wallet(
    raw_wallet: Option<String>,
    requested_wallet: Option<WalletAddress>,
    row_index: usize,
) -> Result<WalletAddress, ActivityParseError> {
    let value = raw_wallet.filter(|value| !value.trim().is_empty()).ok_or({
        ActivityParseError::WindowInvalidated(ActivityWindowInvalidation::MissingWallet {
            row_index,
        })
    })?;
    let wallet = WalletAddress::from_hex(value.trim()).map_err(|_| {
        ActivityParseError::WindowInvalidated(ActivityWindowInvalidation::InvalidWallet {
            row_index,
            value: value.clone(),
        })
    })?;
    if let Some(requested_wallet) = requested_wallet
        && wallet != requested_wallet
    {
        return Err(ActivityParseError::WindowInvalidated(
            ActivityWindowInvalidation::WalletMismatch {
                row_index,
                requested_wallet,
                payload_wallet: wallet,
            },
        ));
    }
    Ok(wallet)
}

fn normalize_row(
    raw: RawActivity,
    wallet: WalletAddress,
    context: &ActivityParseContext,
    raw_row: &str,
) -> Result<NormalizedActivity, ActivityValidationError> {
    let activity_type = required_string(raw.activity_type, "type")?;
    let activity_type = ActivityType::from(activity_type);
    let transaction_hash =
        required_string(raw.transaction_hash, "transactionHash")?.to_ascii_lowercase();
    let condition_id = optional_nonempty(raw.condition_id)
        .map(|value| PolymarketConditionId(value.to_ascii_lowercase()));
    let asset = optional_nonempty(raw.asset).map(PolymarketTokenId);
    let side = parse_side(raw.side)?;
    let outcome_label_present = optional_nonempty(raw.outcome).is_some();
    let outcome = parse_outcome(raw.outcome_index, outcome_label_present)?;
    let price_decimal = raw
        .price
        .ok_or(ActivityValidationError::MissingField { field: "price" })?
        .0;
    let price =
        Price::new(price_decimal).map_err(|error| ActivityValidationError::InvalidPrice {
            value: price_decimal,
            reason: error.to_string(),
        })?;
    let share_decimal = raw
        .size
        .ok_or(ActivityValidationError::MissingField { field: "size" })?
        .0;
    let share_amount = ShareAmount::from_decimal_exact(share_decimal).map_err(|error| {
        ActivityValidationError::InvalidShareAmount {
            value: share_decimal,
            reason: error.to_string(),
        }
    })?;
    let collateral_decimal = raw
        .usdc_size
        .ok_or(ActivityValidationError::MissingField { field: "usdcSize" })?
        .0;
    let source_usdc_amount =
        CollateralAmount::from_decimal_exact(collateral_decimal).map_err(|error| {
            ActivityValidationError::InvalidCollateralAmount {
                value: collateral_decimal,
                reason: error.to_string(),
            }
        })?;
    let source_epoch = raw
        .timestamp
        .ok_or(ActivityValidationError::MissingField { field: "timestamp" })?
        .parse()?;
    let source_epoch = normalize_epoch_seconds(source_epoch);
    let source_time = SourceTimestamp(
        time::OffsetDateTime::from_unix_timestamp(source_epoch)
            .map_err(|_| ActivityValidationError::InvalidTimestamp(source_epoch))?,
    );

    validate_type_specific(
        &activity_type,
        condition_id.as_ref(),
        asset.as_ref(),
        outcome,
        outcome_label_present,
        side,
        share_amount,
    )?;

    Ok(NormalizedActivity {
        activity_type,
        source_id: context.source_id.clone(),
        wallet,
        transaction_hash,
        condition_id,
        asset,
        outcome,
        side,
        price,
        share_amount,
        source_usdc_amount,
        is_combo: raw.is_combo.unwrap_or(false),
        source_time,
        observed_at: context.observed_at.clone(),
        received_at: context.received_at.clone(),
        raw_row_hash: blake3::hash(raw_row.as_bytes()).to_hex().to_string(),
        raw_row_json: raw_row.to_owned(),
        parser_version: ACTIVITY_PARSER_VERSION,
        schema_version: ACTIVITY_SCHEMA_VERSION,
        transport: context.transport,
    })
}

fn normalize_trade_observation(
    raw: RawActivity,
    wallet: WalletAddress,
) -> Result<ActivityTradeObservation, ActivityValidationError> {
    if let Some(activity_type) = optional_nonempty(raw.activity_type)
        && !activity_type.eq_ignore_ascii_case("TRADE")
    {
        return Err(ActivityValidationError::InvalidActivityType {
            value: activity_type,
        });
    }
    let transaction_hash =
        required_string(raw.transaction_hash, "transactionHash")?.to_ascii_lowercase();
    let condition_id = optional_nonempty(raw.condition_id)
        .map(|value| PolymarketConditionId(value.to_ascii_lowercase()))
        .ok_or(ActivityValidationError::MissingField {
            field: "conditionId",
        })?;
    let asset = optional_nonempty(raw.asset)
        .map(PolymarketTokenId)
        .ok_or(ActivityValidationError::MissingField { field: "asset" })?;
    let side =
        parse_side(raw.side)?.ok_or(ActivityValidationError::MissingField { field: "side" })?;
    let outcome_label_present = optional_nonempty(raw.outcome).is_some();
    let outcome = parse_outcome(raw.outcome_index, outcome_label_present)?
        .ok_or(ActivityValidationError::InvalidConditionOutcomeMapping)?;
    let price_decimal = raw
        .price
        .ok_or(ActivityValidationError::MissingField { field: "price" })?
        .0;
    Price::new(price_decimal).map_err(|error| ActivityValidationError::InvalidPrice {
        value: price_decimal,
        reason: error.to_string(),
    })?;
    let share_decimal = raw
        .size
        .ok_or(ActivityValidationError::MissingField { field: "size" })?
        .0;
    // Exactness is still validated; zero is allowed (raw-only, #544 fix 2).
    let _share_amount = ShareAmount::from_decimal_exact(share_decimal).map_err(|error| {
        ActivityValidationError::InvalidShareAmount {
            value: share_decimal,
            reason: error.to_string(),
        }
    })?;
    let source_epoch = raw
        .timestamp
        .ok_or(ActivityValidationError::MissingField { field: "timestamp" })?
        .parse()?;
    let source_epoch = normalize_epoch_seconds(source_epoch);
    let source_time = SourceTimestamp(
        time::OffsetDateTime::from_unix_timestamp(source_epoch)
            .map_err(|_| ActivityValidationError::InvalidTimestamp(source_epoch))?,
    );
    let group_id = SourceActivityGroupId::derive(SourceActivityGroupComponents {
        activity_type: ActivityType::Trade,
        wallet,
        transaction_hash,
        condition_id: Some(condition_id),
        asset: Some(asset),
        outcome: Some(outcome),
        side: Some(side),
    })
    .map_err(|_| ActivityValidationError::Identity)?;
    Ok(ActivityTradeObservation {
        wallet,
        group_id,
        source_time,
    })
}

fn required_string(
    value: Option<String>,
    field: &'static str,
) -> Result<String, ActivityValidationError> {
    let value = value.ok_or(ActivityValidationError::MissingField { field })?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ActivityValidationError::EmptyField { field });
    }
    Ok(trimmed.to_owned())
}

fn optional_nonempty(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    })
}

fn parse_side(value: Option<String>) -> Result<Option<Side>, ActivityValidationError> {
    let Some(value) = optional_nonempty(value) else {
        return Ok(None);
    };
    match value.to_ascii_uppercase().as_str() {
        "BUY" => Ok(Some(Side::Buy)),
        "SELL" => Ok(Some(Side::Sell)),
        _ => Err(ActivityValidationError::InvalidSide { value }),
    }
}

fn parse_outcome(
    value: Option<FlexibleU16>,
    outcome_label_present: bool,
) -> Result<Option<OutcomeId>, ActivityValidationError> {
    let Some(value) = value else {
        if outcome_label_present {
            return Err(ActivityValidationError::InvalidConditionOutcomeMapping);
        }
        return Ok(None);
    };
    let value = value.parse()?;
    if value == 999 && !outcome_label_present {
        return Ok(None);
    }
    Ok(Some(OutcomeId(value)))
}

fn validate_type_specific(
    activity_type: &ActivityType,
    condition_id: Option<&PolymarketConditionId>,
    asset: Option<&PolymarketTokenId>,
    outcome: Option<OutcomeId>,
    outcome_label_present: bool,
    side: Option<Side>,
    share_amount: ShareAmount,
) -> Result<(), ActivityValidationError> {
    // A zero-share position-changing row has an arithmetically zero effect and
    // is retained raw-only (#544 fix 2): effect-field validation (mapping,
    // side, asset) applies only to rows that can mutate a position.
    if share_amount == ShareAmount::ZERO {
        return Ok(());
    }
    match activity_type {
        ActivityType::Trade => {
            if condition_id.is_none() {
                return Err(ActivityValidationError::MissingField {
                    field: "conditionId",
                });
            }
            if asset.is_none() {
                return Err(ActivityValidationError::MissingField { field: "asset" });
            }
            if outcome.is_none() {
                return Err(ActivityValidationError::InvalidConditionOutcomeMapping);
            }
            if side.is_none() {
                return Err(ActivityValidationError::MissingField { field: "side" });
            }
        }
        ActivityType::Split | ActivityType::Merge => {
            if condition_id.is_none() {
                return Err(ActivityValidationError::MissingField {
                    field: "conditionId",
                });
            }
        }
        ActivityType::Redeem => {
            if condition_id.is_none() || outcome.is_none() || !outcome_label_present {
                return Err(ActivityValidationError::InvalidConditionOutcomeMapping);
            }
        }
        ActivityType::Conversion
        | ActivityType::Reward
        | ActivityType::Deposit
        | ActivityType::Withdrawal
        | ActivityType::Yield
        | ActivityType::MakerRebate
        | ActivityType::TakerRebate
        | ActivityType::ReferralReward
        | ActivityType::Unknown(_) => {}
    }
    Ok(())
}

fn normalize_epoch_seconds(timestamp: i64) -> i64 {
    if timestamp > 9_999_999_999 {
        timestamp / 1_000
    } else {
        timestamp
    }
}

/// Checked-floor projection used only when a version-one frame still requires
/// legacy whole-contract `u64` quantity. Version-two paths retain atomics.
pub fn project_legacy_contract_qty_v1(
    amount: ShareAmount,
) -> Result<ContractQty, ActivityValidationError> {
    amount
        .to_decimal()
        .floor()
        .to_u64()
        .map(ContractQty)
        .ok_or(ActivityValidationError::LegacyProjectionOutOfRange)
}

/// Exact share-times-price sum retained by a reconciled aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PriceWeightedShareAmount(pub Decimal);

/// Lowercase BLAKE3 semantic revision hash over one complete group multiset.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct ActivitySemanticRevision(String);

impl ActivitySemanticRevision {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ActivitySemanticRevision {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        let valid = value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        if !valid {
            return Err(serde::de::Error::custom(
                "semantic revision must be 64 lowercase hexadecimal characters",
            ));
        }
        Ok(Self(value))
    }
}

/// Persistable aggregate of every same-identity row in one complete response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityAggregate {
    pub group_id: SourceActivityGroupId,
    pub row_count: u64,
    pub share_sum: ShareAmount,
    pub price_weighted_share_sum: PriceWeightedShareAmount,
    pub source_usdc_sum: CollateralAmount,
    pub source_time: SourceTimestamp,
    /// Identical combo classification shared by every member row.
    pub is_combo: bool,
    pub semantic_revision: ActivitySemanticRevision,
}

impl ActivityAggregate {
    #[must_use]
    pub fn semantically_equal(&self, other: &Self) -> bool {
        self.group_id == other.group_id && self.semantic_revision == other.semantic_revision
    }

    #[must_use]
    pub fn compare_revision(&self, other: &Self) -> ActivityRevisionComparison {
        if self.group_id != other.group_id {
            ActivityRevisionComparison::DifferentGroup
        } else if self.semantic_revision == other.semantic_revision {
            ActivityRevisionComparison::Equal
        } else {
            ActivityRevisionComparison::Changed
        }
    }

    /// Exact volume-weighted trade price, derived from the complete aggregate.
    /// `usdcSize` is deliberately not consulted because it is audit evidence,
    /// not the ledger/classification price owner (#544).
    pub fn volume_weighted_price(&self) -> Result<Price, ActivityAggregationError> {
        if self.share_sum == ShareAmount::ZERO {
            return Err(ActivityAggregationError::ZeroShareSum {
                group_id: self.group_id.to_string(),
            });
        }
        let value = self
            .price_weighted_share_sum
            .0
            .checked_div(self.share_sum.to_decimal())
            .ok_or_else(|| ActivityAggregationError::PriceWeightedSumOverflow {
                group_id: self.group_id.to_string(),
            })?;
        Price::new(value).map_err(|_| ActivityAggregationError::InvalidWeightedPrice {
            group_id: self.group_id.to_string(),
            value,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityRevisionComparison {
    Equal,
    Changed,
    DifferentGroup,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ActivityAggregationError {
    #[error("activity identity: {0}")]
    Identity(#[from] ActivityIdentityError),
    #[error("activity group {group_id} has mixed source timestamps {expected} and {actual}")]
    CausalAmbiguity {
        group_id: String,
        expected: i64,
        actual: i64,
    },
    #[error("activity group {group_id} mixes ordinary and combo rows")]
    MixedComboState { group_id: String },
    #[error("activity group {group_id} has no members")]
    EmptyGroup { group_id: String },
    #[error("activity group {group_id} exact amount overflow")]
    AmountOverflow { group_id: String },
    #[error("activity group {group_id} exact price-weighted sum overflow")]
    PriceWeightedSumOverflow { group_id: String },
    #[error("activity group {group_id} has zero aggregate share amount")]
    ZeroShareSum { group_id: String },
    #[error("activity group {group_id} has invalid volume-weighted price {value}")]
    InvalidWeightedPrice { group_id: String, value: Decimal },
}

/// Group and aggregate one complete fixed-end response without deduplicating
/// member rows. Returned groups are sorted by version-two key for persistence.
pub fn aggregate_activity_rows(
    rows: &[NormalizedActivity],
) -> Result<Vec<ActivityAggregate>, ActivityAggregationError> {
    let mut groups: HashMap<SourceActivityGroupId, Vec<&NormalizedActivity>> = HashMap::new();
    for row in rows {
        groups.entry(row.group_id()?).or_default().push(row);
    }

    let mut groups: Vec<_> = groups.into_iter().collect();
    groups.sort_by(|(left, _), (right, _)| left.key.0.cmp(&right.key.0));
    groups
        .into_iter()
        .map(|(group_id, members)| aggregate_group(group_id, &members))
        .collect()
}

fn aggregate_group(
    group_id: SourceActivityGroupId,
    members: &[&NormalizedActivity],
) -> Result<ActivityAggregate, ActivityAggregationError> {
    let Some(first) = members.first() else {
        return Err(ActivityAggregationError::EmptyGroup {
            group_id: group_id.to_string(),
        });
    };
    let expected = first.source_time.0.unix_timestamp();
    let is_combo = first.is_combo;
    let mut share_sum = ShareAmount::ZERO;
    let mut source_usdc_sum = CollateralAmount::ZERO;
    let mut price_weighted_share_sum = Decimal::ZERO;
    for member in members {
        let actual = member.source_time.0.unix_timestamp();
        if actual != expected {
            return Err(ActivityAggregationError::CausalAmbiguity {
                group_id: group_id.to_string(),
                expected,
                actual,
            });
        }
        if member.is_combo != is_combo {
            return Err(ActivityAggregationError::MixedComboState {
                group_id: group_id.to_string(),
            });
        }
        share_sum = share_sum.checked_add(member.share_amount).map_err(|_| {
            ActivityAggregationError::AmountOverflow {
                group_id: group_id.to_string(),
            }
        })?;
        source_usdc_sum = source_usdc_sum
            .checked_add(member.source_usdc_amount)
            .map_err(|_| ActivityAggregationError::AmountOverflow {
                group_id: group_id.to_string(),
            })?;
        let weighted = member
            .share_amount
            .to_decimal()
            .checked_mul(member.price.0)
            .ok_or_else(|| ActivityAggregationError::PriceWeightedSumOverflow {
                group_id: group_id.to_string(),
            })?;
        price_weighted_share_sum =
            price_weighted_share_sum
                .checked_add(weighted)
                .ok_or_else(|| ActivityAggregationError::PriceWeightedSumOverflow {
                    group_id: group_id.to_string(),
                })?;
    }
    let semantic_revision = derive_semantic_revision(&group_id, members)?;
    let row_count =
        u64::try_from(members.len()).map_err(|_| ActivityAggregationError::AmountOverflow {
            group_id: group_id.to_string(),
        })?;
    Ok(ActivityAggregate {
        group_id,
        row_count,
        share_sum,
        price_weighted_share_sum: PriceWeightedShareAmount(price_weighted_share_sum),
        source_usdc_sum,
        source_time: first.source_time.clone(),
        is_combo,
        semantic_revision,
    })
}

fn derive_semantic_revision(
    group_id: &SourceActivityGroupId,
    members: &[&NormalizedActivity],
) -> Result<ActivitySemanticRevision, ActivityAggregationError> {
    let mut encodings = members
        .iter()
        .map(|member| semantic_row_encoding(member))
        .collect::<Result<Vec<_>, _>>()?;
    encodings.sort();

    let mut hasher = blake3::Hasher::new();
    hasher.update(ACTIVITY_REVISION_DOMAIN);
    hash_component(&mut hasher, group_id.key.0.as_bytes())?;
    hash_component(
        &mut hasher,
        &u64::try_from(members.len())
            .map_err(|_| ActivityIdentityError::ComponentTooLong)?
            .to_be_bytes(),
    )?;
    for encoding in encodings {
        hash_component(&mut hasher, &encoding)?;
    }
    Ok(ActivitySemanticRevision(
        hasher.finalize().to_hex().to_string(),
    ))
}

fn semantic_row_encoding(row: &NormalizedActivity) -> Result<Vec<u8>, ActivityIdentityError> {
    let mut encoder = CanonicalEncoder::default();
    encoder.present(&row.source_time.0.unix_timestamp().to_be_bytes())?;
    encoder.present(&row.share_amount.atomic().to_be_bytes())?;
    encoder.present(row.price.0.normalize().to_string().as_bytes())?;
    encoder.present(if row.is_combo { b"combo" } else { b"ordinary" })?;

    match row.activity_type {
        ActivityType::Trade => {
            encoder.present(b"trade")?;
            encode_effect_components(&mut encoder, row)?;
        }
        ActivityType::Split => {
            encoder.present(b"split")?;
            encode_effect_components(&mut encoder, row)?;
        }
        ActivityType::Merge => {
            encoder.present(b"merge")?;
            encode_effect_components(&mut encoder, row)?;
        }
        ActivityType::Redeem => {
            encoder.present(b"redeem")?;
            encode_effect_components(&mut encoder, row)?;
        }
        ActivityType::Conversion => {
            encoder.present(b"conversion")?;
            encode_effect_components(&mut encoder, row)?;
        }
        ActivityType::Reward
        | ActivityType::Deposit
        | ActivityType::Withdrawal
        | ActivityType::Yield
        | ActivityType::MakerRebate
        | ActivityType::TakerRebate
        | ActivityType::ReferralReward => {
            encoder.present(b"raw_only")?;
        }
        ActivityType::Unknown(_) => {
            encoder.present(b"unknown")?;
            encode_effect_components(&mut encoder, row)?;
        }
    }
    Ok(encoder.finish())
}

fn encode_effect_components(
    encoder: &mut CanonicalEncoder,
    row: &NormalizedActivity,
) -> Result<(), ActivityIdentityError> {
    encoder.optional(row.condition_id.as_ref().map(|value| value.0.as_bytes()))?;
    encoder.optional(row.asset.as_ref().map(|value| value.0.as_bytes()))?;
    let outcome = row.outcome.map(|value| value.0.to_be_bytes());
    encoder.optional(outcome.as_ref().map(<[u8; 2]>::as_slice))?;
    encoder.optional(row.side.map(side_bytes))?;
    Ok(())
}

fn canonical_group_components(
    components: &SourceActivityGroupComponents,
) -> Result<Vec<u8>, ActivityIdentityError> {
    let mut encoder = CanonicalEncoder::default();
    encoder.present(ACTIVITY_GROUP_DOMAIN)?;
    encoder.present(components.activity_type.as_str().as_bytes())?;
    encoder.present(components.wallet.to_string().as_bytes())?;
    encoder.present(components.transaction_hash.as_bytes())?;
    encoder.optional(
        components
            .condition_id
            .as_ref()
            .map(|value| value.0.as_bytes()),
    )?;
    encoder.optional(components.asset.as_ref().map(|value| value.0.as_bytes()))?;
    let outcome = components.outcome.map(|value| value.0.to_be_bytes());
    encoder.optional(outcome.as_ref().map(<[u8; 2]>::as_slice))?;
    encoder.optional(components.side.map(side_bytes))?;
    Ok(encoder.finish())
}

fn side_bytes(side: Side) -> &'static [u8] {
    match side {
        Side::Buy => b"BUY",
        Side::Sell => b"SELL",
    }
}

fn hash_component(hasher: &mut blake3::Hasher, bytes: &[u8]) -> Result<(), ActivityIdentityError> {
    let len = u64::try_from(bytes.len()).map_err(|_| ActivityIdentityError::ComponentTooLong)?;
    hasher.update(&len.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

#[derive(Default)]
struct CanonicalEncoder {
    bytes: Vec<u8>,
}

impl CanonicalEncoder {
    fn present(&mut self, value: &[u8]) -> Result<(), ActivityIdentityError> {
        self.bytes.push(1);
        self.length_prefixed(value)
    }

    fn optional(&mut self, value: Option<&[u8]>) -> Result<(), ActivityIdentityError> {
        match value {
            Some(value) => self.present(value),
            None => {
                self.bytes.push(0);
                self.bytes.extend_from_slice(&0_u64.to_be_bytes());
                Ok(())
            }
        }
    }

    fn length_prefixed(&mut self, value: &[u8]) -> Result<(), ActivityIdentityError> {
        let len =
            u64::try_from(value.len()).map_err(|_| ActivityIdentityError::ComponentTooLong)?;
        self.bytes.extend_from_slice(&len.to_be_bytes());
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    fn wallet() -> WalletAddress {
        WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
    }

    fn components() -> SourceActivityGroupComponents {
        SourceActivityGroupComponents {
            activity_type: ActivityType::Trade,
            wallet: wallet(),
            transaction_hash: "0xabc".to_owned(),
            condition_id: Some(PolymarketConditionId("0xcondition".to_owned())),
            asset: Some(PolymarketTokenId("123".to_owned())),
            outcome: Some(OutcomeId(0)),
            side: Some(Side::Buy),
        }
    }

    #[test]
    fn canonical_components_are_delimiter_safe() {
        let mut first = CanonicalEncoder::default();
        first.present(b"ab").unwrap();
        first.present(b"c").unwrap();
        let mut second = CanonicalEncoder::default();
        second.present(b"a").unwrap();
        second.present(b"bc").unwrap();
        assert_ne!(first.finish(), second.finish());

        let mut absent = CanonicalEncoder::default();
        absent.optional(None).unwrap();
        let mut empty = CanonicalEncoder::default();
        empty.optional(Some(b"")).unwrap();
        assert_ne!(absent.finish(), empty.finish());

        let mut left_components = components();
        left_components.transaction_hash = "0xab".to_owned();
        left_components.condition_id = Some(PolymarketConditionId("c".to_owned()));
        let mut right_components = components();
        right_components.transaction_hash = "0xa".to_owned();
        right_components.condition_id = Some(PolymarketConditionId("bc".to_owned()));
        assert_ne!(
            canonical_group_components(&left_components).unwrap(),
            canonical_group_components(&right_components).unwrap()
        );
    }

    #[test]
    fn stored_group_rejects_component_tampering() {
        let group = SourceActivityGroupId::derive(components()).unwrap();
        let mut stored = serde_json::to_value(group).unwrap();
        stored["components"]["transaction_hash"] = serde_json::json!("0xchanged");
        assert!(serde_json::from_value::<SourceActivityGroupId>(stored).is_err());
    }

    #[test]
    fn legacy_projection_is_checked_floor_for_v1_only() {
        assert_eq!(
            project_legacy_contract_qty_v1(ShareAmount::from_atomic(18_550_000)).unwrap(),
            ContractQty(18)
        );
        assert_eq!(
            project_legacy_contract_qty_v1(ShareAmount::from_atomic(1)).unwrap(),
            ContractQty(0)
        );
    }

    #[test]
    fn exact_decimal_preserves_excess_precision_number_token() {
        // Plain-serde f64 routing rounds this to 10000000000000 (loses one atomic
        // share); the raw-lexeme path must keep it exact (#544 review).
        let row = r#"{"size":10000000000000.000001}"#;
        #[derive(Deserialize)]
        struct Probe {
            size: ExactDecimal,
        }
        let probe: Probe = serde_json::from_str(row).unwrap();
        assert_eq!(
            probe.size.0,
            Decimal::from_str_exact("10000000000000.000001").unwrap()
        );
    }

    #[test]
    fn exact_decimal_rejects_scientific_and_overflow_tokens() {
        #[derive(Deserialize)]
        struct Probe {
            #[allow(dead_code)]
            size: ExactDecimal,
        }
        // Scientific notation hides excess precision behind rounding: reject.
        assert!(serde_json::from_str::<Probe>(r#"{"size":1.00000000000000000001e13}"#).is_err());
        // A quoted magnitude beyond Decimal must reject, not panic or saturate.
        assert!(
            serde_json::from_str::<Probe>(r#"{"size":"79228162514264337593543950335999"}"#)
                .is_err()
        );
    }

    #[test]
    fn share_amount_rejects_decimal_max_without_panicking() {
        // Decimal::MAX * ATOMIC_SCALE overflowed with a panicking multiply before
        // the checked_mul fix (#544 review).
        assert!(ShareAmount::from_decimal_exact(Decimal::MAX).is_err());
    }

    #[test]
    fn websocket_observation_derives_rest_group_without_inventing_missing_fields() {
        let raw = br#"{"proxyWallet":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","conditionId":"0xcondition","asset":"123","side":"BUY","size":"5","price":"0.5","timestamp":"1704067200","transactionHash":"0xABC","outcomeIndex":"0"}"#;
        let observation = parse_activity_trade_observation(raw).unwrap();
        assert_eq!(observation.wallet, wallet());
        assert_eq!(observation.source_time.0.unix_timestamp(), 1_704_067_200);
        assert_eq!(observation.group_id.components(), &components());
    }

    #[test]
    fn websocket_observation_rejects_non_trade_and_missing_asset() {
        let non_trade = br#"{"proxyWallet":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","type":"SPLIT","conditionId":"0xcondition","asset":"123","side":"BUY","size":"5","price":"0.5","timestamp":"1704067200","transactionHash":"0xabc","outcomeIndex":"0"}"#;
        assert!(parse_activity_trade_observation(non_trade).is_err());
        let missing_asset = br#"{"proxyWallet":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","conditionId":"0xcondition","side":"BUY","size":"5","price":"0.5","timestamp":"1704067200","transactionHash":"0xabc","outcomeIndex":"0"}"#;
        assert!(parse_activity_trade_observation(missing_asset).is_err());
    }

    #[test]
    fn zero_share_position_changing_rows_are_raw_only_not_errors() {
        // Live capture 2026-09-01 (wallet 0xfd9b76…, 108 such rows): zero-burn
        // REDEEM legs for empty outcome sides. Zero effect ⇒ raw-only (#544).
        let row = r#"{"proxyWallet":"0xfd9b763674cb096cacec059fcfe60ae82aae09e8","timestamp":1783529873,"conditionId":"0x945561381b820840a1876a9bbacc5e702c50a91ac40936870988d00159d281d6","type":"REDEEM","size":0,"usdcSize":0,"transactionHash":"0x83e3a30760ec2c4cc7a59a44cd54c64f486e8da924427ee7c46c2eb240ac26af","price":0,"asset":"","side":"","outcomeIndex":0}"#;
        let context = ActivityParseContext {
            source_id: SourceId("polymarket-activity-test".to_owned()),
            observed_at: SourceTimestamp(
                time::OffsetDateTime::from_unix_timestamp(1_783_529_873).unwrap(),
            ),
            received_at: ReceivedAt(
                time::OffsetDateTime::from_unix_timestamp(1_783_529_874).unwrap(),
            ),
            transport: ActivityTransport::Rest,
        };
        let parsed = parse_activity_row(
            row.as_bytes(),
            Some(WalletAddress::from_hex("0xfd9b763674cb096cacec059fcfe60ae82aae09e8").unwrap()),
            &context,
        )
        .unwrap();
        assert_eq!(parsed.share_amount, ShareAmount::ZERO);
        assert!(!parsed.is_ordinary_position_change());
        assert!(!parsed.requires_wallet_fence());

        // Zero CONVERSION: no fence; zero-effect raw retention.
        let conv = row.replace("\"type\":\"REDEEM\"", "\"type\":\"CONVERSION\"");
        let parsed = parse_activity_row(
            conv.as_bytes(),
            Some(WalletAddress::from_hex("0xfd9b763674cb096cacec059fcfe60ae82aae09e8").unwrap()),
            &context,
        )
        .unwrap();
        assert!(!parsed.requires_wallet_fence());

        // Negative stays a hard error.
        let neg = row.replace("\"size\":0", "\"size\":-1");
        assert!(
            parse_activity_row(
                neg.as_bytes(),
                Some(
                    WalletAddress::from_hex("0xfd9b763674cb096cacec059fcfe60ae82aae09e8").unwrap()
                ),
                &context,
            )
            .is_err()
        );
    }
}
