//! Lane registry: persistence + mutation. Pure logic lives here;
//! the daemon decides when to load/save.

pub mod meta;
pub mod time;

pub use meta::{execute, match_lane, parse_meta, MetaAction};
pub use time::{age_summary, now_iso};

use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use telepathy_proto::MAX_SAFE_SEQUENCE;
pub use telepathy_proto::{is_valid_lane_id, MAX_LANE_ID_LENGTH};

/// Shared with `server/src/lanes.ts::MAX_LANE_COUNT`.
///
/// A complete state response for 256 normal maximum-length generated lanes,
/// including `pending`, `active`, and `revision`, is under 128 KiB. That is an
/// eightfold margin below the 1 MiB Node <-> telepathyd state transport cap.
pub const MAX_LANE_COUNT: usize = 256;
pub const LANE_CAPACITY_ERROR_MESSAGE: &str = "lane capacity reached; use an existing conversation";

/// Shared with `server/src/lanes.ts`.  The caps apply to every persisted
/// display/metadata string, before snapshot replacement as well as on load.
/// Keeping all three measures explicit prevents Node UTF-16 and Rust scalar
/// iteration from accepting different durable values.
pub const MAX_LANE_NAME_UTF8_BYTES: usize = 128;
pub const MAX_LANE_NAME_UTF16_CODE_UNITS: usize = 128;
pub const MAX_LANE_NAME_CODEPOINTS: usize = 128;
pub const MAX_LANE_TIMESTAMP_UTF8_BYTES: usize = 64;
pub const MAX_LANE_TIMESTAMP_UTF16_CODE_UNITS: usize = 64;
pub const MAX_LANE_TIMESTAMP_CODEPOINTS: usize = 64;

/// Titles originate outside the registry and are never persisted. They still
/// need an output cap because `/api/state` is transported as one bounded body.
pub const MAX_ENRICHED_LANE_TITLE_UTF8_BYTES: usize = 256;
pub const MAX_ENRICHED_LANE_TITLE_CODEPOINTS: usize = 128;

static SAVE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A lane snapshot is replaced atomically, but a failure after `rename` leaves
/// the caller unable to tell whether the replacement survived a crash. Keep
/// that distinction explicit: callers may retry a definite pre-rename error,
/// while an ambiguous post-rename error must fence later writes until they
/// reconcile from disk.
#[derive(Debug)]
pub enum LaneRegistrySaveError {
    InvalidRegistry(String),
    Serialization(serde_json::Error),
    PreRename(std::io::Error),
    AmbiguousPostRename(std::io::Error),
}

/// A requested lane name could not be represented by the shared lane-id
/// grammar. This is caller input, never an invariant failure: callers must
/// return an invalid-input result without changing the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneCreateError {
    BlankName,
    InvalidGeneratedId,
    CapacityReached,
}

impl std::fmt::Display for LaneCreateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlankName => write!(formatter, "lane name must not be blank"),
            Self::InvalidGeneratedId => write!(
                formatter,
                "lane name is too long to produce a valid lane identifier"
            ),
            Self::CapacityReached => write!(formatter, "{LANE_CAPACITY_ERROR_MESSAGE}"),
        }
    }
}

impl std::error::Error for LaneCreateError {}

impl LaneRegistrySaveError {
    pub fn is_ambiguous(&self) -> bool {
        matches!(self, Self::AmbiguousPostRename(_))
    }
}

impl std::fmt::Display for LaneRegistrySaveError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRegistry(error) => write!(formatter, "invalid lane registry: {error}"),
            Self::Serialization(error) => {
                write!(formatter, "cannot serialize lane registry: {error}")
            }
            Self::PreRename(error) => write!(formatter, "lane snapshot was not replaced: {error}"),
            Self::AmbiguousPostRename(error) => write!(
                formatter,
                "lane snapshot may have been replaced before persistence failed: {error}"
            ),
        }
    }
}

impl std::error::Error for LaneRegistrySaveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serialization(error) => Some(error),
            Self::PreRename(error) | Self::AmbiguousPostRename(error) => Some(error),
            Self::InvalidRegistry(_) => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Lane {
    pub id: String,
    pub name: String,
    pub created_at: String,
    pub last_active: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interactions: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LaneRegistry {
    pub lanes: Vec<Lane>,
    pub active_id: String,
    pub previous_id: String,
}

impl LaneRegistry {
    pub fn default_direct() -> Self {
        let now = now_iso();
        Self {
            lanes: vec![Lane {
                id: "telepathy:direct".into(),
                name: "direct".into(),
                created_at: now.clone(),
                last_active: now,
                interactions: None,
            }],
            active_id: "telepathy:direct".into(),
            previous_id: "telepathy:direct".into(),
        }
    }

    pub fn load(path: &PathBuf) -> Self {
        let text = match fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Self::default_direct(),
            Err(e) => panic!("cannot read lane registry {}: {e}", path.display()),
        };
        let registry: Self = serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("corrupt lane registry {}: {e}", path.display()));
        registry
            .validate()
            .unwrap_or_else(|e| panic!("invalid lane registry {}: {e}", path.display()));
        registry
    }

    pub fn save(&self, path: &PathBuf) -> Result<(), LaneRegistrySaveError> {
        self.validate()
            .map_err(LaneRegistrySaveError::InvalidRegistry)?;
        let json =
            serde_json::to_string_pretty(self).map_err(LaneRegistrySaveError::Serialization)?;
        atomic_write(path, &json)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.lanes.is_empty() {
            return Err("lanes must not be empty".into());
        }
        if self.lanes.len() > MAX_LANE_COUNT {
            return Err(format!(
                "lane count exceeds the {MAX_LANE_COUNT} lane limit"
            ));
        }
        let mut ids = std::collections::HashSet::new();
        for (index, lane) in self.lanes.iter().enumerate() {
            if !is_valid_lane_id(&lane.id)
                || !is_valid_persisted_lane_name(&lane.name)
                || !is_valid_lane_timestamp(&lane.created_at)
                || !is_valid_lane_timestamp(&lane.last_active)
            {
                return Err(format!("malformed lane at index {index}"));
            }
            if lane
                .interactions
                .is_some_and(|interactions| interactions > MAX_SAFE_SEQUENCE)
            {
                return Err(format!(
                    "lane at index {index} has an interaction count outside the JSON-safe range"
                ));
            }
            if !ids.insert(&lane.id) {
                return Err(format!("duplicate lane id {}", lane.id));
            }
        }
        if !ids.contains(&self.active_id) || !ids.contains(&self.previous_id) {
            return Err("active/previous lane is missing".into());
        }
        Ok(())
    }

    /// Panics only if the registry is malformed (empty lanes) — a bug, not input.
    pub fn active(&self) -> &Lane {
        self.lanes
            .iter()
            .find(|l| l.id == self.active_id)
            .unwrap_or(&self.lanes[0])
    }

    /// Touch a lane's activity timestamp, returning `None` when the lane is
    /// not present (or the id is not valid).
    pub fn touch(&mut self, id: &str) -> Option<()> {
        if !is_valid_lane_id(id) {
            return None;
        }
        if let Some(l) = self.lanes.iter_mut().find(|l| l.id == id) {
            l.last_active = now_iso();
            Some(())
        } else {
            None
        }
    }

    pub fn switch(&mut self, id: &str) -> Option<Lane> {
        if !is_valid_lane_id(id) {
            return None;
        }
        if !self.lanes.iter().any(|l| l.id == id) {
            return None;
        }
        if self.active_id != id {
            self.previous_id = self.active_id.clone();
            self.active_id = id.to_string();
        }
        let _ = self.touch(id);
        self.lanes.iter().find(|l| l.id == id).cloned()
    }

    /// Create (or return existing) lane; does NOT switch.
    ///
    /// Generation is intentionally fallible because an untrusted name can
    /// exceed the shared wire lane-id bound. Do not turn that input condition
    /// into an assertion: callers need a chance to reject it without a
    /// partial registry mutation.
    pub fn create(&mut self, name: &str) -> Result<Lane, LaneCreateError> {
        self.validate_create(name)?;
        let slug = slugify(name);
        let id = format!("telepathy:repo:{slug}");
        if let Some(l) = self.lanes.iter().find(|l| l.id == id) {
            return Ok(l.clone());
        }
        let now = now_iso();
        let lane = Lane {
            id: id.clone(),
            name: slug,
            created_at: now.clone(),
            last_active: now,
            interactions: None,
        };
        self.lanes.push(lane.clone());
        Ok(lane)
    }

    /// Validate a requested lane name without mutating a registry. HTTP
    /// handlers use this before checking durability state, so malformed input
    /// consistently reports 400 rather than a transient storage failure.
    pub fn validate_create_name(name: &str) -> Result<(), LaneCreateError> {
        if name.trim().is_empty() {
            return Err(LaneCreateError::BlankName);
        }
        let slug = slugify(name);
        if slug.is_empty() {
            return Err(LaneCreateError::InvalidGeneratedId);
        }
        let id = format!("telepathy:repo:{slug}");
        if !is_valid_lane_id(&id) {
            return Err(LaneCreateError::InvalidGeneratedId);
        }
        Ok(())
    }

    /// Validate a create against this registry without mutating it. Existing
    /// lanes remain usable at capacity; only a new lane is rejected.
    pub fn validate_create(&self, name: &str) -> Result<(), LaneCreateError> {
        Self::validate_create_name(name)?;
        let id = format!("telepathy:repo:{}", slugify(name));
        if !self.lanes.iter().any(|lane| lane.id == id) && self.lanes.len() >= MAX_LANE_COUNT {
            return Err(LaneCreateError::CapacityReached);
        }
        Ok(())
    }
}

fn is_within_string_caps(
    value: &str,
    max_utf8_bytes: usize,
    max_utf16_code_units: usize,
    max_codepoints: usize,
) -> bool {
    value.len() <= max_utf8_bytes
        && value.encode_utf16().count() <= max_utf16_code_units
        && value.chars().count() <= max_codepoints
}

/// A persisted lane name is nonempty and bounded, but is otherwise preserved
/// byte-for-byte/character-for-character rather than normalized on restart.
pub fn is_valid_persisted_lane_name(value: &str) -> bool {
    !value.is_empty()
        && is_within_string_caps(
            value,
            MAX_LANE_NAME_UTF8_BYTES,
            MAX_LANE_NAME_UTF16_CODE_UNITS,
            MAX_LANE_NAME_CODEPOINTS,
        )
}

fn is_ascii_digits(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_leap_year(year: u16) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn parse_fixed_digits(value: &str, start: usize, length: usize) -> Option<u16> {
    let digits = value.get(start..start + length)?;
    if !is_ascii_digits(digits) {
        return None;
    }
    digits.parse().ok()
}

/// Current Node snapshots use exact UTC ISO milliseconds while telepathyd
/// writes `epoch-ms:` plus a JSON-safe non-negative integer. Both are already
/// authoritative shared snapshots, so the hard validation accepts those two
/// exact spellings and rejects every other type/range without repairing it.
pub fn is_valid_lane_timestamp(value: &str) -> bool {
    if !is_within_string_caps(
        value,
        MAX_LANE_TIMESTAMP_UTF8_BYTES,
        MAX_LANE_TIMESTAMP_UTF16_CODE_UNITS,
        MAX_LANE_TIMESTAMP_CODEPOINTS,
    ) {
        return false;
    }

    if let Some(milliseconds) = value.strip_prefix("epoch-ms:") {
        return milliseconds.len() <= 16
            && is_ascii_digits(milliseconds)
            && milliseconds
                .parse::<u64>()
                .is_ok_and(|milliseconds| milliseconds <= MAX_SAFE_SEQUENCE);
    }

    let bytes = value.as_bytes();
    if bytes.len() != 24
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'.'
        || bytes[23] != b'Z'
    {
        return false;
    }
    let Some(year) = parse_fixed_digits(value, 0, 4) else {
        return false;
    };
    let Some(month) = parse_fixed_digits(value, 5, 2) else {
        return false;
    };
    let Some(day) = parse_fixed_digits(value, 8, 2) else {
        return false;
    };
    let Some(hour) = parse_fixed_digits(value, 11, 2) else {
        return false;
    };
    let Some(minute) = parse_fixed_digits(value, 14, 2) else {
        return false;
    };
    let Some(second) = parse_fixed_digits(value, 17, 2) else {
        return false;
    };
    let Some(millisecond) = parse_fixed_digits(value, 20, 3) else {
        return false;
    };
    if year == 0
        || !(1..=12).contains(&month)
        || hour > 23
        || minute > 59
        || second > 59
        || millisecond > 999
    {
        return false;
    }
    let days_in_month = [
        31,
        if is_leap_year(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    day >= 1 && day <= days_in_month[(month - 1) as usize]
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AtomicWriteStage {
    /// A missing directory was just created. Its parent must be synced before
    /// this write can claim any durability for the newly-created path.
    BeforeNewDirectoryParentSync,
    BeforeRename,
    AfterRename,
}

fn atomic_write(path: &Path, contents: &str) -> Result<(), LaneRegistrySaveError> {
    atomic_write_with_stage_hook(path, contents, |_| Ok(()))
}

fn atomic_write_with_stage_hook<F>(
    path: &Path,
    contents: &str,
    mut stage_hook: F,
) -> Result<(), LaneRegistrySaveError>
where
    F: FnMut(AtomicWriteStage) -> std::io::Result<()>,
{
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("lanes.json");
    let nonce = SAVE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let temp = parent.join(format!(
        ".{name}.tmp-{}-{timestamp}-{nonce}",
        std::process::id()
    ));
    let mut renamed = false;
    let result = (|| {
        create_parent_directories_durably(parent, &mut stage_hook)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        stage_hook(AtomicWriteStage::BeforeRename)?;
        fs::rename(&temp, path)?;
        renamed = true;
        stage_hook(AtomicWriteStage::AfterRename)?;
        fs::File::open(parent)?.sync_all()?;
        Ok::<_, std::io::Error>(())
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(&temp);
        return Err(if renamed {
            LaneRegistrySaveError::AmbiguousPostRename(error)
        } else {
            LaneRegistrySaveError::PreRename(error)
        });
    }
    Ok(())
}

/// Recursively create `directory`, making every newly-created entry durable
/// before using it as a parent for a durable snapshot. `create_dir_all` gives
/// no way to learn which directories it created, so it cannot provide this
/// durability boundary on its own.
fn create_parent_directories_durably<F>(directory: &Path, stage_hook: &mut F) -> std::io::Result<()>
where
    F: FnMut(AtomicWriteStage) -> std::io::Result<()>,
{
    let mut missing = Vec::new();
    let mut current = directory.to_path_buf();

    loop {
        match fs::metadata(&current) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotADirectory,
                    format!("{} is not a directory", current.display()),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(current.clone());
                current = parent_or_current_directory(&current).to_path_buf();
            }
            Err(error) => return Err(error),
        }
    }

    for created in missing.into_iter().rev() {
        match fs::create_dir(&created) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let metadata = fs::metadata(&created)?;
                if !metadata.is_dir() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotADirectory,
                        format!("{} is not a directory", created.display()),
                    ));
                }
            }
            Err(error) => return Err(error),
        }
        // A concurrent creator may have won the mkdir race. It is still a
        // newly discovered directory in this write path, so sync its parent
        // before relying on it for this snapshot.
        stage_hook(AtomicWriteStage::BeforeNewDirectoryParentSync)?;
        fs::File::open(parent_or_current_directory(&created))?.sync_all()?;
    }
    Ok(())
}

fn parent_or_current_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn slugify(name: &str) -> String {
    let s: String = name
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    s.trim_matches('-').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn full_registry() -> LaneRegistry {
        let mut registry = LaneRegistry::default_direct();
        for index in 1..MAX_LANE_COUNT {
            let name = format!("{index:03}-{}", "x".repeat(109));
            registry.create(&name).unwrap();
        }
        assert_eq!(registry.lanes.len(), MAX_LANE_COUNT);
        registry
    }

    #[test]
    fn lane_id_contract_preserves_direct_and_generated_ids() {
        for id in ["telepathy:direct", "telepathy:repo:geospatial-migration"] {
            assert!(is_valid_lane_id(id), "expected valid lane id: {id}");
        }
    }

    #[test]
    fn lane_id_contract_rejects_unquoted_content_and_unicode() {
        for id in [
            "",
            " ",
            "telepathy:repo:bad\"quote",
            r"telepathy:repo:bad\slash",
            "telepathy:repo:bad\nnewline",
            "telepathy:repo:é",
            &format!("telepathy:repo:{}", "a".repeat(MAX_LANE_ID_LENGTH)),
        ] {
            assert!(!is_valid_lane_id(id), "expected invalid lane id: {id:?}");
        }
    }

    #[test]
    fn invalid_switch_does_not_mutate_registry() {
        let mut registry = LaneRegistry::default_direct();
        let before = registry.clone();
        assert!(registry.switch("telepathy:direct\"altered").is_none());
        assert_eq!(registry, before);
    }

    #[test]
    fn oversized_or_blank_lane_names_are_rejected_without_panicking_or_mutating() {
        let mut registry = LaneRegistry::default_direct();
        let before = registry.clone();
        let oversized = "a".repeat(MAX_LANE_ID_LENGTH);

        assert_eq!(
            registry.create(&oversized),
            Err(LaneCreateError::InvalidGeneratedId)
        );
        assert_eq!(registry.create(" \t\n"), Err(LaneCreateError::BlankName));
        assert_eq!(registry, before);
    }

    #[test]
    fn empty_generated_slugs_are_rejected_without_mutating_the_registry() {
        for name in ["!!!", "💥", "\u{feff}"] {
            let mut registry = LaneRegistry::default_direct();
            let before = registry.clone();

            assert_eq!(
                LaneRegistry::validate_create_name(name),
                Err(LaneCreateError::InvalidGeneratedId),
                "expected empty generated slug to be rejected: {name:?}"
            );
            assert_eq!(
                registry.create(name),
                Err(LaneCreateError::InvalidGeneratedId),
                "expected create to reject empty generated slug: {name:?}"
            );
            assert_eq!(
                registry, before,
                "rejected name mutated the registry: {name:?}"
            );

            let reply = execute(&mut registry, MetaAction::New(name.to_owned()));
            assert_eq!(
                reply,
                "I couldn't create that conversation name. Please use a shorter name."
            );
            assert_eq!(
                registry, before,
                "meta rejection mutated the registry: {name:?}"
            );
        }
    }

    #[test]
    fn valid_lane_slugs_preserve_internal_dash_runs() {
        let mut registry = LaneRegistry::default_direct();

        let lane = registry.create("A..B").unwrap();

        assert_eq!(lane.id, "telepathy:repo:a--b");
        assert_eq!(lane.name, "a--b");
    }

    #[test]
    fn lane_count_boundary_preserves_existing_lanes_and_rejects_new_ones_without_mutation() {
        let mut registry = full_registry();
        let existing_name = registry.lanes[1].name.clone();
        let before = registry.clone();

        assert_eq!(
            registry.create(&existing_name).unwrap().id,
            before.lanes[1].id,
            "an existing lane remains usable at capacity"
        );
        assert_eq!(
            registry.create("one too many"),
            Err(LaneCreateError::CapacityReached)
        );
        assert_eq!(
            registry, before,
            "capacity rejection must not mutate memory"
        );
        assert_eq!(
            execute(&mut registry, MetaAction::New("one too many".into())),
            LANE_CAPACITY_ERROR_MESSAGE
        );
        assert_eq!(
            registry, before,
            "meta capacity reply must not mutate memory"
        );
    }

    #[test]
    fn over_capacity_snapshots_fail_closed_on_load_and_before_save_replacement() {
        let path = std::env::temp_dir().join(format!(
            "telepathy-lanes-over-capacity-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let registry = full_registry();
        registry.save(&path).unwrap();
        let previous = fs::read_to_string(&path).unwrap();

        let mut over_capacity = registry.clone();
        over_capacity.lanes.push(Lane {
            id: "telepathy:repo:overflow".into(),
            name: "overflow".into(),
            created_at: now_iso(),
            last_active: now_iso(),
            interactions: None,
        });
        let error = over_capacity.save(&path).unwrap_err();
        assert!(matches!(error, LaneRegistrySaveError::InvalidRegistry(_)));
        assert_eq!(fs::read_to_string(&path).unwrap(), previous);

        let persisted_over_capacity = serde_json::to_string(&over_capacity).unwrap();
        fs::write(&path, &persisted_over_capacity).unwrap();
        assert!(std::panic::catch_unwind(|| LaneRegistry::load(&path)).is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), persisted_over_capacity);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn lane_metadata_string_and_timestamp_contract_has_exact_cross_runtime_boundaries() {
        for name in [
            "a".repeat(MAX_LANE_NAME_UTF8_BYTES),
            "é".repeat(MAX_LANE_NAME_UTF8_BYTES / 2),
        ] {
            assert!(
                is_valid_persisted_lane_name(&name),
                "expected exact boundary name to be valid: {name:?}"
            );
        }
        for name in [
            "a".repeat(MAX_LANE_NAME_UTF8_BYTES + 1),
            "é".repeat(MAX_LANE_NAME_UTF8_BYTES / 2 + 1),
        ] {
            assert!(
                !is_valid_persisted_lane_name(&name),
                "expected over-bound name to be invalid: {name:?}"
            );
        }

        for timestamp in [
            "epoch-ms:9007199254740991",
            "2024-02-29T23:59:59.999Z",
            "9999-12-31T23:59:59.999Z",
        ] {
            assert!(
                is_valid_lane_timestamp(timestamp),
                "expected valid {timestamp}"
            );
        }
        for timestamp in [
            "epoch-ms:9007199254740992",
            "epoch-ms:-1",
            "epoch-ms:1.5",
            "2023-02-29T00:00:00.000Z",
            "2024-01-01T24:00:00.000Z",
            "2024-01-01T00:00:00.000+00:00",
        ] {
            assert!(
                !is_valid_lane_timestamp(timestamp),
                "expected invalid {timestamp}"
            );
        }
    }

    #[test]
    fn invalid_metadata_snapshot_and_save_fail_closed_without_overwrite() {
        let path = std::env::temp_dir().join(format!(
            "telepathy-lanes-invalid-metadata-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let registry = LaneRegistry::default_direct();
        registry.save(&path).unwrap();
        let previous = fs::read_to_string(&path).unwrap();

        let mut over_name = registry.clone();
        over_name.lanes[0].name = "x".repeat(MAX_LANE_NAME_UTF8_BYTES + 1);
        assert!(matches!(
            over_name.save(&path),
            Err(LaneRegistrySaveError::InvalidRegistry(_))
        ));
        assert_eq!(fs::read_to_string(&path).unwrap(), previous);

        let mut over_timestamp = registry.clone();
        over_timestamp.lanes[0].last_active = "epoch-ms:9007199254740992".into();
        let invalid_snapshot = serde_json::to_string(&over_timestamp).unwrap();
        fs::write(&path, &invalid_snapshot).unwrap();
        assert!(std::panic::catch_unwind(|| LaneRegistry::load(&path)).is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), invalid_snapshot);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn touching_unknown_lane_returns_not_found_without_mutating_registry() {
        let mut registry = LaneRegistry::default_direct();
        let before = registry.clone();

        assert!(registry.touch("telepathy:missing").is_none());
        assert_eq!(registry, before);
    }

    #[test]
    fn invalid_persisted_lane_id_is_rejected_at_load() {
        let path = std::env::temp_dir().join(format!(
            "telepathy-lanes-invalid-id-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(
            &path,
            r#"{"lanes":[{"id":"telepathy:repo:bad\"quote","name":"bad","created_at":"now","last_active":"now"}],"active_id":"telepathy:repo:bad\"quote","previous_id":"telepathy:repo:bad\"quote"}"#,
        )
        .unwrap();
        assert!(std::panic::catch_unwind(|| LaneRegistry::load(&path)).is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn relative_registry_path_can_be_saved_and_reloaded() {
        let path = PathBuf::from(format!(
            ".telepathy-lanes-relative-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let registry = LaneRegistry::default_direct();
        registry.save(&path).unwrap();
        assert_eq!(LaneRegistry::load(&path), registry);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn save_returns_a_definite_pre_rename_error_without_replacing_the_snapshot() {
        let blocker = std::env::temp_dir().join(format!(
            "telepathy-lanes-save-blocker-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&blocker, "not a directory").unwrap();
        let path = blocker.join("lanes.json");

        let error = LaneRegistry::default_direct().save(&path).unwrap_err();
        assert!(matches!(error, LaneRegistrySaveError::PreRename(_)));
        assert!(!path.exists());
        let _ = fs::remove_file(blocker);
    }

    #[test]
    fn post_rename_failure_is_ambiguous_and_leaves_the_replacement_visible() {
        let path = std::env::temp_dir().join(format!(
            "telepathy-lanes-save-post-rename-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&path, "old snapshot").unwrap();

        let error = atomic_write_with_stage_hook(&path, "new snapshot", |stage| {
            if stage == AtomicWriteStage::AfterRename {
                return Err(std::io::Error::other("injected parent fsync failure"));
            }
            Ok(())
        })
        .unwrap_err();
        assert!(matches!(
            error,
            LaneRegistrySaveError::AmbiguousPostRename(_)
        ));
        assert_eq!(fs::read_to_string(&path).unwrap(), "new snapshot");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn nested_directory_creation_syncs_every_new_parent_before_snapshot_write() {
        let root = std::env::temp_dir().join(format!(
            "telepathy-lanes-nested-sync-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = root.join("one").join("two").join("lanes.json");
        let mut parent_syncs = 0;

        atomic_write_with_stage_hook(&path, "durable snapshot", |stage| {
            if stage == AtomicWriteStage::BeforeNewDirectoryParentSync {
                parent_syncs += 1;
            }
            Ok(())
        })
        .unwrap();

        assert_eq!(parent_syncs, 3, "root, one, and two are all new");
        assert_eq!(fs::read_to_string(&path).unwrap(), "durable snapshot");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn nested_directory_parent_sync_failure_is_definite_pre_rename() {
        let root = std::env::temp_dir().join(format!(
            "telepathy-lanes-nested-sync-failure-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = root.join("one").join("two").join("lanes.json");
        let mut parent_syncs = 0;

        let error = atomic_write_with_stage_hook(&path, "must not be written", |stage| {
            if stage == AtomicWriteStage::BeforeNewDirectoryParentSync {
                parent_syncs += 1;
                if parent_syncs == 2 {
                    return Err(std::io::Error::other(
                        "injected newly-created directory parent sync failure",
                    ));
                }
            }
            Ok(())
        })
        .unwrap_err();

        assert!(matches!(error, LaneRegistrySaveError::PreRename(_)));
        assert_eq!(parent_syncs, 2);
        assert!(!path.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn interaction_count_above_json_safe_limit_is_rejected_before_snapshot_write() {
        let path = std::env::temp_dir().join(format!(
            "telepathy-lanes-unsafe-interactions-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&path, "previous snapshot").unwrap();
        let mut registry = LaneRegistry::default_direct();
        registry.lanes[0].interactions = Some(MAX_SAFE_SEQUENCE + 1);

        let error = registry.save(&path).unwrap_err();

        assert!(matches!(error, LaneRegistrySaveError::InvalidRegistry(_)));
        assert_eq!(fs::read_to_string(&path).unwrap(), "previous snapshot");
        let _ = fs::remove_file(path);
    }
}
