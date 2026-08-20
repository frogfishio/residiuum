//! Canonical compiled predicate payloads shared by builders and executors.
//!
//! These types are the execution-level result of RQL/RRE compilation. They
//! contain no host closure and have one deterministic representation.

use crate::canonical::{decode_key, encode_key_map};
use crate::cbor::{self, Value};
use crate::encoding::ValueEncoding;
use crate::error::AtomicsError;
use crate::id::{CollectionId, VersionId};
use crate::outcome::AtomicRefuseReason;
use crate::plan::{CanonicalKey, CanonicalKeyKind};
use num_bigint::{BigInt, Sign};
use std::cmp::Ordering;

const EXACT_SCALAR_PROFILE_V1: u64 = 1;
const BOUNDED_RANGE_PROFILE_V1: u64 = 1;
/// Stable total-order profile used by exact bounded ranges.
pub const CANONICAL_KEY_ORDER_V1: u64 = 1;

/// Hard ceiling for one exact range result commitment.
pub const MAX_RANGE_RESULT_ENTRIES: u32 = 4_096;

/// Hard ceiling for authoritative collection identities examined by one range.
pub const MAX_RANGE_EXAMINED: u32 = 1_000_000;

const DOMAIN_RANGE_COVERAGE: &[u8] = b"RESIDIUUM-AUTHORITATIVE-PRIMARY-KEY-COVERAGE-V1";
const DOMAIN_RANGE_IDENTITY: &[u8] = b"RESIDIUUM-ATOMIC-RANGE-IDENTITY-V1";
const DOMAIN_RANGE_RESULT: &[u8] = b"RESIDIUUM-ATOMIC-RANGE-RESULT-V1";

/// A compiled exact-scalar equality predicate.
///
/// Equality is over admitted canonical scalar bytes under the collection's
/// frozen value encoding. Absence never equals a scalar value; JSON document
/// predicates are compiled by the RQL/RRE layer, not interpreted here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExactScalarEquality {
    encoding: ValueEncoding,
    expected: Vec<u8>,
}

impl ExactScalarEquality {
    /// Construct after validating the expected scalar's canonical encoding.
    pub fn new(encoding: ValueEncoding, expected: &[u8]) -> Result<Self, AtomicsError> {
        encoding.admit_bytes(expected)?;
        Ok(Self {
            encoding,
            expected: expected.to_vec(),
        })
    }

    /// Frozen scalar encoding.
    pub const fn encoding(&self) -> ValueEncoding {
        self.encoding
    }

    /// Canonical expected scalar bytes.
    pub fn expected(&self) -> &[u8] {
        &self.expected
    }

    /// Total evaluation. An absent object does not equal a present scalar.
    pub fn evaluate(&self, observed: Option<&[u8]>) -> bool {
        observed == Some(self.expected.as_slice())
    }
}

/// One exact key/version observation used to commit a bounded range result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeEntry {
    /// Canonical application key.
    pub key: CanonicalKey,
    /// Current establishing event identity.
    pub version: VersionId,
}

/// Canonical, completely observed bounded key range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundedKeyRange {
    collection_id: CollectionId,
    key_kind: CanonicalKeyKind,
    lower: CanonicalKey,
    lower_inclusive: bool,
    upper: CanonicalKey,
    upper_inclusive: bool,
    examination_limit: u32,
    expected_count: u32,
    expected_result: [u8; 32],
    range_identity: [u8; 32],
}

impl BoundedKeyRange {
    /// Compile an exact observation into a deterministic bounded-range predicate.
    ///
    /// `entries` may arrive in any physical scan order. They are validated,
    /// canonically sorted, and committed as exact `(key, version)` identities.
    pub fn observed(
        collection_id: CollectionId,
        lower: CanonicalKey,
        lower_inclusive: bool,
        upper: CanonicalKey,
        upper_inclusive: bool,
        examination_limit: u32,
        entries: &[RangeEntry],
    ) -> Result<Self, AtomicsError> {
        let key_kind = lower.kind();
        if upper.kind() != key_kind
            || examination_limit == 0
            || examination_limit > MAX_RANGE_EXAMINED
            || entries.len() > MAX_RANGE_RESULT_ENTRIES as usize
        {
            return Err(malformed());
        }
        let bound_order = compare_canonical_keys(&lower, &upper)?;
        if bound_order == Ordering::Greater
            || (bound_order == Ordering::Equal && !(lower_inclusive && upper_inclusive))
        {
            return Err(malformed());
        }
        let mut ordered = entries.to_vec();
        ordered.sort_by(|left, right| {
            compare_canonical_keys(&left.key, &right.key).unwrap_or(Ordering::Equal)
        });
        for (index, entry) in ordered.iter().enumerate() {
            if entry.key.kind() != key_kind
                || !key_in_range(&entry.key, &lower, lower_inclusive, &upper, upper_inclusive)?
                || index > 0
                    && compare_canonical_keys(&ordered[index - 1].key, &entry.key)?
                        != Ordering::Less
            {
                return Err(malformed());
            }
        }
        let expected_count = u32::try_from(ordered.len()).map_err(|_| malformed())?;
        if expected_count > examination_limit {
            return Err(malformed());
        }
        let expected_result = range_result_commitment(collection_id, &ordered);
        let range_identity = range_identity(
            collection_id,
            key_kind,
            &lower,
            lower_inclusive,
            &upper,
            upper_inclusive,
        );
        Ok(Self {
            collection_id,
            key_kind,
            lower,
            lower_inclusive,
            upper,
            upper_inclusive,
            examination_limit,
            expected_count,
            expected_result,
            range_identity,
        })
    }

    /// Bound collection identity.
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    /// Frozen key kind and mathematical order domain.
    pub const fn key_kind(&self) -> CanonicalKeyKind {
        self.key_kind
    }

    /// Lower bound.
    pub fn lower(&self) -> &CanonicalKey {
        &self.lower
    }

    /// Whether the lower bound participates.
    pub const fn lower_inclusive(&self) -> bool {
        self.lower_inclusive
    }

    /// Upper bound.
    pub fn upper(&self) -> &CanonicalKey {
        &self.upper
    }

    /// Whether the upper bound participates.
    pub const fn upper_inclusive(&self) -> bool {
        self.upper_inclusive
    }

    /// Maximum authoritative identities the executor may inspect.
    pub const fn examination_limit(&self) -> u32 {
        self.examination_limit
    }

    /// Exact expected result cardinality.
    pub const fn expected_count(&self) -> u32 {
        self.expected_count
    }

    /// Stable semantic identity of the collection, order domain and bounds.
    pub const fn range_identity(&self) -> [u8; 32] {
        self.range_identity
    }

    /// True when `key` belongs to this exact mathematical range.
    pub fn contains(&self, key: &CanonicalKey) -> Result<bool, AtomicsError> {
        key_in_range(
            key,
            &self.lower,
            self.lower_inclusive,
            &self.upper,
            self.upper_inclusive,
        )
    }

    /// Validate a completely enumerated current result against the observation.
    pub fn matches_entries(&self, entries: &[RangeEntry]) -> Result<bool, AtomicsError> {
        if entries.len() != self.expected_count as usize {
            return Ok(false);
        }
        let mut ordered = entries.to_vec();
        ordered.sort_by(|left, right| {
            compare_canonical_keys(&left.key, &right.key).unwrap_or(Ordering::Equal)
        });
        Ok(range_result_commitment(self.collection_id, &ordered) == self.expected_result)
    }
}

/// Deterministically encode a bounded-range compiled payload.
pub fn encode_bounded_range_payload(range: &BoundedKeyRange) -> Result<Vec<u8>, AtomicsError> {
    cbor::encode_map(&[
        (1, Value::Uint(BOUNDED_RANGE_PROFILE_V1)),
        (2, Value::Bytes(range.collection_id.to_bytes().to_vec())),
        (3, Value::Uint(u64::from(range.key_kind.wire_code()))),
        (4, Value::Uint(CANONICAL_KEY_ORDER_V1)),
        (5, Value::Map(encode_key_map(&range.lower)?)),
        (6, Value::Uint(u64::from(range.lower_inclusive))),
        (7, Value::Map(encode_key_map(&range.upper)?)),
        (8, Value::Uint(u64::from(range.upper_inclusive))),
        (9, Value::Bytes(range_coverage_domain().to_vec())),
        (10, Value::Uint(u64::from(range.examination_limit))),
        (11, Value::Uint(u64::from(range.expected_count))),
        (12, Value::Bytes(range.expected_result.to_vec())),
        (13, Value::Bytes(range.range_identity.to_vec())),
    ])
}

/// Decode and fully validate a bounded-range compiled payload.
pub fn decode_bounded_range_payload(bytes: &[u8]) -> Result<BoundedKeyRange, AtomicsError> {
    let map = cbor::decode_map(bytes).map_err(|_| malformed())?;
    if map.len() != 13 || require_uint(&map, 1)? != BOUNDED_RANGE_PROFILE_V1 {
        return Err(malformed());
    }
    let collection_id = CollectionId::from_bytes(require_fixed::<16>(&map, 2)?)?;
    let key_kind = u8::try_from(require_uint(&map, 3)?)
        .ok()
        .and_then(CanonicalKeyKind::from_wire_code)
        .ok_or_else(malformed)?;
    if require_uint(&map, 4)? != CANONICAL_KEY_ORDER_V1
        || require_fixed::<32>(&map, 9)? != range_coverage_domain()
    {
        return Err(malformed());
    }
    let lower = decode_key(require_value(&map, 5)?)?;
    let lower_inclusive = require_bool(&map, 6)?;
    let upper = decode_key(require_value(&map, 7)?)?;
    let upper_inclusive = require_bool(&map, 8)?;
    let examination_limit = u32::try_from(require_uint(&map, 10)?).map_err(|_| malformed())?;
    let expected_count = u32::try_from(require_uint(&map, 11)?).map_err(|_| malformed())?;
    let expected_result = require_fixed::<32>(&map, 12)?;
    let encoded_identity = require_fixed::<32>(&map, 13)?;
    if lower.kind() != key_kind
        || upper.kind() != key_kind
        || examination_limit == 0
        || examination_limit > MAX_RANGE_EXAMINED
        || expected_count > MAX_RANGE_RESULT_ENTRIES
        || expected_count > examination_limit
    {
        return Err(malformed());
    }
    let bound_order = compare_canonical_keys(&lower, &upper)?;
    if bound_order == Ordering::Greater
        || (bound_order == Ordering::Equal && !(lower_inclusive && upper_inclusive))
    {
        return Err(malformed());
    }
    let expected_identity = range_identity(
        collection_id,
        key_kind,
        &lower,
        lower_inclusive,
        &upper,
        upper_inclusive,
    );
    if encoded_identity != expected_identity {
        return Err(malformed());
    }
    Ok(BoundedKeyRange {
        collection_id,
        key_kind,
        lower,
        lower_inclusive,
        upper,
        upper_inclusive,
        examination_limit,
        expected_count,
        expected_result,
        range_identity: expected_identity,
    })
}

/// Mathematical order for the four canonical Atomic key kinds.
pub fn compare_canonical_keys(
    left: &CanonicalKey,
    right: &CanonicalKey,
) -> Result<Ordering, AtomicsError> {
    if left.kind() != right.kind() {
        return Err(malformed());
    }
    Ok(match (left, right) {
        (CanonicalKey::String(left), CanonicalKey::String(right)) => left.cmp(right),
        (CanonicalKey::Opaque(left), CanonicalKey::Opaque(right)) => left.cmp(right),
        (CanonicalKey::Integer(left), CanonicalKey::Integer(right)) => {
            BigInt::from_signed_bytes_be(left).cmp(&BigInt::from_signed_bytes_be(right))
        }
        (
            CanonicalKey::Decimal {
                coefficient: left,
                scale: left_scale,
            },
            CanonicalKey::Decimal {
                coefficient: right,
                scale: right_scale,
            },
        ) => {
            let mathematical = decimal_cmp(left, *left_scale, right, *right_scale);
            if mathematical == Ordering::Equal {
                left_scale.cmp(right_scale)
            } else {
                mathematical
            }
        }
        _ => unreachable!("matching key kinds have matching variants"),
    })
}

fn decimal_cmp(left: &[u8], left_scale: i64, right: &[u8], right_scale: i64) -> Ordering {
    let left = BigInt::from_signed_bytes_be(left);
    let right = BigInt::from_signed_bytes_be(right);
    let sign_order = sign_rank(left.sign()).cmp(&sign_rank(right.sign()));
    if sign_order != Ordering::Equal {
        return sign_order;
    }
    if left.sign() == Sign::NoSign {
        return Ordering::Equal;
    }
    let left_digits = left.magnitude().to_str_radix(10);
    let right_digits = right.magnitude().to_str_radix(10);
    let left_exponent = left_digits.len() as i128 - i128::from(left_scale);
    let right_exponent = right_digits.len() as i128 - i128::from(right_scale);
    let mut magnitude = left_exponent.cmp(&right_exponent);
    if magnitude == Ordering::Equal {
        let length = left_digits.len().max(right_digits.len());
        magnitude = (0..length)
            .map(|index| {
                let left = left_digits.as_bytes().get(index).copied().unwrap_or(b'0');
                let right = right_digits.as_bytes().get(index).copied().unwrap_or(b'0');
                left.cmp(&right)
            })
            .find(|order| *order != Ordering::Equal)
            .unwrap_or(Ordering::Equal);
    }
    if left.sign() == Sign::Minus {
        magnitude.reverse()
    } else {
        magnitude
    }
}

const fn sign_rank(sign: Sign) -> u8 {
    match sign {
        Sign::Minus => 0,
        Sign::NoSign => 1,
        Sign::Plus => 2,
    }
}

fn key_in_range(
    key: &CanonicalKey,
    lower: &CanonicalKey,
    lower_inclusive: bool,
    upper: &CanonicalKey,
    upper_inclusive: bool,
) -> Result<bool, AtomicsError> {
    let lower_order = compare_canonical_keys(key, lower)?;
    let upper_order = compare_canonical_keys(key, upper)?;
    Ok(
        (lower_order == Ordering::Greater || lower_inclusive && lower_order == Ordering::Equal)
            && (upper_order == Ordering::Less || upper_inclusive && upper_order == Ordering::Equal),
    )
}

/// Identity of the complete authoritative primary-key coverage domain.
pub fn range_coverage_domain() -> [u8; 32] {
    *blake3::hash(DOMAIN_RANGE_COVERAGE).as_bytes()
}

fn range_identity(
    collection_id: CollectionId,
    key_kind: CanonicalKeyKind,
    lower: &CanonicalKey,
    lower_inclusive: bool,
    upper: &CanonicalKey,
    upper_inclusive: bool,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN_RANGE_IDENTITY);
    hasher.update(collection_id.as_bytes());
    hasher.update(&[key_kind.wire_code(), CANONICAL_KEY_ORDER_V1 as u8]);
    hash_key(&mut hasher, lower);
    hasher.update(&[u8::from(lower_inclusive)]);
    hash_key(&mut hasher, upper);
    hasher.update(&[u8::from(upper_inclusive)]);
    hasher.update(&range_coverage_domain());
    *hasher.finalize().as_bytes()
}

fn range_result_commitment(collection_id: CollectionId, entries: &[RangeEntry]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN_RANGE_RESULT);
    hasher.update(collection_id.as_bytes());
    hasher.update(&(entries.len() as u32).to_be_bytes());
    for entry in entries {
        hash_key(&mut hasher, &entry.key);
        hasher.update(entry.version.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn hash_key(hasher: &mut blake3::Hasher, key: &CanonicalKey) {
    let bytes = key.identity_bytes();
    hasher.update(&(bytes.len() as u32).to_be_bytes());
    hasher.update(&bytes);
}

/// Deterministically encode an exact-scalar compiled payload.
pub fn encode_exact_scalar_payload(
    predicate: &ExactScalarEquality,
) -> Result<Vec<u8>, AtomicsError> {
    cbor::encode_map(&[
        (1, Value::Uint(EXACT_SCALAR_PROFILE_V1)),
        (2, Value::Uint(u64::from(predicate.encoding.wire_code()))),
        (3, Value::Bytes(predicate.expected.clone())),
    ])
}

/// Decode and fully validate an exact-scalar compiled payload.
pub fn decode_exact_scalar_payload(bytes: &[u8]) -> Result<ExactScalarEquality, AtomicsError> {
    let map = cbor::decode_map(bytes).map_err(|_| malformed())?;
    if map.len() != 3 {
        return Err(malformed());
    }
    let profile = require_uint(&map, 1)?;
    if profile != EXACT_SCALAR_PROFILE_V1 {
        return Err(malformed());
    }
    let encoding = u8::try_from(require_uint(&map, 2)?)
        .ok()
        .and_then(ValueEncoding::from_wire_code)
        .ok_or_else(malformed)?;
    let expected = require_bytes(&map, 3)?;
    ExactScalarEquality::new(encoding, &expected).map_err(|_| malformed())
}

fn require_uint(map: &[(u64, Value)], field: u64) -> Result<u64, AtomicsError> {
    match map
        .iter()
        .find(|(key, _)| *key == field)
        .map(|(_, value)| value)
    {
        Some(Value::Uint(value)) => Ok(*value),
        _ => Err(malformed()),
    }
}

fn require_bytes(map: &[(u64, Value)], field: u64) -> Result<Vec<u8>, AtomicsError> {
    match map
        .iter()
        .find(|(key, _)| *key == field)
        .map(|(_, value)| value)
    {
        Some(Value::Bytes(value)) => Ok(value.clone()),
        _ => Err(malformed()),
    }
}

fn require_fixed<const N: usize>(
    map: &[(u64, Value)],
    field: u64,
) -> Result<[u8; N], AtomicsError> {
    require_bytes(map, field)?
        .try_into()
        .map_err(|_| malformed())
}

fn require_value(map: &[(u64, Value)], field: u64) -> Result<&Value, AtomicsError> {
    map.iter()
        .find(|(key, _)| *key == field)
        .map(|(_, value)| value)
        .ok_or_else(malformed)
}

fn require_bool(map: &[(u64, Value)], field: u64) -> Result<bool, AtomicsError> {
    match require_uint(map, field)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(malformed()),
    }
}

fn malformed() -> AtomicsError {
    AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_scalar_payload_round_trips_and_is_total() {
        let predicate = ExactScalarEquality::new(ValueEncoding::Integer, &[1]).unwrap();
        let bytes = encode_exact_scalar_payload(&predicate).unwrap();
        assert_eq!(decode_exact_scalar_payload(&bytes).unwrap(), predicate);
        assert!(predicate.evaluate(Some(&[1])));
        assert!(!predicate.evaluate(Some(&[2])));
        assert!(!predicate.evaluate(None));
    }

    #[test]
    fn malformed_or_noncanonical_payload_is_refused() {
        let bad_integer = cbor::encode_map(&[
            (1, Value::Uint(1)),
            (
                2,
                Value::Uint(u64::from(ValueEncoding::Integer.wire_code())),
            ),
            (3, Value::Bytes(vec![0, 1])),
        ])
        .unwrap();
        assert_eq!(
            decode_exact_scalar_payload(&bad_integer).unwrap_err(),
            malformed()
        );
        assert_eq!(decode_exact_scalar_payload(&[0]).unwrap_err(), malformed());
    }

    fn cid(first: u8) -> CollectionId {
        let mut bytes = [0; 16];
        bytes[0] = first;
        CollectionId::from_bytes(bytes).unwrap()
    }

    fn vid(first: u8) -> VersionId {
        let mut bytes = [0; 16];
        bytes[0] = first;
        VersionId::from_bytes(bytes).unwrap()
    }

    #[test]
    fn mathematical_key_order_is_not_subject_byte_order() {
        assert_eq!(
            compare_canonical_keys(&CanonicalKey::integer(-129), &CanonicalKey::integer(-1))
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            compare_canonical_keys(&CanonicalKey::integer(127), &CanonicalKey::integer(128))
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            compare_canonical_keys(
                &CanonicalKey::decimal(10, 1),
                &CanonicalKey::decimal(100, 2),
            )
            .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            compare_canonical_keys(
                &CanonicalKey::decimal(-11, 1),
                &CanonicalKey::decimal(-105, 2),
            )
            .unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn decimal_order_matches_exact_cross_scaled_arithmetic() {
        for left_coefficient in -20i128..=20 {
            for right_coefficient in -20i128..=20 {
                for left_scale in -3i64..=3 {
                    for right_scale in -3i64..=3 {
                        let common = left_scale.max(right_scale);
                        let left = BigInt::from(left_coefficient)
                            * BigInt::from(10u8).pow((common - left_scale) as u32);
                        let right = BigInt::from(right_coefficient)
                            * BigInt::from(10u8).pow((common - right_scale) as u32);
                        let mathematical = left.cmp(&right);
                        let expected = if mathematical == Ordering::Equal {
                            left_scale.cmp(&right_scale)
                        } else {
                            mathematical
                        };
                        assert_eq!(
                            compare_canonical_keys(
                                &CanonicalKey::decimal(left_coefficient, left_scale),
                                &CanonicalKey::decimal(right_coefficient, right_scale),
                            )
                            .unwrap(),
                            expected,
                            "{left_coefficient}e-{left_scale} versus {right_coefficient}e-{right_scale}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn bounded_range_payload_round_trips_and_commits_exact_versions() {
        let range = BoundedKeyRange::observed(
            cid(1),
            CanonicalKey::integer(-10),
            true,
            CanonicalKey::integer(10),
            false,
            100,
            &[
                RangeEntry {
                    key: CanonicalKey::integer(2),
                    version: vid(2),
                },
                RangeEntry {
                    key: CanonicalKey::integer(-2),
                    version: vid(1),
                },
            ],
        )
        .unwrap();
        let bytes = encode_bounded_range_payload(&range).unwrap();
        assert_eq!(decode_bounded_range_payload(&bytes).unwrap(), range);
        assert!(range.contains(&CanonicalKey::integer(-10)).unwrap());
        assert!(!range.contains(&CanonicalKey::integer(10)).unwrap());
        assert!(range
            .matches_entries(&[
                RangeEntry {
                    key: CanonicalKey::integer(-2),
                    version: vid(1),
                },
                RangeEntry {
                    key: CanonicalKey::integer(2),
                    version: vid(2),
                },
            ])
            .unwrap());
        assert!(!range
            .matches_entries(&[RangeEntry {
                key: CanonicalKey::integer(-2),
                version: vid(9),
            }])
            .unwrap());
    }

    #[test]
    fn bounded_range_refuses_empty_geometry_duplicates_and_unbounded_work() {
        assert_eq!(
            BoundedKeyRange::observed(
                cid(1),
                CanonicalKey::string("same"),
                true,
                CanonicalKey::string("same"),
                false,
                10,
                &[],
            )
            .unwrap_err(),
            malformed()
        );
        let duplicate = RangeEntry {
            key: CanonicalKey::string("k"),
            version: vid(1),
        };
        assert_eq!(
            BoundedKeyRange::observed(
                cid(1),
                CanonicalKey::string("a"),
                true,
                CanonicalKey::string("z"),
                true,
                10,
                &[duplicate.clone(), duplicate],
            )
            .unwrap_err(),
            malformed()
        );
        assert_eq!(
            BoundedKeyRange::observed(
                cid(1),
                CanonicalKey::string("a"),
                true,
                CanonicalKey::string("z"),
                true,
                MAX_RANGE_EXAMINED + 1,
                &[],
            )
            .unwrap_err(),
            malformed()
        );

        let valid = BoundedKeyRange::observed(
            cid(1),
            CanonicalKey::string("a"),
            true,
            CanonicalKey::string("z"),
            true,
            10,
            &[],
        )
        .unwrap();
        let encoded = encode_bounded_range_payload(&valid).unwrap();
        let mut map = cbor::decode_map(&encoded).unwrap();
        let (_, Value::Bytes(coverage)) = map.iter_mut().find(|(field, _)| *field == 9).unwrap()
        else {
            panic!("coverage field changed shape")
        };
        coverage[0] ^= 1;
        let corrupted = cbor::encode_map(&map).unwrap();
        assert_eq!(
            decode_bounded_range_payload(&corrupted).unwrap_err(),
            malformed()
        );
    }
}
