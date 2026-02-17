import numpy as np
import numpy.typing as npt
import pandas as pd

from ice_keeper.zorder_udf import (
    INTERLEAVE_TABLE,
    detect_type_and_normalize,
    interleave_with_lookup,
    zorder2Tuple,
)


def test_detect_type_and_normalize() -> None:
    # Test with integer series
    int_series: pd.Series = pd.Series([1, 2, 3, None])
    result: npt.NDArray[np.uint8] = detect_type_and_normalize(int_series)
    assert result.shape == (4, 8)
    assert np.all(result[3] == 0)  # Check null handling

    # Test with string series
    str_series: pd.Series = pd.Series(["abc", "def", None])
    result = detect_type_and_normalize(str_series)
    assert result.shape == (3, 8)
    assert np.all(result[2] == 0)  # Check null handling

    # Test with datetime series
    datetime_series: pd.Series = pd.Series([pd.Timestamp("2026-01-30"), None])
    result = detect_type_and_normalize(datetime_series)
    assert result.shape == (2, 8)
    assert np.all(result[1] == 0)  # Check null handling


def test_interleave_with_lookup() -> None:
    # Prepare test data
    bytes1: npt.NDArray[np.uint8] = np.array([[0xFF, 0x00], [0xAA, 0x55]], dtype=np.uint8)
    bytes2: npt.NDArray[np.uint8] = np.array([[0x00, 0xFF], [0x55, 0xAA]], dtype=np.uint8)
    bytes1 = np.pad(bytes1, ((0, 0), (0, 6)))  # Pad to shape (n, 8)
    bytes2 = np.pad(bytes2, ((0, 0), (0, 6)))

    # Call the function
    result: npt.NDArray[np.uint8] = interleave_with_lookup(bytes1, bytes2)

    # Verify the result shape
    assert result.shape == (2, 16)


def test_interleave_table() -> None:
    # Verify the INTERLEAVE_TABLE dimensions
    assert INTERLEAVE_TABLE.shape == (256, 256)

    # Verify specific values in the table
    assert INTERLEAVE_TABLE[0, 0] == 0
    assert INTERLEAVE_TABLE[255, 255] == 0xFFFF  # noqa: PLR2004


def test_interleave_1_with_1() -> None:
    # Prepare test data for interleaving 1 with 1
    bytes1: npt.NDArray[np.uint8] = np.zeros((1, 8), dtype=np.uint8)
    bytes2: npt.NDArray[np.uint8] = np.zeros((1, 8), dtype=np.uint8)

    # Set the first byte to 1
    bytes1[0, 7] = 1
    bytes2[0, 7] = 1

    # Call the interleave_with_lookup function
    result: npt.NDArray[np.uint8] = interleave_with_lookup(bytes1, bytes2)

    # Verify that the interleaved result is 3
    assert result[0, 14] == 0
    assert result[0, 15] == 3  # noqa: PLR2004


# ============================================================================
# Tests for detect_type_and_normalize
# ============================================================================


def test_small_positive_integers() -> None:
    """Test small positive integers (32-bit range)."""
    int_series = pd.Series([1, 10, 100, 1000], dtype=np.int64)
    result = detect_type_and_normalize(int_series)
    assert result.shape == (4, 8)
    # Verify all results are non-zero
    assert not np.all(result == 0)


def test_large_positive_integers() -> None:
    """Test large positive integers (beyond 32-bit)."""
    large_series = pd.Series([10_000_000_000, 100_000_000_000], dtype=np.int64)
    result = detect_type_and_normalize(large_series)
    assert result.shape == (2, 8)
    assert not np.all(result == 0)


def test_negative_integers() -> None:
    """Test negative integers are normalized correctly."""
    neg_series = pd.Series([-1, -10, -100, -1000], dtype=np.int64)
    result = detect_type_and_normalize(neg_series)
    assert result.shape == (4, 8)
    # Negative numbers should produce non-zero bytes
    assert not np.all(result == 0)


def test_negative_integer_ordering() -> None:
    """Test that negative integers maintain correct ordering after normalization."""
    # Create series with values that should sort in this order
    values = pd.Series([-1000, -100, -10, -1, 0, 1, 10, 100, 1000], dtype=np.int64)
    result = detect_type_and_normalize(values)

    # Convert each 8-byte row to an integer for comparison
    as_ints = [int.from_bytes(row.tobytes(), byteorder="big") for row in result]

    # Check that the byte representation maintains the ordering
    assert as_ints == sorted(as_ints), "Negative integers should maintain numerical order"


def test_around_zero() -> None:
    """Test integers around zero maintain correct ordering."""
    values = pd.Series([-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5], dtype=np.int64)
    result = detect_type_and_normalize(values)

    as_ints = [int.from_bytes(row.tobytes(), byteorder="big") for row in result]
    assert as_ints == sorted(as_ints), "Values around zero should maintain order"


def test_min_max_int32() -> None:
    """Test 32-bit integer boundaries."""
    boundary_series = pd.Series([-2147483648, 2147483647], dtype=np.int64)
    result = detect_type_and_normalize(boundary_series)
    assert result.shape == (2, 8)

    # Verify ordering is preserved
    as_ints = [int.from_bytes(row.tobytes(), byteorder="big") for row in result]
    assert as_ints[0] < as_ints[1], "Min should be less than max"


def test_min_max_int64() -> None:
    """Test 64-bit integer boundaries."""
    boundary_series = pd.Series([-9223372036854775808, 9223372036854775807], dtype=np.int64)
    result = detect_type_and_normalize(boundary_series)
    assert result.shape == (2, 8)

    as_ints = [int.from_bytes(row.tobytes(), byteorder="big") for row in result]
    assert as_ints[0] < as_ints[1], "Min int64 should be less than max int64"


def test_zero_integer() -> None:
    """Test zero is handled correctly."""
    zero_series = pd.Series([0], dtype=np.int64)
    result = detect_type_and_normalize(zero_series)
    assert result.shape == (1, 8)
    # Zero should produce a specific non-zero pattern (due to sign bit flip)
    assert not np.all(result == 0)


def test_null_integers() -> None:
    """Test null handling in integer series."""
    null_series = pd.Series([1, None, 3, None, 5], dtype="Int64")
    result = detect_type_and_normalize(null_series)
    assert result.shape == (5, 8)
    # Null rows should be all zeros
    assert np.all(result[1] == 0)
    assert np.all(result[3] == 0)
    # Non-null rows should be non-zero
    assert not np.all(result[0] == 0)
    assert not np.all(result[2] == 0)
    assert not np.all(result[4] == 0)


def test_all_nulls() -> None:
    """Test series with all nulls."""
    null_series = pd.Series([None, None, None], dtype="Int64")
    result = detect_type_and_normalize(null_series)
    assert result.shape == (3, 8)
    assert np.all(result == 0)


def test_empty_string() -> None:
    """Test empty strings."""
    str_series = pd.Series(["", "a", ""])
    result = detect_type_and_normalize(str_series)
    assert result.shape == (3, 8)
    # Empty strings should be all zeros
    assert np.all(result[0] == 0)
    assert np.all(result[2] == 0)


def test_short_strings() -> None:
    """Test short strings are padded correctly."""
    str_series = pd.Series(["hi", "bye", "a"])
    result = detect_type_and_normalize(str_series)
    assert result.shape == (3, 8)

    # Verify "hi" is encoded and padded
    assert result[0, 0] == ord("h")
    assert result[0, 1] == ord("i")
    assert np.all(result[0, 2:] == 0)  # Rest should be padding


def test_long_strings() -> None:
    """Test long strings are truncated to 8 bytes."""
    str_series = pd.Series(["verylongstring", "anotherlongone"])
    result = detect_type_and_normalize(str_series)
    assert result.shape == (2, 8)

    # Should only contain first 8 bytes
    expected = b"verylong"
    assert np.array_equal(result[0], np.frombuffer(expected, dtype=np.uint8))


def test_exact_8_char_strings() -> None:
    """Test strings exactly 8 characters."""
    str_series = pd.Series(["12345678", "abcdefgh"])
    result = detect_type_and_normalize(str_series)
    assert result.shape == (2, 8)

    expected1 = b"12345678"
    assert np.array_equal(result[0], np.frombuffer(expected1, dtype=np.uint8))


def test_unicode_strings() -> None:
    """Test unicode strings (multi-byte characters)."""
    str_series = pd.Series(["hello🌎", "café"])
    result = detect_type_and_normalize(str_series)
    assert result.shape == (2, 8)
    # Should handle multi-byte UTF-8 encoding
    assert result.shape == (2, 8)


def test_null_strings() -> None:
    """Test null strings."""
    str_series = pd.Series(["abc", None, "xyz"])
    result = detect_type_and_normalize(str_series)
    assert result.shape == (3, 8)
    assert np.all(result[1] == 0)


def test_timestamps_positive() -> None:
    """Test positive timestamps (after epoch)."""
    datetime_series = pd.Series([pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31"), pd.Timestamp("2026-01-30")])
    result = detect_type_and_normalize(datetime_series)
    assert result.shape == (3, 8)
    assert not np.all(result == 0)


def test_timestamps_before_epoch() -> None:
    """Test timestamps before Unix epoch (1970)."""
    datetime_series = pd.Series(
        [pd.Timestamp("1960-01-01"), pd.Timestamp("1969-12-31"), pd.Timestamp("1970-01-01"), pd.Timestamp("2000-01-01")]
    )
    result = detect_type_and_normalize(datetime_series)
    assert result.shape == (4, 8)

    # Verify ordering is preserved
    as_ints = [int.from_bytes(row.tobytes(), byteorder="big") for row in result]
    assert as_ints == sorted(as_ints), "Timestamps should maintain chronological order"


def test_timestamp_ordering() -> None:
    """Test that timestamps maintain correct ordering."""
    datetime_series = pd.Series(
        [
            pd.Timestamp("2020-01-01"),
            pd.Timestamp("2021-01-01"),
            pd.Timestamp("2022-01-01"),
            pd.Timestamp("2023-01-01"),
            pd.Timestamp("2024-01-01"),
        ]
    )
    result = detect_type_and_normalize(datetime_series)

    as_ints = [int.from_bytes(row.tobytes(), byteorder="big") for row in result]
    assert as_ints == sorted(as_ints), "Timestamps should be in chronological order"


def test_null_timestamps() -> None:
    """Test null timestamps."""
    datetime_series = pd.Series([pd.Timestamp("2024-01-01"), None, pd.Timestamp("2024-12-31")])
    result = detect_type_and_normalize(datetime_series)
    assert result.shape == (3, 8)
    assert np.all(result[1] == 0)


def test_binary_short() -> None:
    """Test short binary data is padded."""
    binary_series = pd.Series([b"\x01\x02", b"\xff"])
    result = detect_type_and_normalize(binary_series)
    assert result.shape == (2, 8)

    # First row should have 0x01, 0x02, then zeros
    assert result[0, 0] == 0x01
    assert result[0, 1] == 0x02  # noqa: PLR2004
    assert np.all(result[0, 2:] == 0)


def test_binary_long() -> None:
    """Test long binary data is truncated."""
    binary_series = pd.Series([b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a"])
    result = detect_type_and_normalize(binary_series)
    assert result.shape == (1, 8)

    # Should only contain first 8 bytes
    expected = np.array([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08], dtype=np.uint8)
    assert np.array_equal(result[0], expected)


def test_binary_exact_8_bytes() -> None:
    """Test binary data exactly 8 bytes."""
    binary_series = pd.Series([b"\x01\x02\x03\x04\x05\x06\x07\x08"])
    result = detect_type_and_normalize(binary_series)
    assert result.shape == (1, 8)

    expected = np.array([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08], dtype=np.uint8)
    assert np.array_equal(result[0], expected)


# ============================================================================
# Tests for interleave_with_lookup
# ============================================================================


def test_interleave_zeros() -> None:
    """Interleaving zeros should produce zeros."""
    bytes1 = np.zeros((3, 8), dtype=np.uint8)
    bytes2 = np.zeros((3, 8), dtype=np.uint8)
    result = interleave_with_lookup(bytes1, bytes2)

    assert result.shape == (3, 16)
    assert np.all(result == 0)


def test_interleave_ones() -> None:
    """Interleaving all ones should produce all ones."""
    bytes1 = np.full((2, 8), 0xFF, dtype=np.uint8)
    bytes2 = np.full((2, 8), 0xFF, dtype=np.uint8)
    result = interleave_with_lookup(bytes1, bytes2)

    assert result.shape == (2, 16)
    assert np.all(result == 0xFF)  # noqa: PLR2004


def test_interleave_alternating_patterns() -> None:
    """Test alternating bit patterns."""
    bytes1 = np.full((1, 8), 0xAA, dtype=np.uint8)  # 10101010
    bytes2 = np.full((1, 8), 0x55, dtype=np.uint8)  # 01010101
    result = interleave_with_lookup(bytes1, bytes2)

    assert result.shape == (1, 16)
    # Interleaved pattern should be different from inputs
    assert not np.all(result == 0xAA)  # noqa: PLR2004
    assert not np.all(result == 0x55)  # noqa: PLR2004


def test_interleave_single_bit() -> None:
    """Test interleaving single bits in different positions."""
    # Test bit in position 0 (LSB)
    bytes1 = np.zeros((1, 8), dtype=np.uint8)
    bytes2 = np.zeros((1, 8), dtype=np.uint8)
    bytes1[0, 7] = 0x01
    bytes2[0, 7] = 0x01

    result = interleave_with_lookup(bytes1, bytes2)
    assert result[0, 15] == 0x03  # Both bits set in interleaved result  # noqa: PLR2004


def test_interleave_deterministic() -> None:
    """Same inputs should always produce same output."""
    bytes1 = np.random.randint(0, 256, (5, 8), dtype=np.uint8)  # noqa: NPY002
    bytes2 = np.random.randint(0, 256, (5, 8), dtype=np.uint8)  # noqa: NPY002

    result1 = interleave_with_lookup(bytes1, bytes2)
    result2 = interleave_with_lookup(bytes1, bytes2)

    assert np.array_equal(result1, result2)


def test_interleave_different_inputs_different_outputs() -> None:
    """Different inputs should produce different outputs."""
    bytes1_a = np.array([[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]], dtype=np.uint8)
    bytes2_a = np.array([[0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10]], dtype=np.uint8)

    bytes1_b = np.array([[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]], dtype=np.uint8)
    bytes2_b = np.array([[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02]], dtype=np.uint8)

    result_a = interleave_with_lookup(bytes1_a, bytes2_a)
    result_b = interleave_with_lookup(bytes1_b, bytes2_b)

    assert not np.array_equal(result_a, result_b)


def test_interleave_batch() -> None:
    """Test interleaving works correctly for batches."""
    batch_size = 100
    bytes1 = np.random.randint(0, 256, (batch_size, 8), dtype=np.uint8)  # noqa: NPY002
    bytes2 = np.random.randint(0, 256, (batch_size, 8), dtype=np.uint8)  # noqa: NPY002

    result = interleave_with_lookup(bytes1, bytes2)

    assert result.shape == (batch_size, 16)


# ============================================================================
# Tests for INTERLEAVE_TABLE
# ============================================================================


def test_table_dimensions() -> None:
    """Verify table has correct dimensions."""
    assert INTERLEAVE_TABLE.shape == (256, 256)


def test_table_corners() -> None:
    """Test corner values of lookup table."""
    assert INTERLEAVE_TABLE[0, 0] == 0
    assert INTERLEAVE_TABLE[255, 255] == 0xFFFF  # noqa: PLR2004
    assert INTERLEAVE_TABLE[255, 0] == 0xAAAA  # 10101010 10101010  # noqa: PLR2004
    assert INTERLEAVE_TABLE[0, 255] == 0x5555  # 01010101 01010101  # noqa: PLR2004


def test_table_symmetry() -> None:
    """Test that table values follow expected bit patterns."""
    # Interleaving 0x0F (00001111) with 0x00 should produce a specific pattern
    result = INTERLEAVE_TABLE[0x0F, 0x00]
    assert result != 0

    # Interleaving 0x00 with 0x0F should produce a different pattern
    result2 = INTERLEAVE_TABLE[0x00, 0x0F]
    assert result2 != 0
    assert result != result2


# ============================================================================
# Integration Tests
# ============================================================================


"""Integration tests for the complete Z-order function."""


def test_int64() -> None:
    """Test that negative and positive numbers maintain correct Z-order."""
    values_col1 = [-(2**63), -1, 0, 1, (2**63) - 1]
    values_col2 = [-(2**63), -1, 0, 1, (2**63) - 1]

    # Create Series for each value paired with 0
    col1_series = pd.Series(values_col1, dtype=np.int64)
    col2_series = pd.Series(values_col2, dtype=np.int64)

    # Compute Z-order keys for all values at once
    zorder_keys = zorder2Tuple(col1_series, col2_series)

    assert zorder_keys[0] == bytearray(
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
    )
    assert zorder_keys[1] == bytearray(
        [0x3F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
    )
    assert zorder_keys[2] == bytearray(
        [0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
    )
    assert zorder_keys[3] == bytearray(
        [0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03]
    )
    assert zorder_keys[4] == bytearray(
        [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
    )


def test_negative_positive_ordering() -> None:
    """Test that negative and positive numbers maintain correct Z-order."""
    values = [-100, -50, -10, -1, 0, 1, 10, 50, 100]

    # Create Series for each value paired with 0
    col1_series = pd.Series(values, dtype=np.int64)
    col2_series = pd.Series([0] * len(values), dtype=np.int64)

    # Compute Z-order keys for all values at once
    zorder_keys = zorder2Tuple(col1_series, col2_series)

    # Create pairs of (value, zorder_key)
    pairs = list(zip(values, zorder_keys, strict=False))

    # Sort by Z-order key
    sorted_pairs = sorted(pairs, key=lambda x: x[1])
    sorted_values = [v for v, _ in sorted_pairs]

    assert sorted_values == values, "Z-order should preserve numerical ordering"


def test_2d_spatial_locality() -> None:
    """Test that Z-order maintains spatial locality for 2D points."""
    # Points close in 2D space should have similar Z-order keys
    points = [
        (100, 100),  # Point A
        (101, 101),  # Point B - close to A
        (1000, 1000),  # Point C - far from A
    ]

    # Create Series for all points at once
    col1 = pd.Series([p[0] for p in points], dtype=np.int64)
    col2 = pd.Series([p[1] for p in points], dtype=np.int64)
    keys = zorder2Tuple(col1, col2)

    # Extract individual keys
    key_a, key_b, key_c = keys[0], keys[1], keys[2]

    # Distance in Z-order space
    dist_ab = abs(int.from_bytes(key_a, "big") - int.from_bytes(key_b, "big"))
    dist_ac = abs(int.from_bytes(key_a, "big") - int.from_bytes(key_c, "big"))

    # Points closer in space should be closer in Z-order
    assert dist_ab < dist_ac


def test_deterministic_across_calls() -> None:
    """Test Z-order is deterministic."""
    # Create Series for the test values
    col1 = pd.Series([12345], dtype=np.int64)
    col2 = pd.Series(["test"])

    result1 = zorder2Tuple(col1, col2)
    result2 = zorder2Tuple(col1, col2)

    # Compare the bytes objects
    assert result1[0] == result2[0]
