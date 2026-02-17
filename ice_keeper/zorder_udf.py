import struct

import numpy as np
import pandas as pd
from numpy.typing import NDArray

# Build lookup table once at module level
INTERLEAVE_TABLE = np.zeros((256, 256), dtype=np.uint16)


def _build_interleave_table() -> None:
    """Build lookup table for fast byte interleaving."""
    for b1 in range(256):
        for b2 in range(256):
            result = 0
            for bit in range(8):
                bit1 = (b1 >> bit) & 1
                bit2 = (b2 >> bit) & 1
                result |= bit1 << (2 * bit + 1)
                result |= bit2 << (2 * bit)
            INTERLEAVE_TABLE[b1, b2] = result


_build_interleave_table()


def detect_type_and_normalize(series: pd.Series) -> NDArray[np.uint8]:  # noqa: C901
    """Convert Series to (n, 8) byte array based on dtype."""
    dtype = series.dtype
    n = len(series)
    # Nulls are all zeros by default - result is initialized with np.zeros()
    result = np.zeros((n, 8), dtype=np.uint8)
    null_mask = series.isna()

    if isinstance(dtype, pd.Int32Dtype) or str(dtype) == "Int32":
        # 32-bit signed int - flip sign bit to preserve order
        valid_vals = series[~null_mask].astype(np.int32).values  # noqa: PD011
        valid_indices = np.where(~null_mask)[0]

        # Use NumPy's view to reinterpret as unsigned
        unsigned_vals = valid_vals.view(np.uint32) ^ np.uint32(0x80000000)

        for idx, i in enumerate(valid_indices):
            packed = struct.pack(">I", unsigned_vals[idx])
            # This writes the 4 values to positions 0,1,2,3 of row i
            result[i, :4] = np.frombuffer(packed, dtype=np.uint8)

    elif isinstance(dtype, pd.Int64Dtype) or str(dtype) == "Int64" or dtype == np.int64 or dtype == "int64":  # noqa: PLR1714
        # 64-bit integers
        valid_vals = series[~null_mask].astype(np.int64).values  # noqa: PD011
        valid_indices = np.where(~null_mask)[0]
        # Use NumPy's view to reinterpret as unsigned
        unsigned_vals = valid_vals.view(np.uint64) ^ np.uint64(0x8000000000000000)

        for idx, i in enumerate(valid_indices):
            # View as unsigned, then XOR
            packed = struct.pack(">Q", unsigned_vals[idx])
            # ALL 8 bytes of row i (columns 0, 1, 2, 3, 4, 5, 6, 7)
            result[i, :] = np.frombuffer(packed, dtype=np.uint8)

    elif dtype == "object":
        for idx in range(n):
            if not null_mask.iloc[idx]:
                val = series.iloc[idx]
                if isinstance(val, str):
                    encoded = val.encode("utf-8")[:8].ljust(8, b"\x00")
                elif isinstance(val, (bytes, bytearray)):
                    encoded = bytes(val)[:8].ljust(8, b"\x00")
                else:
                    encoded = str(val).encode("utf-8")[:8].ljust(8, b"\x00")
                result[idx, :] = np.frombuffer(encoded, dtype=np.uint8)

    elif np.issubdtype(dtype, np.datetime64):
        # Timestamps - convert to milliseconds
        valid_indices = np.where(~null_mask)[0]
        for i in valid_indices:
            # Convert to milliseconds since epoch
            ts_ns = pd.Timestamp(series.iloc[i]).value
            ts_ms = ts_ns // 1000000
            signed_value = np.array(ts_ms, dtype=np.int64)
            # View as unsigned, then XOR
            unsigned = signed_value.view(np.uint64) ^ np.uint64(0x8000000000000000)
            packed = struct.pack(">Q", int(unsigned))
            # ALL 8 bytes of row i (columns 0, 1, 2, 3, 4, 5, 6, 7)
            result[i, :] = np.frombuffer(packed, dtype=np.uint8)

    return result


def interleave_with_lookup(bytes1: NDArray[np.uint8], bytes2: NDArray[np.uint8]) -> NDArray[np.uint8]:
    """Fast interleaving using pre-computed lookup table.

    bytes1, bytes2: shape (n, 8)
    Returns: shape (n, 16)
    """
    n = bytes1.shape[0]
    result = np.zeros((n, 16), dtype=np.uint8)

    for i in range(8):
        # Vectorized lookup for entire column at once
        interleaved = INTERLEAVE_TABLE[bytes1[:, i], bytes2[:, i]]
        # b1 = bytes1[:, i]
        # b2 = bytes2[:, i]
        # print(f"{b1.tobytes().hex()}, {b2.tobytes().hex()} => {interleaved.tobytes().hex()}")

        # Split 16-bit result into high and low bytes
        result[:, i * 2] = (interleaved >> 8).astype(np.uint8)
        result[:, i * 2 + 1] = (interleaved & 0xFF).astype(np.uint8)

    return result


def zorder2Tuple(col1: pd.Series, col2: pd.Series) -> pd.Series:  # noqa: N802
    """Optimized Z-order UDF using lookup table for bit interleaving.

    Automatically handles different data types based on Series dtype.
    """
    # Convert to bytes
    bytes1 = detect_type_and_normalize(col1)
    bytes2 = detect_type_and_normalize(col2)

    # Interleave using lookup table
    interleaved = interleave_with_lookup(bytes1, bytes2)

    # Convert to Series of bytes objects
    return pd.Series([bytes(row) for row in interleaved])
