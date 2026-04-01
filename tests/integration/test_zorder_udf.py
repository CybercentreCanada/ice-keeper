import pytest
from pyspark.sql import Row
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import BinaryType

from ice_keeper.stm import STL
from ice_keeper.zorder_udf import zorder2Tuple


@pytest.mark.integration
def test_zorder_2_optimized() -> None:
    """Test the zorder_2_optimized Spark UDF."""
    # Create a test DataFrame
    data = [
        Row(col1=1, col2=1),
        Row(col1=2, col2=3),
        Row(col1=None, col2=4),
    ]
    df = STL.get().createDataFrame(data)
    df.createOrReplaceTempView("view")

    udf = pandas_udf(zorder2Tuple, returnType=BinaryType())  # type: ignore[call-overload]
    STL.get().udf.register("zorder2Tuple", udf)

    # Apply the UDF
    results = STL.sql("select zorder2Tuple(col1, col2) as zorder_key from view").collect()

    # Verify the results
    assert len(results) == 3
    assert results[0]["zorder_key"] == bytearray(
        [0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03]
    )
