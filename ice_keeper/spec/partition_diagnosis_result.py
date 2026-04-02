from typing import Any

from pydantic import BaseModel, Field
from pyspark.sql.types import Row


class PartitionDiagnosisResult(BaseModel):
    partition_age: int
    target_file_size: int
    partition_filters: list[dict[str, Any]] = Field(default_factory=list)

    @classmethod
    def from_row(cls, row: Row) -> "PartitionDiagnosisResult":
        return cls(**row.asDict(recursive=True))
