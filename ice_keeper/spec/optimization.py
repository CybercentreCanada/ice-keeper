import logging
import re
from enum import Enum

logger = logging.getLogger("ice-keeper")


class Strategy(Enum):
    BINPACK = "binpack"
    SORT = "sort"
    NONE = "none"

    @classmethod
    def from_string(cls, value: str) -> "Strategy":
        # value is explicitly set to "binpack"
        if value == Strategy.BINPACK.value:
            return Strategy.BINPACK
        # else, if the string contains something then is a sort or zorder sort.
        if value:
            return Strategy.SORT
        # if the value is blank then nothing at all was specified.
        return Strategy.NONE


class OptimizationSpec:
    def __init__(self, strategy: Strategy, sorted_column_names: list[str], *, is_zordered: bool) -> None:
        self.strategy = strategy
        self.sorted_column_names = sorted_column_names
        self.is_zordered = is_zordered

    def is_sorted(self) -> bool:
        return self.strategy == Strategy.SORT

    def is_binpack(self) -> bool:
        return self.strategy == Strategy.BINPACK

    @classmethod
    def _strip_qualifiers(cls, s: str) -> str:
        s = re.sub(r" (NULLS FIRST|NULLS LAST)$", "", s, flags=re.IGNORECASE)
        s = re.sub(r" (ASC|DESC)$", "", s, flags=re.IGNORECASE)
        return s.strip()

    @classmethod
    def from_string(cls, optimization_strategy: str) -> "OptimizationSpec":
        strategy = Strategy.from_string(optimization_strategy)

        is_zordered = False
        sorted_column_names: list[str] = []
        if strategy == Strategy.SORT:
            # Regular expression to match 'zorder' followed by arguments in parentheses
            match = re.search(r"zorder\((.*?)\)", optimization_strategy)
            # Extract and split the arguments if 'zorder' is present
            if match:
                is_zordered = True
                zorder_args = match.group(1)
                sorted_column_names = [cls._strip_qualifiers(arg) for arg in zorder_args.split(",")]
            else:
                sorted_column_names = [cls._strip_qualifiers(arg) for arg in optimization_strategy.split(",")]
        return OptimizationSpec(strategy, sorted_column_names, is_zordered=is_zordered)
