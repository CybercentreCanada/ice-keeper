import pytest

from ice_keeper.task.action.optimization.datafile_summary import DataFilesSummary


class TestParseInterval:
    def test_parse_day(self) -> None:
        unit, amount = DataFilesSummary._parse_interval("7d")
        assert unit == "day"
        assert amount == 7

    def test_parse_hour(self) -> None:
        unit, amount = DataFilesSummary._parse_interval("24h")
        assert unit == "hour"
        assert amount == 24

    def test_parse_month(self) -> None:
        unit, amount = DataFilesSummary._parse_interval("3M")
        assert unit == "month"
        assert amount == 3

    def test_parse_year(self) -> None:
        unit, amount = DataFilesSummary._parse_interval("2Y")
        assert unit == "year"
        assert amount == 2

    def test_parse_zero(self) -> None:
        unit, amount = DataFilesSummary._parse_interval("0d")
        assert unit == "day"
        assert amount == 0

    def test_parse_negative_day(self) -> None:
        unit, amount = DataFilesSummary._parse_interval("-1d")
        assert unit == "day"
        assert amount == -1

    def test_parse_negative_month(self) -> None:
        unit, amount = DataFilesSummary._parse_interval("-3M")
        assert unit == "month"
        assert amount == -3

    def test_parse_negative_year(self) -> None:
        unit, amount = DataFilesSummary._parse_interval("-1Y")
        assert unit == "year"
        assert amount == -1

    def test_parse_case_insensitive_month(self) -> None:
        unit, amount = DataFilesSummary._parse_interval("3m")
        assert unit == "month"
        assert amount == 3

    def test_parse_case_insensitive_year(self) -> None:
        unit, amount = DataFilesSummary._parse_interval("2y")
        assert unit == "year"
        assert amount == 2

    def test_invalid_suffix(self) -> None:
        with pytest.raises(ValueError, match="Unsupported interval suffix"):
            DataFilesSummary._parse_interval("7x")

    def test_invalid_amount(self) -> None:
        with pytest.raises(ValueError, match="Invalid interval amount"):
            DataFilesSummary._parse_interval("abcd")

    def test_empty_amount(self) -> None:
        with pytest.raises(ValueError, match="Invalid interval amount"):
            DataFilesSummary._parse_interval("d")

    def test_double_negative(self) -> None:
        with pytest.raises(ValueError, match="Invalid interval amount"):
            DataFilesSummary._parse_interval("--1d")
