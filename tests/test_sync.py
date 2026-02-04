import pytest
import pandas as pd
from src.sync_engine import ClockifySyncEngine
from unittest.mock import MagicMock

def test_to_iso8601():
    # We need a mock engine to test the method
    engine = MagicMock(spec=ClockifySyncEngine)
    engine.to_iso8601 = ClockifySyncEngine.to_iso8601.__get__(engine)
    
    assert engine.to_iso8601(8) == "PT8H0M"
    assert engine.to_iso8601(7.5) == "PT7H30M"
    assert engine.to_iso8601(0.25) == "PT0H15M"

def test_clean_number():
    engine = MagicMock(spec=ClockifySyncEngine)
    engine.clean_number = ClockifySyncEngine.clean_number.__get__(engine)
    
    assert engine.clean_number("40") == 40.0
    assert engine.clean_number("37,5") == 37.5
    assert engine.clean_number(None) == 0.0
    assert engine.clean_number("abc") == 0.0

def test_column_normalization():
    df = pd.DataFrame(columns=[" NTID email ", "Weekly Hours"])
    df.columns = df.columns.str.strip()
    assert "NTID email" in df.columns
    assert "Weekly Hours" in df.columns

def test_display_name():
    engine = MagicMock(spec=ClockifySyncEngine)
    engine.get_display_name = ClockifySyncEngine.get_display_name.__get__(engine)
    
    assert engine.get_display_name({"name": " John Doe ", "email": "john@example.com"}) == "John Doe"
    assert engine.get_display_name({"email": "john@example.com"}) == "john@example.com"
    assert engine.get_display_name({}) == ""
