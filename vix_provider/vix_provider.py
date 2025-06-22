"""
VixProvider – minimal custom Qlib provider for our Parquet store.
Works with Qlib 0.9.6 helper APIs (`D.features`, `D.instruments`, …).
"""

from __future__ import annotations
import os
from typing import List, Iterable
import pandas as pd
from functools import lru_cache


class VixProvider:
    # ---------- boilerplate ---------- #
    def __init__(self, provider_uri: str):
        self.root = os.path.abspath(provider_uri)

    def _file(self, symbol: str, freq: str) -> str:
        # always look for lower-case filenames
        return os.path.join(self.root, freq, f"{symbol.lower()}.parquet")


    # ---------- calendar & symbols ---------- #
    @lru_cache(maxsize=None)
    def calendar(self, freq: str = "daily"):
        path = os.path.join(self.root, freq, "calendar.txt")
        return [pd.Timestamp(x.strip()) for x in open(path)]

    @lru_cache(maxsize=None)
    def instruments(self, freq: str = "daily") -> list[str]:
        p = os.path.join(self.root, freq)
        # return upper-case tickers so workflow uses “SPY”
        return [f[:-8].upper() for f in os.listdir(p) if f.endswith(".parquet")]

    # lets D.instruments() work without kwargs
    def instrument(self, *_a, **_kw):
        return self.instruments()

    # ---------- core loader ---------- #
    def load(
        self,
        fields: List[str],
        instruments: Iterable[str],
        freq: str = "daily",
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> pd.DataFrame:
        # strip leading '$' that Qlib adds
        fields = [f.lstrip("$") for f in fields]

        parts = []
        for sym in instruments:
            fp = self._file(sym, freq)
            if not os.path.exists(fp):
                continue
            df = pd.read_parquet(fp)[fields]
            if start_time:
                df = df.loc[start_time:end_time]
            df = (
                df.reset_index()                          # index → column
                  .rename(columns={"index": "datetime"})
                  .assign(instrument=sym)
            )
            parts.append(df)

        if not parts:
            raise ValueError("No data loaded for given params")

        return (
            pd.concat(parts)
              .set_index(["datetime", "instrument"])
              .sort_index()
        )

    # ---------- feature helper (called by D.features) ---------- #
    def features(
        self,
        instruments: Iterable[str],
        fields: List[str],
        start_time: str | None = None,
        end_time: str | None = None,
        freq: str = "daily",
    ) -> pd.DataFrame:
        return self.load(fields, instruments, freq, start_time, end_time)

