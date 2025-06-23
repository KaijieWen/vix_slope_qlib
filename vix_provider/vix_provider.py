"""
VixProvider – Parquet data provider for Qlib 0.9.6
==================================================
* file names stored lower-case     <symbol>.parquet
* instruments exposed upper-case (“SPY”, “VIXY”, “VXZ”, …)
* returns MultiIndexed DataFrame  (instrument, datetime)
"""

from __future__ import annotations
import os, pandas as pd
from functools import lru_cache
from typing import Iterable, List

# ---------------------------------------------------------------------
class VixProvider:
    # ----- boilerplate ------------------------------------------------
    def __init__(self, provider_uri: str):
        self.root = os.path.abspath(provider_uri)

    def _file(self, symbol: str, freq: str) -> str:
        return os.path.join(self.root, freq, f"{symbol.lower()}.parquet")

    # ----- calendar & symbols ----------------------------------------
    @lru_cache(None)
    def calendar(self, freq: str = "daily"):
        fp = os.path.join(self.root, freq, "calendar.txt")
        return [pd.Timestamp(x.strip()) for x in open(fp)]

    @lru_cache(None)
    def instruments(self, freq: str = "daily") -> list[str]:
        p = os.path.join(self.root, freq)
        return [f[:-8].upper() for f in os.listdir(p) if f.endswith(".parquet")]

    # alias so  D.instruments()  works without kwargs
    def instrument(self, *_a, **_kw) -> list[str]:
        return self.instruments()

    # ----- core loader ------------------------------------------------
    def load(
        self,
        fields: List[str],
        instruments: Iterable[str],
        freq: str = "daily",
        start_time: str | None = None,
        end_time:   str | None = None,
    ) -> pd.DataFrame:
        """
        Return raw columns only.
        Expression parsing is handled by Qlib’s calculator layer,
        so this method just delivers the underlying data.
        """
        fields = [f.lstrip("$") for f in fields]          # "$close" → "close"

        frames = []
        for sym in instruments:
            fp = self._file(sym, freq)
            if not os.path.exists(fp):
                continue
            df = pd.read_parquet(fp)[fields]
            if start_time:
                df = df.loc[start_time:end_time]

            df = (
                df.reset_index()                          # index ⇒ column
                  .rename(columns={"index": "datetime"})
                  .assign(instrument=sym.upper())
            )
            frames.append(df)

        if not frames:
            raise ValueError("No data loaded for given parameters")

        return (
            pd.concat(frames)
              .set_index(["instrument", "datetime"])
              .sort_index()
        )

# ---------------------------------------------------------------------
# Nothing else needed – Qlib’s own expression engine will call .load()
# to obtain the primitive columns it needs for any DSL expression.
