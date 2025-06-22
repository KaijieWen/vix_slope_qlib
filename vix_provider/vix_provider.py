"""
VixProvider – custom Parquet provider for Qlib 0.9.6.
"""

from __future__ import annotations
import os, pandas as pd
from functools import lru_cache
from typing import List, Iterable


class VixProvider:
    # ------------------------------------------------------------------
    def __init__(self, provider_uri: str):
        self.root = os.path.abspath(provider_uri)

    # ---------- helpers ------------------------------------------------
    def _file(self, symbol: str, freq: str) -> str:
        """Return full path for <symbol>.parquet (filenames are lower-case)."""
        return os.path.join(self.root, freq, f"{symbol.lower()}.parquet")

    # ---------- calendar & symbol list --------------------------------
    @lru_cache(None)
    def calendar(self, freq: str = "daily"):
        cal = os.path.join(self.root, freq, "calendar.txt")
        return [pd.Timestamp(x.strip()) for x in open(cal)]

    @lru_cache(None)
    def instruments(self, freq: str = "daily") -> list[str]:
        p = os.path.join(self.root, freq)
        return [f[:-8].upper() for f in os.listdir(p) if f.endswith(".parquet")]

    # alias so `D.instruments()` works
    def instrument(self, *_, **__) -> list[str]:
        return self.instruments()

    # ---------- core loader -------------------------------------------
    def load(
        self,
        fields: List[str],
        instruments: Iterable[str],
        freq: str = "daily",
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> pd.DataFrame:
        fields = [f.lstrip("$") for f in fields]          # Qlib passes “$close”
        frames = []

        for sym in instruments:
            fp = self._file(sym, freq)
            if not os.path.exists(fp):
                continue
            df = pd.read_parquet(fp)[fields]
            if start_time:
                df = df.loc[start_time:end_time]
            df = (
                df.reset_index()                         # index → column
                  .rename(columns={"index": "datetime"})
                  .assign(instrument=sym.upper())
            )
            frames.append(df)

        if not frames:
            raise ValueError("No data loaded for given parameters")

        return (
            pd.concat(frames)
              .set_index(["instrument", "datetime"])     # instrument first
              .sort_index()
        )

    # ---------- thin wrapper used by `D.features` ---------------------
    def features(
        self,
        instruments: Iterable[str],
        fields: list[str],
        start_time: str | None = None,
        end_time: str | None = None,
        freq: str = "daily",
    ) -> pd.DataFrame:
        df = self.load(fields, instruments, freq, start_time, end_time)

        # map   close → $close,   open → $open,  ...
        rename_map = {f.lstrip("$"): f for f in fields}
        return df.rename(columns=rename_map)
