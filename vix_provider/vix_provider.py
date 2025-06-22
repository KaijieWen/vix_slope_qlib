"""
VixProvider – minimal custom Qlib provider for our Parquet store.
Works on Qlib 0.9.6 without importing BaseProvider.
"""
import os
from typing import List, Iterable
import pandas as pd
from functools import lru_cache


class VixProvider:                       # ← no BaseProvider
    def __init__(self, provider_uri: str):
        self.root = os.path.abspath(provider_uri)

    # ---------- helpers ----------
    def _file(self, symbol: str, freq: str) -> str:
        return os.path.join(self.root, freq, f"{symbol}.parquet")

    # ---------- APIs expected by Qlib ----------
    @lru_cache(maxsize=None)
    def calendar(self, freq: str):
        cal_path = os.path.join(self.root, freq, "calendar.txt")
        return [pd.Timestamp(x.strip()) for x in open(cal_path)]

    @lru_cache(maxsize=None)
    def instruments(self, freq: str):
        p = os.path.join(self.root, freq)
        return [f[:-8] for f in os.listdir(p) if f.endswith(".parquet")]

    def load(
        self,
        fields: List[str],
        instruments: Iterable[str],
        freq: str,
        start_time: str,
        end_time: str,
    ) -> pd.DataFrame:
        dfs = []
        for sym in instruments:
            fp = self._file(sym, freq)
            if not os.path.exists(fp):
                continue
            df = pd.read_parquet(fp)[fields]
            df = df.loc[start_time:end_time]
            df["instrument"] = sym
            dfs.append(df)
        if not dfs:
            raise ValueError("No data loaded")
        out = pd.concat(dfs)
        return out.set_index(["datetime", "instrument"]).sort_index()
