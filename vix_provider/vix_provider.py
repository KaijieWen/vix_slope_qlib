"""
VixProvider – minimal custom provider for our Parquet store.
Now 100 % compatible with Qlib 0.9.6 helper APIs.
"""
import os
from typing import List, Iterable
import pandas as pd
from functools import lru_cache


class VixProvider:
    def __init__(self, provider_uri: str):
        self.root = os.path.abspath(provider_uri)

    # ---------- helpers ----------
    def _file(self, symbol: str, freq: str) -> str:
        return os.path.join(self.root, freq, f"{symbol}.parquet")

    # ---------- Qlib-expected APIs ----------
    @lru_cache(maxsize=None)
    def calendar(self, freq: str = "daily"):
        path = os.path.join(self.root, freq, "calendar.txt")
        return [pd.Timestamp(x.strip()) for x in open(path)]

    # Core lookup with a default freq
    @lru_cache(maxsize=None)
    def instruments(self, freq: str = "daily") -> List[str]:
        p = os.path.join(self.root, freq)
        return [f[:-8] for f in os.listdir(p) if f.endswith(".parquet")]

    # Wrapper → lets D.instruments() work out-of-the-box
    def instrument(self, *_, **__) -> List[str]:
        return self.instruments()


    # ---------------------------------------------------------------
    # called by D.calendar() when no freq arg is supplied
    def calendar(self, freq: str = "daily"):
        return self.__class__.calendar.__wrapped__(self, freq)  # reuse cached

    # called by D.features(...)
    def features(self, instruments, fields, start_time=None, end_time=None, freq="daily"):
        return self.load(fields, instruments, freq, start_time, end_time)
    # ---------------------------------------------------------------

    # Data loader
    def load(
        self,
        fields: List[str],
        instruments: Iterable[str],
        freq: str = "daily",
        start_time: str = None,
        end_time: str = None,
    ) -> pd.DataFrame:
        # ➡️  strip the leading “$” that Qlib puts on field names
        fields = [f.lstrip("$") for f in fields]       #  << add this line

        dfs = []
        for sym in instruments:
            fp = self._file(sym, freq)
            if not os.path.exists(fp):
                continue
            df = pd.read_parquet(fp)[fields]           # fields now match columns
            if start_time:
                df = df.loc[start_time:end_time]
            df["instrument"] = sym
            dfs.append(df)
        if not dfs:
            raise ValueError("No data loaded")
        return (
            pd.concat(dfs)
            .set_index(["datetime", "instrument"])
            .sort_index()
        )
