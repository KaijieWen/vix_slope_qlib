"""
VixProvider – Parquet data provider for Qlib 0.9.6
==================================================
• file names lower-case  <symbol>.parquet
• instruments returned upper-case (“SPY”, “VIXY”, …)
• DataFrame index order = (instrument, datetime)
"""

from __future__ import annotations
import os, pandas as pd
from functools import lru_cache
from typing import Iterable, List
import numpy as np


class VixProvider:
    # ────────────────────────── boilerplate ──────────────────────────
    def __init__(self, provider_uri: str):
        self.root = os.path.abspath(provider_uri)

    def _file(self, symbol: str, freq: str) -> str:
        return os.path.join(self.root, freq, f"{symbol.lower()}.parquet")

    # ─────────────────── calendar & instruments ──────────────────────
    @lru_cache(None)
    def calendar(self, freq: str = "daily"):
        fp = os.path.join(self.root, freq, "calendar.txt")
        return [pd.Timestamp(x.strip()) for x in open(fp)]

    @lru_cache(None)
    def instruments(self, freq: str = "daily") -> list[str]:
        p = os.path.join(self.root, freq)
        return [f[:-8].upper() for f in os.listdir(p) if f.endswith(".parquet")]

    #  alias so  D.instruments()  works without args
    def instrument(self, *_a, **_kw) -> list[str]:
        return self.instruments()

    # ───────────────────────── core loader ───────────────────────────
    def load(
        self,
        fields: List[str],
        instruments: Iterable[str],
        freq: str = "daily",
        start_time: str | None = None,
        end_time:   str | None = None,
    ) -> pd.DataFrame:
        """
        Return primitive columns only; Qlib’s expression engine sits on top.
        """
        fields = [f.lstrip("$") for f in fields]        # "$close" → "close"

        parts = []
        for sym in instruments:
            fp = self._file(sym, freq)
            if not os.path.exists(fp):
                continue
            df = pd.read_parquet(fp)[fields]
            if start_time:
                df = df.loc[start_time:end_time]
            df = (
                df.reset_index()                       # index → column
                  .rename(columns={"index": "datetime"})
                  .assign(instrument=sym.upper())
            )
            parts.append(df)

        if not parts:
            raise ValueError("No data loaded for given parameters")

        return (
            pd.concat(parts)
              .set_index(["instrument", "datetime"])
              .sort_index()
        )

    # ───────────────────── minimal features helper ───────────────────
    def features(
        self,
        instruments: Iterable[str],
        fields: list[str],
        start_time: str | None = None,
        end_time: str | None = None,
        freq: str = "daily",
    ) -> pd.DataFrame:
        """
        Handles:
          • "$close"
          • RV5  → Std(Log($spy_close).Diff(1),5)*15.874507866387544
          • TARGET_5D / TARGET_10D
        All other expressions raise NotImplemented (tests skip them for now).
        """
        # --- base close price matrix ---------------------------------
        base = (
            self.load(["close"], instruments, freq, start_time, end_time)
              .unstack("instrument")["close"]          # datetime × symbol
        )

        cols = []
        for f in fields:
            tag = f.replace(" ", "")                   # strip blanks for match

            # 1) raw close ---------------------------------------------------
            if tag == "$close":
                col = (
                    base.stack()                       # back to MultiIndex
                        .to_frame(name=f)
                )

            # 2) RV5 ---------------------------------------------------------
            elif tag.lower() == "std(log($spy_close).diff(1),5)*15.874507866387544".lower():
                spy = base["SPY"]
                rv  = np.log(spy).diff().rolling(5).std() * 15.874507866387544
                col = (
                    rv.to_frame(name=f)
                      .assign(instrument="SPY")
                      .set_index("instrument", append=True)
                      .swaplevel()                     # (instrument, datetime)
                )

            # 3) target labels ----------------------------------------------
            elif tag.upper() in {"TARGET_5D", "TARGET_10D"}:
                horizon = 5 if "5D" in tag.upper() else 10
                spy     = base["SPY"]
                tgt     = np.sign(spy.shift(-horizon) / spy - 1).replace(0, np.nan)
                col = (
                    tgt.to_frame(name=f)
                       .assign(instrument="SPY")
                       .set_index("instrument", append=True)
                       .swaplevel()
                )

            else:
                raise NotImplementedError(f"Unsupported expression: {f}")

            cols.append(col)

        return pd.concat(cols, axis=1).sort_index()
