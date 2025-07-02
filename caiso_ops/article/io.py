from __future__ import annotations

import pandas as pd
import pathlib



FLOURISH_TARGET = pathlib.Path.cwd() / "flourish"


class _FlourishWriter(object):

    _column_beautifier = {
        "da_energy": "Day-Ahead Energy",
        "rt_energy": "Real-Time Energy",
        "as": "Ancillary Services",
    }

    def __init__(self, target: str | pathlib.Path):
        self.target = pathlib.Path(target)

    def beautify(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform the following steps before writing to CSV:
            1. Map common column names to flourish-friendly labels
            2. Reorder columns of CAISO index data to [DA, RT, AS]
        """
        # step 1
        try:
            out = df.rename(columns=self._column_beautifier)
        except TypeError:
            # dealing with a pd.Series
            return df

        # step 2
        all_rev_columns = [
            "Day-Ahead Energy",
            "Real-Time Energy",
            "Ancillary Services",
        ]
        rev_columns = [c for c in all_rev_columns if c in out.columns]
        if rev_columns:
            other_columns = list(out.columns.difference(rev_columns))
            out = out.loc[:, rev_columns + other_columns]

        return out

    def write(
        self,
        data: pd.DataFrame,
        name: str | pathlib.Path,
    ):
        target = self.target / str(name)
        target_dir = target.parent

        target_dir.mkdir(parents=True, exist_ok=True)
        formatted = self.beautify(data)
        formatted.to_csv(target)

FlourishWriter = _FlourishWriter(FLOURISH_TARGET)
