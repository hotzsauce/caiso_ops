from __future__ import annotations 

import caiso_ops as ops
import numpy as np
import pandas as pd

from caiso_ops.article.io import FlourishWriter

pd.options.plotting.backend = "plotly"



def index_lookback(
    start: str = "2024-01-01",
    freq: str = "1ME",
    agg_size: int = 0,
):
    """
    Create a time-series of the CAISO index, aggregated to as, rt- and da-energy
    """
    ix = (
        ops.fetch_index()
        .loc[pd.to_datetime(start):, :]
        .resample(freq)
        .sum()
    )

    # compute the aggregate number for every `agg_size` `freq` periods
    if agg_size > 0:
        n, m = ix.shape
        k = ix.shape[0] // agg_size
        columns = [f"agg_{i}" for i in range(k)]

        revenues = ix.sum(axis="columns")
        averages = np.zeros(n, dtype=float)
        for row in (range(i, min(i+agg_size, n)) for i in range(0, n, agg_size)):
            averages[row] = revenues.iloc[row].mean()
        ix["Average"] = averages

    return ix

def revenue_waterfall(
    curr_start: str = "2025-01-01",
    curr_end: str = "",
    ref_start: str = "2024-01-01",
    ref_end: str = "2024-06-30",
):
    """
    Create the usual revenue waterfall dataset
    """
    if curr_end:
        curr_end = pd.to_datetime(curr_end)
    else:
        curr_end = pd.Timestamp.now()
    curr_start = pd.to_datetime(curr_start)
    ref_start = pd.to_datetime(ref_start)
    ref_end = pd.to_datetime(ref_end)

    strftime = lambda dt: dt.strftime("%b %d, %Y")
    ref_label = f"{strftime(ref_start)} to {strftime(ref_end)}"
    curr_label = f"{strftime(curr_start)} to {strftime(curr_end)}"

    # ruc energy is actively excluded from the CAISO index, as per Samira
    ix = (
        ops.fetch_index(agg_rt_energy=False, agg_as=False)
        .drop(columns=["ruc_energy"])
    )

    ref_ix = ix.loc[ref_start:ref_end, :].sum().rename(ref_label)
    curr_ix = ix.loc[curr_start:curr_end, :].sum().rename(curr_label)

    # calculate the changes between the two periods
    waterfall = (curr_ix - ref_ix).rename("Revenues")
    index_labels = waterfall.index.to_list()

    waterfall[ref_label] = ref_ix.sum()
    waterfall[curr_label] = np.nan

    out = waterfall.loc[[ref_label] + index_labels + [curr_label]]
    out.index = ops.io.to_display(out.index)

    return out

if __name__ == "__main__":
    df = index_lookback(freq="1ME", agg_size=6)
    # FlourishWriter.write(df, "index/monthly_lookback.csv")

    df = revenue_waterfall()
    FlourishWriter.write(df, "index/waterfall.csv")
