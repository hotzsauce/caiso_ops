from __future__ import annotations 

import caiso_ops as ops
import numpy as np
import pandas as pd
import plotly.express as px

from caiso_ops.article.io import FlourishWriter

pd.options.plotting.backend = "plotly"



def price_extremes(
    first_date: str = "2024-01-01",
    final_date: str = "",
):
    """
    Create a time-series of the highest and lowest prices observed each day
    """

    prices = (
        ops.fetch_energy_prices("da")
        .set_index("timestamp")
        ["lmp"]
        .resample("1d")
        .agg(["min", "max"])
        .rename(columns={
            "min": "Daily Minimum",
            "max": "Daily Maximum"
        })
    )
    return prices

def congestion_prices(
    first_date: str = "2024-01-01",
    final_date: str = "",
):
    """
    Create a time-series of the congestion price component of the LMP
    """
    first_date = pd.to_datetime(first_date)
    if not final_date:
        final_date = pd.Timestamp.now()
    else:
        final_date = pd.to_datetime(final_date)

    congestion = (
        ops.fetch_energy_prices("da")
        .set_index("timestamp")
        .loc[first_date:final_date, "congestion_price"]
    )
    return congestion



if __name__ == "__main__":
    df = price_extremes()
    print(df)
    # FlourishWriter.write(df, "markets/price_extrema.csv")
