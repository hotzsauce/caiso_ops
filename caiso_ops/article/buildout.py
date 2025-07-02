from __future__ import annotations
from rich import print

import caiso_ops as ops
import numpy as np
import pandas as pd
import plotly.express as px

from caiso_ops.article.io import FlourishWriter

pd.options.plotting.backend = "plotly"



def fetch_battery_assets() -> pd.DataFrame:
    """
    Primary method for formatting the `caiso_generator_capabilities` table
    """
    filters = (
        "(energy_source == 'LESR') "
        # "& (baa_id == 'CISO') "
        # "& (classification == 'Participating Unit') "
    )
    df = (
        ops.fetch_generator_capabilities() # grabs the data from iceberg
        .query(filters)
        .dropna(subset=["cod"])
    )

    dates = ["cod", "valid_from", "valid_to"]
    for date in dates:
        df[date] = df[date].dt.tz_localize(None).dt.normalize()

    return df

def fleet_wide_buildout(
    freq: str = "1ME",
    volume: str = "net_dependable_capacity",
    *args,
    **kwargs,
):
    """
    Create a time series of the CAISO-wide total rated power

    # Note
    net_dependable_capacity for `caiso_generator_capability` or
    mw_capacity for `caiso_index_capacity`
    """

    first_date = "2021-01-01"
    final_date = "2025-06-30"

    if volume == "net_dependable_capacity":

        def fleet_capacity_on_date(
            date: datetime,
            batteries: pd.DataFrame
        ) -> float:
            # Filter rows based on date conditions
            idx = (
                (date >= batteries["cod"])
                & (date >= batteries["valid_from"])
                & (pd.isna(batteries["valid_to"]) | (date < batteries["valid_to"]))
            )

            filtered = batteries.loc[idx, :]
            deduplicated = (
                filtered
                .sort_values("valid_from", na_position="first")
                .drop_duplicates(
                    subset="resource_id",
                    keep="first",
                )
            )
            return deduplicated["net_dependable_capacity"].sum()

        batteries = fetch_battery_assets()
        series = (
            pd.date_range(first_date, final_date, freq=freq)
            .to_frame()
            .squeeze()
            .apply(
                lambda dt: fleet_capacity_on_date(dt, batteries)
            )
        )
    elif volume == "mw_capacity":
        series = (
            ops.fetch_index_capacity(first_date=first_date)
            .set_index("date")
            .resample(freq)
            .last()
            ["mw_capacity"]
        )
    else:
        raise ValueError(f"Unrecognized `volume` value: '{volume}'")

    old_label = "Existing Capacity"
    new_label = "New Capacity"

    # create one column with time-(t-1) existing capacity and another column
    # with time-t new capacity
    df = series.rename(old_label).to_frame()
    df[new_label] = -1 * (df[old_label].diff(-1).shift(1))
    df[old_label] = df[old_label].shift(1, fill_value=df[old_label].iloc[0])
    return df

def new_operational_assets(date: str = "2025-01-01", *args, **kwargs):
    """
    Create a DataFrame of the asses whose COD is >= `date`
    """
    df = fetch_battery_assets()
    df = (
        df.loc[df.cod >= pd.to_datetime(date), :]
        .sort_values("cod")
    )
    return df



if __name__ == "__main__":
    freq = "1ME"
    df = fleet_wide_buildout(freq)
    FlourishWriter.write(df, f"buildout/fleet_wide_ts_{freq}.csv")

    freq = "1QE"
    df = fleet_wide_buildout(freq)
    FlourishWriter.write(df, f"buildout/fleet_wide_ts_{freq}.csv")

    # df = new_operational_assets("2025-01-01")
    # FlourishWriter.write(df, "buildout/new_assets_in_2025.csv")
