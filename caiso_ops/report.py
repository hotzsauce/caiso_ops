"""
Creating aspects of the benchmark report
"""
from __future__ import annotations

from functools import cached_property
import numpy as np
import pandas as pd

import caiso_ops.data as data
from caiso_ops.prices import negative_duration
from caiso_ops.tb_spreads import TopBottomSpread



class ReportData(object):

    @cached_property
    def contracted_volumes(self):
        df = data.fetch_contracted_volumes()
        return df

    @cached_property
    def da_anc(self):
        df = data.fetch_as_prices(market="da")
        # AS_CAISO_EXP is the price hub that factors in gen, load, and
        # intertie nodes
        return df.loc[df.anc_region == "AS_CAISO_EXP", :]

    @cached_property
    def da_energy(self):
        df = data.fetch_energy_prices(market="da")
        return df

    @cached_property
    def index(self):
        df = data.fetch_index()
        return df

    @cached_property
    def fuel_mix(self):
        df = data.fetch_generation("all")
        return df

    @cached_property
    def load(self):
        df = data.fetch_load()
        return df

    @cached_property
    def net_load(self):
        df = self.load.merge(
            self.fuel_mix
            .set_index("timestamp")
            [["solar", "wind"]]
            .sum(axis="columns")
            .rename("solar_wind"),
            left_on="timestamp",
            right_index=True,
        )
        df["net_load"] = df["load"] - df["solar_wind"]
        return df[["timestamp", "net_load"]]

    @cached_property
    def rt_anc(self):
        df = data.fetch_as_prices(market="rt")
        return df

    @cached_property
    def rt_energy(self):
        df = data.fetch_energy_prices(market="rt")
        return df


class DriverTable(object):
    """
    A "Key Drivers" table that aggregates statistics for a prior 'reference'
    period and the 'current' period
    """

    # order is important for _entries
    _entries = {
        "Merchant Revenues": "battery_revenues",
        "TB4 Spreads": "price_spreads",
        "Negatively Priced Hours": "negative_prices",
        "Average Negative Price": "negative_price_magnitude",
        "Average Negative Duration": "negative_price_duration",
        #
        "Solar Gen.": "solar_generation",
        "Solar Peak": "solar_peak",
        "Load": "load_total",
        "Net Load": "load_net",
        #
        "Regulation Prices": "regulation_prices",
    }

    # order is *not* important for _units
    _units = {
        "Merchant Revenues": "$/kW/year",
        "Load": "GW",
        "Net Load": "GW",
        "Negatively Priced Hours": "No. Hours",
        "Average Negative Price": "$/MWh",
        "Average Negative Duration": "No. Hours",
        "Regulation Prices": "$/MW",
        "Solar Gen.": "TWh",
        "Solar Peak": "GW",
        "TB4 Spreads": "$/MWh",
    }

    def __init__(
        self,
        curr_start: str,
        curr_end: str = "",
        ref_start: str = "2024-01-01",
        ref_end: str = "2024-06-30",
        data: Optional[ReportData] = None,
    ):
        self.curr_start = pd.to_datetime(curr_start)
        if curr_end:
            self.curr_end = pd.to_datetime(curr_end)
        else:
            self.curr_end = pd.Timestamp.now()

        self.ref_start = pd.to_datetime(ref_start)
        self.ref_end = pd.to_datetime(ref_end)

        self.data = data if data else ReportData()

    def create(self):
        date_fmt = lambda x: x.strftime("%b %d, %Y")

        curr = (self.curr_start, self.curr_end)
        ref = (self.ref_start, self.ref_end)


        rows = []
        for label, func_name in self._entries.items():
            func = getattr(self, func_name)
            unit = self._units.get(label, "")

            row = [label, unit, func(*ref), func(*curr)]
            rows.append(row)

        curr_label = "-".join(map(date_fmt, curr))
        ref_label = "-".join(map(date_fmt, ref))
        df = pd.DataFrame(
            rows,
            columns=["Variable", "Units", ref_label, curr_label],
        )

        df["Pct. Change"] = 100 * (df[curr_label] - df[ref_label]) / df[ref_label]
        return df

    # (potential) entries of the table

    def battery_revenues(
        self,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
    ) -> float:
        """
        Calculate the cumulative battery revenues in $/kW/year terms
        """
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)

        total_revenue = (
            self.data.index
            .loc[start:end, :]
            .sum().sum()
        )
        number_of_days = (end - start).days
        return total_revenue * (365 / number_of_days) / 1_000

    def load_net(
        self,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
    ) -> float:
        """
        Calculate the average load across CAISO, in GW
        """
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)

        return (
            self.data.net_load
            .set_index("timestamp")
            .loc[start:end, :]
            .squeeze()
            .mean()
            / 1_000
        )

    def load_total(
        self,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
    ) -> float:
        """
        Calculate the average load across CAISO, in GW
        """
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)

        load = (
            self.data.load
            .set_index("timestamp")
            .loc[start:end, :]
            .squeeze()
            .mean()
        )

        return load / 1_000

    def negative_prices(
        self,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        market: str = "da",
    ) -> float:
        """
        Count the number of hours with negative prices
        """
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)

        scale_factor = 1 if market == "da" else 12

        count = (
            self.get_price_data(market)
            .set_index("timestamp")
            .loc[start:end, :]
            ["lmp"]
            .lt(0.0)
            .sum()
        )
        return count / scale_factor

    def negative_price_magnitude(
        self,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        market: str = "da",
    ) -> float:
        """
        Compute the average price, conditional on it being a negatively
        priced period
        """
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)

        return (
            self.get_price_data(market)
            .set_index("timestamp")
            .loc[start:end, ["lmp"]]
            .query("lmp < 0")
            .squeeze()
            .mean()
        )

    def negative_price_duration(
        self,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        market: str = "da",
    ) -> float:
        """
        Compute the daily average of the duration of negative prices
        """
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)

        scale_factor = 1 if market == "da" else 12

        prices = (
            self.get_price_data(market)
            .set_index("timestamp")
            .loc[start:end, "lmp"]
        )
        return negative_duration(prices).mean() / scale_factor

    def price_spreads(
        self,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        market: str = "da",
        tb: int = 4,
    ) -> float:
        """
        Calculate the average of the TB spreads within a period, in $/MWh
        """
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)

        resolution = 60 if market == "da" else 5
        spreader = TopBottomSpread(tb, resolution)

        average_spread = (
            self.get_price_data(market)
            .set_index("timestamp")
            .loc[start:end, :]
            .resample("1d")
            ["lmp"]
            .apply(spreader)
            .mean()
        )
        return average_spread

    def regulation_prices(
        self,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        market: str = "da",
    ) -> float:
        """
        Calculate the volume-weighted average price of the reg. down and reg. up
        prices for the period, in $/MW. IFM volumes are used, not FMM
        """
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)

        volumes = (
            self.data.contracted_volumes
            .pivot_table(
                index="timestamp",
                columns="market",
                values="volume_mw",
                aggfunc="sum",
            )
            .loc[start:end, ["ifm ru", "ifm rd"]]
            .sum()
        )

        price = (
            self.get_anc_price_data(market)
            .set_index("timestamp")
            .loc[start:end, ["reg_up", "reg_down"]]
            .mean(axis="index", numeric_only=True)
        )

        return np.average(price, weights=volumes)

    def solar_generation(
        self,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
    ) -> float:
        """
        Calculate the total energy generated by solar, in TWh
        """
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)

        total_gen = (
            self.data.fuel_mix
            .set_index("timestamp")
            .loc[start:end, "solar"]
            .resample("1h")
            .mean()
            .sum()
            / 1_000_000
        )
        return total_gen

    def solar_peak(
        self,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
    ) -> float:
        """
        Calculate the average daily peak of solar generation, in GW
        """
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)

        average_peak = (
            self.data.fuel_mix
            .set_index("timestamp")
            .loc[start:end, "solar"]
            .resample("1h")
            .mean()
            .resample("1d")
            .max()
            .mean()
            / 1_000
        )
        return average_peak

    # utility methods

    def get_anc_price_data(self, market: str) -> pd.DataFrame:
        if (market == "da") or (market == "rt"):
            return getattr(self.data, f"{market}_anc")
        else:
            raise ValueError(f"unrecognized market: '{market}'")

    def get_price_data(self, market: str) -> pd.DataFrame:
        if (market == "da") or (market == "rt"):
            return getattr(self.data, f"{market}_energy")
        else:
            raise ValueError(f"unrecognized market: '{market}'")
