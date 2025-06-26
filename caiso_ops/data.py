from __future__ import annotations

import numpy as np
import pandas as pd
import pathlib
import re
import zipfile

import caiso_ops.oasis as oasis
import caiso_ops.sql as sql
import caiso_ops.utils as utils



DATA = pathlib.Path().home().resolve() / "docs" / "data" / "caiso"



def vcat(iterable, process):
    # utility function to stop the following
    # frames = []
    # for thing in container:
    #   ...
    # return pd.concat(frames, axis="index")
    frames = [process(file) for file in filter_ds_store(iterable)]
    return pd.concat(frames, axis="index")

def filter_ds_store(iterable):
    for file in iterable:
        if not is_ds_store(file):
            yield file

def is_ds_store(obj):
    str_obj = str(obj)
    return str_obj.split("/")[-1] == ".DS_Store"



class DataFetcher(object):

    def __init__(
        self,
        in_dir: str,
        out_file: str,
        pool: str | Path = DATA,
        sql_interface: Optional[Callable] = None,
        warn: bool = True,
    ):
        self.in_dir = in_dir
        self.out_file = out_file
        self.pool = pathlib.Path(str(pool)).resolve()
        self.sql_interface = sql_interface
        self.warn = warn

    def load(self, *args, **kwargs) -> pd.DataFrame:
        if self.target.exists():
            return pd.read_parquet(self.target)
        else:
            df = self.read(*args, **kwargs)
            df = self.process(df)
            df.to_parquet(self.target)
            return df

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def read(self, *args, **kwargs) -> pd.DataFrame:
        if self.source.exists() and not self.source.is_dir():
            return self.read_local_data()
        elif (
            self.source.exists()
            and self.source.is_dir()
            and any(self.source.iterdir())
        ):
            # if it's an empty directory, control flow doesn't go here
            return self.read_local_data()
        else:
            if self.sql_interface is None:
                cls = type(self).__name__
                msg = (
                    f"'{cls}' objects don't have a SQL interface; pull the data"
                    " manually."
                )
                raise RuntimeError(msg)

            self.source.mkdir(parents=True, exist_ok=True)
            source_file = self.source / "data.parquet"

            df = self.sql_interface(*args, **kwargs)
            df.to_parquet(source_file)
            return self.read_local_data()

    def read_local_data(self):
        if self.warn:
            import warnings
            warnings.warn(
                "\nusing default local data reader",
                UserWarning
            )
        return vcat(self.source.iterdir(), self.unzip_or_read_single_file)

    def read_single_file(self, io: IO) -> pd.DataFrame:
        try:
            return pd.read_csv(io)
        except:
            try:
                return pd.read_parquet(io)
            except:
                try:
                    return pd.read_excel(io)
                except:
                    str_io = str(io) # not sure this is generalizable
                    filetype = str_io.split(".")[-1]
                    raise TypeError(f"unrecognized filetype: {filetype}")

    def unzip(self, path: Path) -> pd.DataFrame:
        with zipfile.ZipFile(path) as zf:
            return vcat(
                zf.namelist(),
                lambda f: self.read_single_file(zf.open(f))
            )

    def unzip_or_read_single_file(self, path: Path) -> pd.DataFrame:
        """
        Given a path, either unpack the zip file or read the single file
        """
        try:
            suffix = path.suffix
        except AttributeError:
            path = pathlib.Path(str(path))
            suffix = path.suffix

        if suffix == ".zip":
            return self.unzip(path)
        else:
            # raise NotImplementedError("not yet")
            return self.read_single_file(path)

    @property
    def source(self):
        return self.pool / self.in_dir

    @property
    def target(self):
        return self.pool / self.out_file



class AncillaryServicePriceFetcher(DataFetcher):

    def __init__(
        self,
        market: str,
        in_dir: str = "as_prices",
        out_file: str = "as_prices.parquet",
    ):
        def puller(*args, **kwargs):
            return sql.read_as_prices(*args, market=market, **kwargs)
        in_dir = in_dir + f"/{market}"
        out_file = out_file.split(".")[0] + f"_{market}.parquet"
        super().__init__(in_dir, out_file, sql_interface=puller)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        # UTC -> California time; then strip tzinfo
        date = "intervalstarttime"
        df[date] = df[date].dt.tz_convert("US/Pacific").dt.tz_localize(None)

        to_drop = [
            "intervalendtime", "opr_dt", "opr_hr", "opr_interval", "opr_type",
            "market_run_id", "price_unit",
        ]
        df.drop(columns=to_drop, inplace=True)

        return (
            df
            .rename(columns={date: "timestamp"})
            .sort_values("timestamp")
        )

class IndexCapacityFetcher(DataFetcher):

    def __init__(
        self,
        in_dir: str = "index_capacity",
        out_file: str = "index_capacity.parquet",
    ):
        def puller(*args, **kwargs):
            return sql.read_index_capacity(*args, **kwargs)
        super().__init__(in_dir, out_file, sql_interface=puller)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        # UTC -> California time; then strip tzinfo
        date = "date"
        df[date] = pd.to_datetime(df[date])

        df.sort_values(date, inplace=True)
        return df

class IndexPriceFetcher(DataFetcher):

    def __init__(
        self,
        in_dir: str = "index_price",
        out_file: str = "index_price.parquet",
    ):
        def puller(*args, **kwargs):
            return sql.read_index_price(*args, **kwargs)
        super().__init__(in_dir, out_file, sql_interface=puller)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        # UTC -> California time; then strip tzinfo
        date = "timestamp"
        df[date] = df[date].dt.tz_convert("US/Pacific").dt.tz_localize(None)

        # save some memory
        df["market"] = df["market"].astype("category")
        df["price_unit"] = df["price_unit"].astype("category")

        df.sort_values([date, "market"], inplace=True)
        df.drop(columns=["date"], inplace=True)
        return df

class IndexRevenueFetcher(DataFetcher):

    def __init__(
        self,
        in_dir: str = "index_revenue",
        out_file: str = "index_revenue.parquet",
    ):
        def puller(*args, **kwargs):
            return sql.read_index_revenue(*args, **kwargs)
        super().__init__(in_dir, out_file, sql_interface=puller)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        # UTC -> California time; then strip tzinfo
        date = "timestamp"
        df[date] = df[date].dt.tz_convert("US/Pacific").dt.tz_localize(None)

        # save some data
        df["market"] = df["market"].astype("category")

        df.sort_values([date, "market"], inplace=True)
        df.drop(columns=["date"], inplace=True)
        return df

class IndexVolumeFetcher(DataFetcher):

    def __init__(
        self,
        in_dir: str = "index_volume",
        out_file: str = "index_volume.parquet",
    ):
        def puller(*args, **kwargs):
            return sql.read_index_volume(*args, **kwargs)
        super().__init__(in_dir, out_file, sql_interface=puller)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        # UTC -> California time; then strip tzinfo
        date = "timestamp"
        df[date] = df[date].dt.tz_convert("US/Pacific").dt.tz_localize(None)

        # save some data
        df["market"] = df["market"].astype("category")

        df.sort_values([date, "market"], inplace=True)
        df.drop(columns=["date", "period_length"], inplace=True)
        return df

class EnergyPriceFetcher(DataFetcher):

    def __init__(
        self,
        market: str,
        in_dir: str = "energy_prices",
        out_file: str = "energy_prices.parquet",
    ):
        def puller(*args, **kwargs):
            return sql.read_energy_prices(*args, market=market, **kwargs)
        in_dir = in_dir + f"/{market}"
        out_file = out_file.split(".")[0] + f"_{market}.parquet"
        super().__init__(in_dir, out_file, sql_interface=puller)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        # UTC -> California time; then strip tzinfo
        date = "intervalstarttime"
        df[date] = df[date].dt.tz_convert("US/Pacific").dt.tz_localize(None)

        to_drop = [
            "intervalendtime", "opr_dt", "opr_hr", "opr_interval",
            "market_run_id", "price_unit",
        ]
        df.drop(columns=to_drop, inplace=True)

        return (
            df
            .rename(columns={date: "timestamp"})
            .sort_values("timestamp")
        )

class LoadFetcher(DataFetcher):

    def __init__(
        self,
        in_dir: str = "load",
        out_file: str = "load.parquet",
    ):
        def puller(*args, **kwargs):
            raise NotImplementedError("SQL load pull")
            return sql.read_generation(*args, kind="all", **kwargs)
        super().__init__(in_dir, out_file, sql_interface=puller)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        # UTC -> California time; then strip tzinfo
        date = "interval_start_local"
        df[date] = df[date].dt.tz_localize(None)

        to_drop = [
            "interval_start_utc",
            "interval_end_local",
            "interval_end_utc"
        ]
        return (
            df
            .drop(columns=to_drop)
            .rename(columns={date: "timestamp"})
        )

class NodalEnergyPriceFetcher(EnergyPriceFetcher):

    def __init__(
        self,
        market: str,
        in_dir: str = "energy_prices/nodal",
        out_file: str = "energy_prices_nodal.parquet",
    ):
        super().__init__(market, in_dir, out_file)

class GenerationFuelMixFetcher(DataFetcher):
    """
    Note this is meant for GridStatus' `caiso_fuel_mix` dataset
    """

    def __init__(
        self,
        in_dir: str = "generation/all",
        out_file: str = "generation_all.parquet",
    ):
        def puller(*args, **kwargs):
            raise NotImplementedError("SQL fuel mix pull")
            return sql.read_generation(*args, kind="all", **kwargs)
        super().__init__(in_dir, out_file, sql_interface=puller)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        # UTC -> California time; then strip tzinfo
        date = "interval_start_local"
        df[date] = df[date].dt.tz_localize(None)

        to_drop = [
            "interval_start_utc",
            "interval_end_local",
            "interval_end_utc"
        ]
        return (
            df
            .drop(columns=to_drop)
            .rename(columns={date: "timestamp"})
        )

class RenewableGenerationFetcher(DataFetcher):

    def __init__(
        self,
        in_dir: str = "generation/renewable",
        out_file: str = "generation_renewable.parquet",
    ):
        def puller(*args, **kwargs):
            return sql.read_generation(*args, kind="renewable", **kwargs)
        super().__init__(in_dir, out_file, sql_interface=puller)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        # UTC -> California time; then strip tzinfo
        date = "intervalstarttime"
        df[date] = df[date].dt.tz_convert("US/Pacific").dt.tz_localize(None)

        to_drop = [
            "intervalendtime", "opr_dt", "opr_hr", "opr_interval",
            "market_run_id", "market_run_id_pos", "renew_pos", "group",
        ]
        df.drop(columns=to_drop, inplace=True)

        return (
            df
            .rename(columns={date: "timestamp"})
            .sort_values("timestamp")
        )

class ResourceNodeFetcher(DataFetcher):

    def __init__(
        self,
        in_dir: str = "resource_node",
        out_file: str = "resource_node.parquet",
    ):
        def puller(*args, **kwargs):
            engine = oasis.OasisInterface()
            return engine.pull("resource_node", *args, **kwargs)
        super().__init__(in_dir, out_file, sql_interface=puller)

    process = lambda _, df: df

class MasterListFetcher(DataFetcher):

    def __init__(
        self,
        in_dir: str = "master_list",
        out_file: str = "master_list.parquet",
    ):
        def puller(*args, **kwargs):
            engine = oasis.OasisInterface()
            return engine.pull("master_list", *args, **kwargs)
        super().__init__(in_dir, out_file, sql_interface=puller)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        # replicates Ovais's process in his `caiso_benchmark.ipynb` notebook
        print(df)

        rows = (df["RESOURCE_AGG_TYPE"] == "N") & (df["ENERGY_SOURCE"] == "LESR")
        cols = [
            "RESOURCE_ID", "GEN_UNIT_NAME", "NET_DEPENDABLE_CAPACITY",
            "NAMEPLATE_CAPACITY", "OWNER_OR_QF", "ENERGY_SOURCE", "ZONE",
            "PTO_AREA", "COD" , "BAA_ID", "UDC"
        ]

        return (
            df.loc[rows, cols]
            .reset_index(drop=True)
            .dropna(subset=["COD"])
            .drop_duplicates(subset=["RESOURCE_ID"])
            .sort_values(by=["COD"], ascending=False)
        )




def fetch_asset_database(date: str = ""):
    return (
        fetch_master_list(date)
        .merge(
            fetch_resource_nodes(date),
            on="RESOURCE_ID",
            how="left",
        )
    )

def fetch_as_prices(
    market: str = "da",
    *args,
    **kwargs,
):
    if (market == "da") or (market == "rt"):
        fetcher = AncillaryServicePriceFetcher(market)
        return fetcher.load(*args, **kwargs)
    else:
        raise ValueError(f"unrecognized market: '{market}'")

def fetch_energy_prices(
    market: str = "da",
    nodal: bool = False,
    *args,
    **kwargs
) -> pd.DataFrame:
    if (market == "da") or (market == "rt"):
        if nodal:
            return fetch_energy_prices_nodal(market, *args, **kwargs)
        fetcher = EnergyPriceFetcher(market)
        return fetcher.load(*args, **kwargs)
    else:
        raise ValueError(f"unrecognized market: '{market}'")

def fetch_energy_prices_nodal(
    market: str = "da",
    *args,
    **kwargs
) -> pd.DataFrame:
    if (market == "da") or (market == "rt"):
        # use the master list and resource nodes to get a list of nodes that
        # are attached to batteries
        nodes = fetch_asset_database()["NODE_ID"].tolist()
        fetcher = NodalEnergyPriceFetcher(market)
        # raise NotImplementedError("fetch_energy_prices_nodal")
        return fetcher.load(*args, node=nodes, **kwargs)
    else:
        raise ValueError(f"unrecognized market: '{market}'")

def fetch_generation(kind: str = "all", *args, **kwargs) -> pd.DataFrame:
    if kind == "all":
        fetcher = GenerationFuelMixFetcher()
    elif kind == "renewable":
        fetcher = RenewableGenerationFetcher()
    else:
        raise ValueError(f"unrecognized generation kind: '{kind}'")
    return fetcher.load(*args, **kwargs)

def fetch_index(norm: str = "mw", **kwargs):
    """
    Calculate the CAISO index at a 5-minute frequency
    """

    # aggregate the services and energy revenue streams
    rev = fetch_index_revenue()
    rev["service"] = utils.aggr_services(rev.market, **kwargs)

    out = (
        rev
        .groupby(["timestamp", "service"], as_index=False)
        ["revenue"]
        .sum()
        .pivot(index="timestamp", columns="service", values="revenue")
        .fillna(0.0) # `ruc_energy` observations are NaNs sometimes
    )
    service_columns = out.columns.to_list()

    # luckily don't need to do much here
    denom = f"{norm}_capacity"
    cap = fetch_index_capacity()

    # compute the index value!
    ix = (
        out.join(
            cap.set_index("date")[denom],
            on=out.index.floor("D"),
        )
        .drop(columns=["key_0"])
    )
    ix[service_columns] = ix[service_columns].div(ix[denom], axis=0)

    # could optionally drop rows with all-NaNs here
    return ix.drop(columns=[denom])

def fetch_index_capacity(*args, **kwargs) -> pd.DataFrame:
    fetcher = IndexCapacityFetcher()
    return fetcher.load(*args, **kwargs)

def fetch_index_price(*args, **kwargs) -> pd.DataFrame:
    fetcher = IndexPriceFetcher()
    return fetcher.load(*args, **kwargs)

def fetch_index_revenue(*args, **kwargs) -> pd.DataFrame:
    fetcher = IndexRevenueFetcher()
    return fetcher.load(*args, **kwargs)

def fetch_index_volume(*args, **kwargs) -> pd.DataFrame:
    fetcher = IndexVolumeFetcher()
    return fetcher.load(*args, **kwargs)

def fetch_load(*args, **kwargs) -> pd.DataFrame:
    fetcher = LoadFetcher()
    return fetcher.load(*args, **kwargs)

def fetch_master_list(date: str = ""):
    if not date:
        # import datetime
        from datetime import datetime
        date = datetime.today().strftime("%Y-%m-%d")
    fetcher = MasterListFetcher()
    return fetcher.load(first_date=date)

def fetch_resource_nodes(date: str = ""):
    if not date:
        from datetime import datetime
        date = datetime.today().strftime("%Y-%m-%d")
    fetcher = ResourceNodeFetcher()
    return fetcher.load(first_date=date)
