from __future__ import annotations

from sqlalchemy import create_engine
from trino.auth import BasicAuthentication

import pandas as pd



class SqlInterface(object):

    def __init__(
        self,
        user    : str = "trino_read",
        password: str = "SecretTrinoModoPassword123!",
        host    : str = "trino-prod.modoint.com",
        port    : int = 443,
        catalog : str = "iceberg",
        echo: bool = True,
    ):
        self.auth = BasicAuthentication(user, password)
        self.engine = create_engine(
            f"trino://{user}@{host}:{port}/{catalog}",
            connect_args={
                "http_scheme": "https",
                "auth": self.auth,
            },
            echo=echo,
        )
        self.connection = self.engine.connect()

    def read_sql(self, sql: str) -> pd.DataFrame:
        return pd.read_sql(sql, self.connection)



AND = "AND"
OR = "OR"
class AbstractFilter(object):

    def __str__(self):
        return "WITH " + self.header

    def __and__(self, other: AbstractFilter):
        if not isinstance(other, AbstractFilter):
            raise TypeError(f"Must be a filter: {type(other)}")
        return CompoundFilter([self, other], [AND])

    def __or__(self, other: AbstractFilter):
        if not isinstance(other, AbstractFilter):
            raise TypeError(f"Must be a filter: {type(other)}")
        return CompoundFilter([self, other], [OR])

    def join_str(self):
        return f"JOIN\n\t{self.table} {self.alias} USING ({self.merge})"

class CompoundFilter(AbstractFilter):

    def __init__(
        self,
        filters: Iterable[AbstractFilter],
        operators: Iterable[AND, OR]
    ):
        n = len(filters)
        m = len(operators)
        if n != m + 1:
            raise ValueError(f"mismatched number of filters ({n}) & ops ({m})")

        # TODO: delete this
        if n != 2:
            raise NotImplementedError("more than two filters not permitted")

        self.table = ""
        self.alias = "p"
        self.merge = filters[0].merge

        # we create a table that joins the IDs in the appropriate way
        self.header = ", ".join(map(lambda f: f.header, filters))
        self.filters = filters
        self.operators = operators

    def join_str(self):
        join_type= "UNION" if self.operators[0] == OR else "INTERSECT"
        fmt = lambda f: f"SELECT {f.merge} FROM {f.table}"

        f1, f2 = map(fmt, self.filters)
        a, m = self.alias, self.merge
        return f"JOIN (\n\t{f1}\n\t{join_type}\n\t{f2}\n) {a} USING ({m})"





class SqlQuery(object):

    def __init__(
        self,
        base: str,
        filt: Optional[AbstractFilter],
    ):
        self.base = base
        self.filt = filt

        if self.filt is None:
            is_cte = base.split(None, 2)[0].lower() != "select"
            if is_cte:
                self.query = "WITH " + self.base
            else:
                self.query = self.base
        else:
            where = "WHERE"
            base_parts = base.split(where)

            joins = filt.join_str()
            _base = "".join([base_parts[0], joins, where] + base_parts[1:])

            is_cte = base.split(None, 2)[1].lower() == "as"
            if is_cte:
                self.query = ",\n".join((str(self.filt), _base))
            else:
                self.query = "\n".join((str(self.filt), _base))

    def pull(self, **kwargs):
        engine = SqlInterface(**kwargs)
        return engine.read_sql(self.query)



class AncillaryServicePricesDA(SqlQuery):

    def __init__(
        self,
        first_date: str,
        final_date: str,
        filt: Optional[AbstractFilter] = None,
    ):
        base = f"""
        SELECT
            *
        FROM
            iceberg.prod.caiso_dam_as_price AS as_pr_da
        WHERE
            as_pr_da.opr_dt >= DATE '{first_date}'
            AND as_pr_da.opr_dt < DATE '{final_date}'
        ORDER BY
            as_pr_da.intervalstarttime
        """
        super().__init__(base, filt)
        self.first_date = first_date
        self.final_date = final_date

class AncillaryServicePricesRT(SqlQuery):

    def __init__(
        self,
        first_date: str,
        final_date: str,
        filt: Optional[AbstractFilter] = None,
    ):
        base = f"""
        SELECT
            *
        FROM
            iceberg.prod.caiso_fmm_as_price AS as_pr_rt
        WHERE
            as_pr_rt.opr_dt >= DATE '{first_date}'
            AND as_pr_rt.opr_dt < DATE '{final_date}'
        ORDER BY
            as_pr_rt.intervalstarttime
        """
        super().__init__(base, filt)
        self.first_date = first_date
        self.final_date = final_date

class CaisoIndexCapacity(SqlQuery):

    def __init__(
        self,
        first_date: str,
        final_date: str,
        filt: Optional[AbstractFilter] = None,
    ):
        base = f"""
        SELECT
            *
        FROM
            iceberg.prod.caiso_index_capacity AS ixc
        WHERE
            ixc.date >= DATE '{first_date}'
            AND ixc.date < DATE '{final_date}'
        ORDER BY
            ixc.date
        """
        super().__init__(base, filt)
        self.first_date = first_date
        self.final_date = final_date

class CaisoIndexPrice(SqlQuery):

    def __init__(
        self,
        first_date: str,
        final_date: str,
        filt: Optional[AbstractFilter] = None,
    ):
        base = f"""
        SELECT
            *
        FROM
            iceberg.prod.caiso_index_price_data AS ixp
        WHERE
            ixp.timestamp >= DATE '{first_date}'
            AND ixp.timestamp < DATE '{final_date}'
        ORDER BY
            ixp.timestamp
        """
        super().__init__(base, filt)
        self.first_date = first_date
        self.final_date = final_date

class CaisoIndexRevenue(SqlQuery):

    def __init__(
        self,
        first_date: str,
        final_date: str,
        filt: Optional[AbstractFilter] = None,
    ):
        base = f"""
        SELECT
            *
        FROM
            iceberg.prod.caiso_index_revenue AS ixr
        WHERE
            ixr.timestamp >= DATE '{first_date}'
            AND ixr.timestamp < DATE '{final_date}'
        ORDER BY
            ixr.timestamp
        """
        super().__init__(base, filt)
        self.first_date = first_date
        self.final_date = final_date

class CaisoIndexVolume(SqlQuery):

    def __init__(
        self,
        first_date: str,
        final_date: str,
        filt: Optional[AbstractFilter] = None,
    ):
        base = f"""
        SELECT
            *
        FROM
            iceberg.prod.caiso_index_volume_data AS ixv
        WHERE
            ixv.timestamp >= DATE '{first_date}'
            AND ixv.timestamp < DATE '{final_date}'
        ORDER BY
            ixv.timestamp
        """
        super().__init__(base, filt)
        self.first_date = first_date
        self.final_date = final_date

class EnergyPricesDA(SqlQuery):

    def __init__(
        self,
        first_date: str,
        final_date: str,
        node: str | Iterable[str],
        filt: Optional[AbstractFilter] = None,
    ):
        if isinstance(node, str):
            node_list = f"'{node}'"
        else:
            node_list = ", ".join(map(lambda x: f"'{x}'", node))

        base = f"""
        SELECT
            *
        FROM
            iceberg.prod.caiso_dam_lmp AS pr_da
        WHERE
            pr_da.opr_dt >= DATE '{first_date}'
            AND pr_da.opr_dt < DATE '{final_date}'
            AND pr_da.node IN (
                {node_list}
            )
        ORDER BY
            pr_da.intervalstarttime
        """
        super().__init__(base, filt)
        self.first_date = first_date
        self.final_date = final_date

class EnergyPricesRT(SqlQuery):

    def __init__(
        self,
        first_date: str,
        final_date: str,
        node: str | Iterable[str],
        filt: Optional[AbstractFilter] = None,
    ):
        if isinstance(node, str):
            node_list = f"'{node}'"
        else:
            node_list = ", ".join(map(lambda x: f"'{x}'", node))

        base = f"""
        SELECT
            *
        FROM
            iceberg.prod.caiso_rtd_lmp AS pr_rt
        WHERE
            pr_rt.opr_dt >= DATE '{first_date}'
            AND pr_rt.opr_dt < DATE '{final_date}'
            AND pr_rt.node IN (
                {node_list}
            )
        ORDER BY
            pr_rt.intervalstarttime
        """
        super().__init__(base, filt)
        self.first_date = first_date
        self.final_date = final_date

class GenerationFuelMix(SqlQuery):

    def __init__(
        self,
        first_date: str,
        final_date: str,
        filt: Optional[AbstractFilter] = None,
    ):
        base = f"""
        SELECT
            *
        FROM
            iceberg.prod.caiso_generation_by_fuel_type AS gen_fuel
        WHERE
            gen_fuel.time >= DATE '{first_date}'
            AND gen_fuel.time < DATE '{final_date}'
        ORDER BY
            gen_fuel.time
        """
        super().__init__(base, filt)
        self.first_date = first_date
        self.final_date = final_date

class RenewableGeneration(SqlQuery):

    def __init__(
        self,
        first_date: str,
        final_date: str,
        filt: Optional[AbstractFilter] = None,
    ):
        base = f"""
        SELECT
            *
        FROM
            iceberg.prod.caiso_wind_solar_gen AS green_gen
        WHERE
            green_gen.opr_dt >= DATE '{first_date}'
            AND green_gen.opr_dt < DATE '{final_date}'
        ORDER BY
            green_gen.intervalstarttime
        """
        super().__init__(base, filt)
        self.first_date = first_date
        self.final_date = final_date





def read_as_prices(
    first_date: str = "2025-01-01",
    final_date: str = "",
    market: str = "da",
):
    if not final_date:
        from datetime import datetime
        final_date = datetime.today().strftime("%Y-%m-%d")

    if market == "da":
        return AncillaryServicePricesDA(first_date, final_date).pull()
    elif market == "rt":
        return AncillaryServicePricesRT(first_date, final_date).pull()
    else:
        raise ValueError(f"unrecognized market: '{market}'")

def read_energy_prices(
    first_date: str = "2025-01-01",
    final_date: str = "",
    market: str = "da",
    node: str | Iterable[str] = "DGAP_CISO-APND",
):
    if not final_date:
        from datetime import datetime
        final_date = datetime.today().strftime("%Y-%m-%d")

    if market == "da":
        return EnergyPricesDA(first_date, final_date, node).pull()
    elif market == "rt":
        return EnergyPricesRT(first_date, final_date, node).pull()
    else:
        raise ValueError(f"unrecognized market: '{market}'")

def read_generation(
    first_date: str = "2025-01-01",
    final_date: str = "",
    kind: str = "all",
):
    if not final_date:
        from datetime import datetime
        final_date = datetime.today().strftime("%Y-%m-%d")

    if kind == "all":
        return GenerationFuelMix(first_date, final_date).pull()
    elif kind == "renewable":
        return RenewableGeneration(first_date, final_date).pull()
    else:
        raise NotImplementedError(f"unrecognized generation kind: '{kind}'")

def read_index_capacity(first_date: str = "2025-01-01", final_date: str = ""):
    if not final_date:
        from datetime import datetime
        final_date = datetime.today().strftime("%Y-%m-%d")
    return CaisoIndexCapacity(first_date, final_date).pull()

def read_index_price(first_date: str = "2025-01-01", final_date: str = ""):
    if not final_date:
        from datetime import datetime
        final_date = datetime.today().strftime("%Y-%m-%d")
    return CaisoIndexPrice(first_date, final_date).pull()

def read_index_revenue(first_date: str = "2025-01-01", final_date: str = ""):
    if not final_date:
        from datetime import datetime
        final_date = datetime.today().strftime("%Y-%m-%d")
    return CaisoIndexRevenue(first_date, final_date).pull()

def read_index_volume(first_date: str = "2025-01-01", final_date: str = ""):
    if not final_date:
        from datetime import datetime
        final_date = datetime.today().strftime("%Y-%m-%d")
    return CaisoIndexVolume(first_date, final_date).pull()
