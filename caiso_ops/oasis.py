from __future__ import annotations

import io
import pandas as pd
import requests
import zipfile



class OasisInterface(object):

    url = "https://oasis.caiso.com/oasisapi/SingleZip?resultformat=6"
    queries = {
        "resource_node": "ATL_RESOURCE",
        "master_list": "ATL_GEN_CAP_LST",
    }
    query_versions = {
        "resource_node": "1",
        "master_list": "4",
    }

    def __init__(self):
        pass

    def _create_url(
        self,
        query: str,
        **kwargs,
    ):
        try:
            queryname = self.queries[query]
        except KeyError:
            raise KeyError(f"unrecognized query name: '{query}'")
        try:
            version = self.query_versions[query]
        except KeyError:
            raise KeyError(f"query '{query}' doesn't have an associated version")

        config = dict(queryname=queryname, version=version)
        config.update(**kwargs)

        return "&".join([self.url] + [k+"="+v for k, v in config.items()])

    @classmethod
    def _access_oasis(cls, url: str) -> pd.DataFrame:
        response = requests.get(url)
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            frames = []
            for file in zf.namelist():
                if _is_datafile(file):
                    df = pd.read_csv(zf.open(file))
                    frames.append(df)
            return pd.concat(frames, axis="index")

    def pull(
        self,
        query: str,
        first_date: str,
        final_date: str = "",
        resource_id: str = "ALL",
        agge_type: str = "ALL",
        resource_type: str = "ALL",
        **kwargs,
    ):
        startdate, enddate = _format_caiso_dates(first_date, final_date)

        access_point = self._create_url(
            query,
            startdatetime=startdate,
            enddatetime=enddate,
            resource_id=resource_id,
            agge_type=agge_type,
            resource_type=resource_type,
        )
        return self._access_oasis(access_point)



def _format_caiso_dates(
    first_date: str,
    final_date: str = "",
) -> Tuple[str, str]:

    first_date = pd.to_datetime(first_date)
    if final_date:
        final_date = pd.to_datetime(final_date)
    else:
        final_date = first_date + pd.Timedelta(days=1)

    return (
        _caiso_strftime(first_date),
        _caiso_strftime(final_date),
    )

def _caiso_strftime(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y%m%d") + "T07:00-0000"

def _is_datafile(filename: str) -> bool:
    return filename.endswith(".csv")
