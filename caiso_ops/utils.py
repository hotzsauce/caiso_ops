from __future__ import annotations

import numpy as np
import pandas as pd



def aggr_services(
    services: Iterable[str],
    agg_energy: bool = False,
    agg_rt_energy: bool = True,
    agg_as: bool = True,

) -> np.ndarray[str]:
    """
    Given an iterable of CAISO market labels, aggregate them to Modo's
    typical services groupings
    """

    # the middle column of `parts` is just the delimiter, oddly enough
    parts = np.char.partition(np.asarray(services, dtype=str), " ")
    market = parts[:, 0]
    service = parts[:, 2]

    is_energy = service == "energy"
    mapped = np.where(is_energy,
                      market + "_energy",
                      service)

    # optionally collapse all AS into just "as"
    if agg_as:
        mapped[~is_energy] = "as"

    # optionally collapse 'fmm', 'ifm', 'rtd', and 'ruc' energy into the
    # same bucket
    if agg_energy:
        mapped[is_energy] = "energy"
    else:
        # lump 'fmm', 'rtd', and 'ruc' together
        if agg_rt_energy:
            rt_energy = is_energy & (market != "ifm")
            da_energy = is_energy & (market == "ifm")

            mapped[rt_energy] = "rt_energy"
            mapped[da_energy] = "da_energy"

    return mapped
