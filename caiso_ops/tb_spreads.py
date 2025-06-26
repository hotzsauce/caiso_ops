from __future__ import annotations

import numpy as np
import pandas as pd



class TopBottomSpread(object):

    def __init__(
        self,
        tb: int = 4,
        resolution: int = 60, # no. of minutes between observations
    ):
        self.tb = tb
        self.resolution = resolution

        self.obs_per_hour = 60 // self.resolution
        self.n = self.tb * self.obs_per_hour # no. of top and bottom obs

    def __call__(
        self,
        obj: np.ndarray,
    ):
        arranged = np.sort(obj)
        # print(arranged[:self.n])
        # print(arranged[-self.n:])
        # print()
        return arranged[-self.n:].sum() - arranged[:self.n].sum()
