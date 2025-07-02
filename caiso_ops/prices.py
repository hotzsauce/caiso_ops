"""
Methods for operations common to price data
"""
from __future__ import annotations

import numpy as np

def negative_duration(series: pd.Series) -> pd.Series:
    """
    Return the longest daily streak of negative prices.

    For each calendar day contained in *series*, this function finds the
    maximum number of **consecutive rows** whose values are strictly below
    zero.  The result is reported in the same temporal resolution in which
    *series* is provided.

    Args:
        series (pd.Series):
            A one-dimensional numeric Series indexed by a
            :class:`~pandas.DatetimeIndex`.  Any frequency is accepted; the raw
            sampling resolution is preserved in the output.

    Returns:
        pd.Series:
            A daily Series whose values are the length of the longest run of
            negative prices on that day.  If a day has no negative prices, the
            returned value is ``0``.

    Raises:
        ValueError: If *series* is not indexed by a ``DatetimeIndex``.

    Notes:
        * NaNs are interpreted as non-negative and therefore interrupt
          streaks.
        * The helper :pyfunc:`_daily_neg_duration` performs the intra-day
          computation; see its docstring for the algorithmic details.
        * This function is agnostic to day-ahead and real-time prices, so any
          sort of scaling needs to be done by the user after calling this method

    Examples:
        >>> import pandas as pd
        >>> from prices import negative_duration
        >>> s = pd.Series(
        ...     [-4, -1, 3, -6, -2, -5],
        ...     index=pd.date_range("2025-06-01 00:00", periods=6, freq="H"),
        ... )
        >>> negative_duration(s).iloc[0]
        2
    """
    return (
        (series < 0)
        .astype(int)
        .resample("1d")
        .apply(_daily_neg_duration)
    )

def _daily_neg_duration(seq: Sequence) -> int:
    """
    Compute the length of the longest contiguous block of ones.

    This helper treats *seq* as a binary sequence in which ``1`` denotes a
    negative-price period and ``0`` denotes a non-negative period.  It returns
    the length (count of elements) of the longest consecutive run of ones.

    Args:
        seq (Sequence[int] | numpy.ndarray):
            A one-dimensional array-like of zeros and ones.

    Returns:
        int: The maximum run length of ones found in *seq*.  Returns ``0`` if
        the sequence contains no ones.

    Example:
        >>> _daily_neg_duration([0, 1, 1, 0, 1])
        2
    """
    arr = np.asarray(seq, dtype=np.int8).ravel()

    # pad with sentinels and locate start and end of each run
    padded = np.concatenate(([0], arr, [0]))
    diffed = np.diff(padded)

    starts = np.flatnonzero(diffed == 1)
    ends = np.flatnonzero(diffed == -1)

    return (ends - starts).max(initial=0)
