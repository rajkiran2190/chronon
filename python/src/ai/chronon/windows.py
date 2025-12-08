from typing import Union

import gen_thrift.common.ttypes as common


def _days(length: int) -> common.Window:
    return common.Window(length=length, timeUnit=common.TimeUnit.DAYS)


def _hours(length: int) -> common.Window:
    return common.Window(length=length, timeUnit=common.TimeUnit.HOURS)


def _from_str(s: str) -> common.Window:
    """
    converts strings like "30d", "2h" etc into common.Window

    Args:
        s (str): Duration string in format "<number>(d|h)" where d=days, h=hours

    Returns:
        common.Window: Window object with specified duration

    Raises:
        ValueError: If string format is invalid
    """

    if not s or len(s) < 2:
        raise ValueError(f"Invalid duration format: {s}")

    # Get the numeric value and unit
    value = s[:-1]
    unit = s[-1].lower()

    try:
        length = int(value)
        if length <= 0:
            raise ValueError(f"Duration must be positive: {s}")

        if unit == "d":
            return _days(length)
        elif unit == "h":
            return _hours(length)
        else:
            raise ValueError(f"Invalid time unit '{unit}'. Must be 'd' for days or 'h' for hours")

    except ValueError as e:
        if "invalid literal for int()" in str(e):
            raise ValueError(f"Invalid numeric value in duration: {value}") from e
        raise e from None


def normalize_window(w: Union[common.Window, str]) -> common.Window:
    """
    Normalizes a window specification to a common.Window object.

    Accepts either a Window object directly or a string like "30d" or "24h".
    This is used across the codebase (e.g., in GroupBy aggregations and TrainingSpec).

    Args:
        w: Either a common.Window object or a string like "7d", "24h"

    Returns:
        common.Window: The normalized window object

    Raises:
        TypeError: If the input is neither a string nor a Window object
    """
    if isinstance(w, str):
        return _from_str(w)
    elif isinstance(w, common.Window):
        return w
    else:
        raise TypeError(
            f"Window should be either a string like '7d', '24h', or a Window type, "
            f"got {type(w).__name__}"
        )
