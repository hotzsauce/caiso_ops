from __future__ import annotations

import re

# mapping CAISO/Modo abbreviations to display names
MARKET_DISPLAY_NAMES = {
    "ifm": "Integrated Forward Market",
    "fmm": "15-Minute Market",
    "rtd": "Real-Time Dispatch",
    "ruc": "Residual Unit Committment",
}
SERVICE_DISPLAY_NAMES = {
    "energy": "Energy",
    "sr": "Spinning Reserve",
    "nsr": "Non-Spinning Reserve",
    "rd": "Regulation Down",
    "ru": "Regulation Up",
}

def _string_to_display(string: str) -> str:
    try:
        return MARKET_DISPLAY_NAMES[string]
    except KeyError:
        pass

    try:
        return SERVICE_DISPLAY_NAMES[string]
    except KeyError:
        pass

    return string



DELIMS = r"[\s;\.\-\_]"
class CaisoFormatter(object):

    def __init__(self):
        self.cache = {}

    def __call__(self, entries: Iterable[str]) -> List[str]:
        return [self.resolve(entry) for entry in entries]

    @staticmethod
    def format(string: str) -> str:
        return " ".join(map(_string_to_display, re.split(DELIMS, string)))

    def resolve(self, string: str) -> str:
        try:
            return self.cache[string]
        except KeyError:
            formatted = self.format(string)
            self.cache[string] = formatted
            return formatted

def to_display(obj: str | Iterable[str]) -> str | Iterable[str]:
    if isinstance(obj, str):
        obj = [obj]

    fmt = CaisoFormatter()
    return fmt(obj)
