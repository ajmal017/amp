import enum


class AssetClass(enum.Enum):
    Futures = "Futures"
    ETFs = "etfs"
    Forex = "forex"
    Stocks = "stocks"
    SP500 = "sp_500"


class Frequency(enum.Enum):
    Minutely = "T"
    Daily = "D"
    Hourly = "H"
    Tick = "tick"


class ContractType(enum.Enum):
    Continuous = "continuous"
    Expiry = "expiry"


class Extension(enum.Enum):
    CSV = "csv"
    Parquet = "pq"
