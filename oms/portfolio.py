"""
Import as:

import oms.portfolio as omportfo
"""
import abc
import collections
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from tqdm.autonotebook import tqdm

import core.key_sorted_ordered_dict as cksoordi
import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hsql as hsql
import oms.broker as ombroker

_LOG = logging.getLogger(__name__)


# #############################################################################
# AbstractPortfolio
# #############################################################################


# TODO(gp): -> Portfolio?
class AbstractPortfolio(abc.ABC):
    """
    Store holdings over time, e.g., many shares of each asset are owned at any
    time.

    Cash is treated as any other asset to keep code uniform.

    The tables are indexed by knowledge time, i.e., when this information became
    known by this object.
    """

    # ID of asset representing cash.
    CASH_ID: int = -1

    # An `holding_df` represents the holdings over time, e.g.,
    # ```
    #                            asset_id  curr_num_shares
    # 2000-01-01 09:35:00-05:00        -1        1000000.0
    # 2000-01-01 09:35:00-05:00       314        1000000.0
    # ...
    # 2000-01-01 09:30:00-05:00       214       -1000000.0
    # ```
    # Columns required in a `holding_df`.
    HOLDINGS_COLS = ["asset_id", "curr_num_shares"]

    # Columns that a dataframe with prices should have.
    PRICE_COLS = ["price", "value"]

    def __init__(
        self,
        broker: ombroker.AbstractBroker,
        mark_to_market_col: str,
        pricing_method: str,
        initial_holdings: pd.Series,
        *,
        retrieve_initial_holdings_from_db: bool = False,
        max_num_bars: Optional[int] = 100,
    ):
        """
        Constructor.

        :param mark_to_market_col: column name used as price to mark holdings to
            market
        :param pricing_method: pricing methodology to use for valuing assets.
            If e.g. "twap", then we also include the bar duration as a
            pandas-style suffix: "twap.5T"
        :param initial_holdings: initial positions in shares indexed by integer
            asset_ids; no NaNs allowed unless
            `retrieve_initial_holdings_from_db=True`, in which case all values
            must be NaN.
        :param retrieve_initial_holdings_from_db: `True` iff holdings are
            initialized via an external database. The asset ids of nonzero
            holdings must be a subset of the index of `initial_holdings`.
        :param max_num_bars: maximum number of bars to store in memory; if
            `None`, then impose no restriction.
        """
        _LOG.debug(hprint.to_str("mark_to_market_col"))
        # Set and unpack broker.
        hdbg.dassert_issubclass(broker, ombroker.AbstractBroker)
        self.broker = broker
        self._account = broker.account
        self._timestamp_col = broker.timestamp_col
        # Extract `market_data` from `broker`.
        self.market_data = broker.market_data
        # Extract `get_wall_clock_time` from `market_data`.
        self._get_wall_clock_time = broker.market_data.get_wall_clock_time
        self._asset_id_col = broker.market_data.asset_id_col
        self._mark_to_market_col = mark_to_market_col
        # Parse `pricing_method`.
        self._pricing_type, self._bar_duration = self._parse_pricing_method(
            pricing_method
        )
        # Initialize bookkeeping dictionaries.
        # At each call to `mark_to_market()`, we capture `wall_clock_time` and
        # perform a sequence of updates to the following dictionaries.
        # We initialize the collection of dictionaries from `holdings_df`.
        # - timestamp to pd.Series of holdings in shares (indexed by asset_id)
        self._max_num_bars = max_num_bars
        self._asset_holdings = cksoordi.KeySortedOrderedDict(
            pd.Timestamp, self._max_num_bars
        )
        # - timestamp to float
        self._cash = cksoordi.KeySortedOrderedDict(
            pd.Timestamp, self._max_num_bars
        )
        # - timestamp to pd.DataFrame of price, value (indexed by asset_id)
        self._assets_marked_to_market = cksoordi.KeySortedOrderedDict(
            pd.Timestamp, self._max_num_bars
        )
        # - timestamp to pd.Series of notional flows (indexed by asset_id)
        self._flows = cksoordi.KeySortedOrderedDict(
            pd.Timestamp, self._max_num_bars
        )
        # - timestamp to pd.Series of statistics
        self._statistics = cksoordi.KeySortedOrderedDict(
            pd.Timestamp, self._max_num_bars
        )
        # Validate universe and holdings.
        self._retrieve_initial_holdings_from_db = (
            retrieve_initial_holdings_from_db
        )
        # The client passed initial holdings and not just the allowed
        # universe, so we need to make sure that the holdings are valid
        # (e.g., contain no NaNs).
        if not self._retrieve_initial_holdings_from_db:
            self._validate_initial_holdings(initial_holdings)
        initial_holdings.index.name = "asset_id"
        initial_holdings.name = "curr_num_shares"
        self._initial_holdings = initial_holdings
        # Set the initial universe.
        self._initial_universe = initial_holdings.index.drop(
            AbstractPortfolio.CASH_ID
        )

    def __str__(self) -> str:
        """
        Return the state of the Portfolio in terms of the holdings as a string.
        """
        act = []
        num_periods = None
        precision = 2
        act.append(
            "# historical holdings=\n%s"
            % hpandas.df_to_str(
                self.get_historical_holdings(num_periods),
                num_rows=None,
                precision=precision,
            )
        )
        act.append(
            "# historical holdings marked to market=\n%s"
            % hpandas.df_to_str(
                self.get_historical_holdings_marked_to_market(num_periods),
                num_rows=None,
                precision=precision,
            )
        )
        act.append(
            "# historical flows=\n%s"
            % hpandas.df_to_str(
                self.get_historical_flows(num_periods),
                num_rows=None,
                precision=precision,
            )
        )
        act.append(
            "# historical pnl=\n%s"
            % hpandas.df_to_str(
                self.get_historical_pnl(num_periods),
                num_rows=None,
                precision=precision,
            )
        )
        act.append(
            "# historical statistics=\n%s"
            % hpandas.df_to_str(
                self.get_historical_statistics(num_periods),
                num_rows=None,
                precision=precision,
            )
        )
        act = "\n".join(act)
        return act

    @staticmethod
    def read_state(
        log_dir: str,
        *,
        tz: str = "America/New_York",
        cast_asset_ids_to_int: bool = True,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Read and process logged portfolio state.
        """
        holdings_df = AbstractPortfolio._load_df_from_files(
            log_dir, "holdings", tz
        )
        holdings_mtm_df = AbstractPortfolio._load_df_from_files(
            log_dir, "holdings_marked_to_market", tz
        )
        flows_df = AbstractPortfolio._load_df_from_files(log_dir, "flows", tz)
        stats_df = AbstractPortfolio._load_df_from_files(
            log_dir, "statistics", tz
        )
        if cast_asset_ids_to_int:
            holdings_df.columns = holdings_df.columns.astype("int64")
            holdings_mtm_df.columns = holdings_mtm_df.columns.astype("int64")
            flows_df.columns = flows_df.columns.astype("int64")
        pnl_df = AbstractPortfolio._compute_pnl(holdings_mtm_df, flows_df)
        dfs = {
            "holdings": holdings_df,
            "holdings_marked_to_market": holdings_mtm_df,
            "flows": flows_df,
            "pnl": pnl_df,
        }
        portfolio_df = pd.concat(dfs.values(), axis=1, keys=dfs.keys())
        return portfolio_df, stats_df

    @classmethod
    def from_cash(
        cls,
        *args: Any,
        initial_cash: float,
        asset_ids: Optional[List[int]] = None,
        **kwargs: Any,
    ) -> "AbstractPortfolio":
        """
        Initialize with no non-cash assets.
        """
        if initial_cash < 0:
            _LOG.warning("Initial cash=%0.2f", initial_cash)
        if not asset_ids:
            asset_ids = []
        hdbg.dassert_not_in(AbstractPortfolio.CASH_ID, asset_ids)
        holdings_dict = {asset_id: 0 for asset_id in asset_ids}
        holdings_dict[AbstractPortfolio.CASH_ID] = initial_cash
        portfolio = cls.from_dict(
            *args,
            holdings_dict=holdings_dict,
            **kwargs,
        )
        return portfolio

    @classmethod
    def from_dict(
        cls,
        *args: Any,
        holdings_dict: Dict[int, float],
        **kwargs: Any,
    ) -> "AbstractPortfolio":
        """
        Initialize from a dict of holdings and initial timestamp.

        :param holdings_dict: dictionary from asset_id to position
        """
        hdbg.dassert_isinstance(holdings_dict, dict)
        initial_holdings = pd.Series(holdings_dict)
        portfolio = cls(
            *args,
            initial_holdings,
            **kwargs,
        )
        return portfolio

    @property
    def universe(self) -> List[int]:
        # TODO(Paul): Consider making this time-dependent.
        return self._initial_universe.to_list()

    def get_asset_ids_not_in_universe(self, asset_ids: List[int]) -> List[int]:
        """
        Get subset of asset ids not in universe. Empty list if null.

        :param asset_ids: query asset_ids
        :return: asset ids in `asset_ids` not in portfolio universe.
        """
        idx = pd.Index(asset_ids)
        diff = idx.difference(self._initial_universe)
        return diff.to_list()

    def mark_to_market(self) -> pd.DataFrame:
        """
        Mark the portfolio to market.

        This function checks the portfolio state at `wall_clock_time` and
        updates the internal state.
          - Holdings are as of `wall_clock_time` (e.g., any updates from fills)
          - Uses `market_data` to price holdings
          - Computes portfolios statistics such as leverage, exposure, etc.

        # TODO(Paul): Add a dataframe snippet.

        :return: dataframe with HOLDINGS and PRICE columns
        """
        _LOG.debug("Marking to market...")
        # The first time this function is called we set the "initial holdings".
        # TODO(Paul): See if `_set_holdings()` can call into `mark_to_market()`
        #     or vice versa.
        if not self._asset_holdings:
            self._set_holdings(self._initial_holdings)
        else:
            # Update asset_holdings, cash.
            self._observe_holdings()
            # Get the latest timestamp.
            timestamp, asset_holdings = self._asset_holdings.peek()
            # Price the assets.
            self._price_assets(asset_holdings)
            # Calculate statistics.
            self._compute_statistics()
            # Perform sanity-checks.
            # These four dictionaries should have the same keys. Here we check that
            # they have the same length.
            # TODO(Paul): Maybe check keys instead of length.
            hdbg.dassert_eq(
                len(self._asset_holdings), len(self._assets_marked_to_market)
            )
            hdbg.dassert_eq(len(self._asset_holdings), len(self._cash))
            hdbg.dassert_eq(len(self._asset_holdings), len(self._statistics))
        #
        df = self.get_cached_mark_to_market()
        _LOG.debug("mark_to_market_df=\n%s", hpandas.df_to_str(df, num_rows=None))
        return df

    def get_cached_mark_to_market(self) -> pd.DataFrame:
        """
        Retrieve the last cached mark-to-market dataframe.

        This does not perform a new observation of the market.

        return: same as `mark_to_market()`
        """
        # TODO(Paul): Gracefully fail instead of assert.
        hdbg.dassert(self._asset_holdings, "No cached information available.")
        # Get latest timestamp available.
        timestamp, asset_holdings = self._asset_holdings.peek()
        _LOG.debug("Retrieving holdings at timestamp=%s", timestamp)
        # Create a `holdings_df` with assets and cash.
        holdings_df = asset_holdings.reset_index()
        cash_df = AbstractPortfolio._create_holdings_df_from_cash(
            self._cash[timestamp], timestamp
        )
        holdings_df.columns = AbstractPortfolio.HOLDINGS_COLS
        holdings_df.index = [timestamp] * holdings_df.shape[0]
        holdings_df = pd.concat([holdings_df, cash_df])
        holdings_df = holdings_df.convert_dtypes()
        # Get mark-to-market values.
        marked_to_market = self._assets_marked_to_market[timestamp]
        cash_row = pd.DataFrame(
            index=[AbstractPortfolio.CASH_ID],
            columns=AbstractPortfolio.PRICE_COLS,
            data=[[1.0, self._cash[timestamp]]],
        )
        marked_to_market = pd.concat([marked_to_market, cash_row])
        marked_to_market.index.name = "asset_id"
        marked_to_market = marked_to_market.reset_index()
        marked_to_market.index = [timestamp] * marked_to_market.index.shape[0]
        # Merge holdings and mark-to-market dataframes.
        df = holdings_df.merge(marked_to_market, on="asset_id")
        df["wall_clock_timestamp"] = [timestamp] * df.shape[0]
        df.index = [timestamp] * df.shape[0]
        df = df.convert_dtypes()
        return df

    def get_historical_statistics(
        self, num_periods: Optional[int] = 10
    ) -> pd.DataFrame:
        """
        Return a dataframe of portfolio statistics over time.
        """
        stats_odict = self._statistics.get_ordered_dict(num_periods)
        df = pd.DataFrame(stats_odict).transpose()
        # Add `pnl` by diffing the snapshots of `net_wealth`.
        # pnl = df["net_wealth"].diff().rename("pnl").to_frame()
        # In principle, thw two PnL calculations should agree. However, if
        # a price for a bar is missing, this second method is more stable.
        pnl = (
            self.get_historical_pnl(num_periods=num_periods)
            .sum(axis=1, min_count=1)
            .rename("pnl")
            .to_frame()
        )
        df = pnl.merge(df, how="outer", left_index=True, right_index=True)
        df = df.astype("float")
        return df

    def get_historical_holdings(
        self, num_periods: Optional[int] = 10
    ) -> pd.DataFrame:
        """
        Return a dataframe of portfolio holdings in shares over time.
        """
        asset_holdings_odict = self._asset_holdings.get_ordered_dict(num_periods)
        asset_holdings = pd.DataFrame(asset_holdings_odict).transpose()
        cash_odict = self._cash.get_ordered_dict(num_periods)
        cash = pd.DataFrame(cash_odict.values(), cash_odict.keys())
        cash.columns = [AbstractPortfolio.CASH_ID]
        asset_holdings = pd.concat([asset_holdings, cash], axis=1)
        asset_holdings.columns.name = self._asset_id_col
        # Explicitly cast to float. This makes the string representation of
        # the dataframe more uniform and better.
        # We replace NaNs with numpy NaNs, since numpy NaNs are recognized as
        # having "float" type.
        asset_holdings.fillna(np.nan, inplace=True)
        asset_holdings = asset_holdings.astype("float")
        return asset_holdings

    def get_historical_holdings_marked_to_market(
        self, num_periods: Optional[int] = 10
    ) -> pd.DataFrame:
        """
        Return a dataframe of portfolio holdings in dollars over time.
        """
        asset_values = collections.OrderedDict()
        for k, v in self._assets_marked_to_market.get_ordered_dict(
            num_periods
        ).items():
            asset_values[k] = v["value"]
        asset_values = pd.DataFrame(asset_values).transpose()
        cash_odict = self._cash.get_ordered_dict(num_periods)
        cash = pd.DataFrame(cash_odict.values(), cash_odict.keys())
        cash.columns = [AbstractPortfolio.CASH_ID]
        asset_values = pd.concat([asset_values, cash], axis=1)
        asset_values.columns.name = self._asset_id_col
        # Explicitly cast to float. This makes the string representation of
        # the dataframe more uniform and better.
        # We replace NaNs with numpy NaNs, since numpy NaNs are recognized as
        # having "float" type.
        asset_values.fillna(np.nan, inplace=True)
        asset_values = asset_values.astype("float")
        return asset_values

    def get_historical_flows(
        self, num_periods: Optional[int] = 10
    ) -> pd.DataFrame:
        """
        Return a dataframe of asset cash flows over time.
        """
        flows_odict = self._flows.get_ordered_dict(num_periods)
        flows = pd.DataFrame(flows_odict).transpose()
        flows.columns.name = self._asset_id_col
        # Explicitly cast to float. This makes the string representation of
        # the dataframe more uniform and better.
        # We replace NaNs with numpy NaNs, since numpy NaNs are recognized as
        # having "float" type.
        flows.fillna(np.nan, inplace=True)
        flows = flows.astype("float")
        return flows

    def get_historical_pnl(self, num_periods: Optional[int] = 10) -> pd.DataFrame:
        """
        Return a dataframe of per-asset PnL over time.
        """
        # Because the pnl calculation takes a diff, we extract an extra period
        # for this operation.
        if num_periods is not None:
            num_periods += 1
        # Get snapshots of assets marked to market.
        mtm = self.get_historical_holdings_marked_to_market(num_periods)
        flows = self.get_historical_flows(num_periods)
        # Compute PnL.
        pnl = self._compute_pnl(mtm, flows)
        #
        pnl.columns.name = self._asset_id_col
        pnl = pnl.astype("float")
        if num_periods is not None:
            pnl = pnl.tail(num_periods - 1)
        return pnl

    def log_state(self, log_dir: str, num_periods: Optional[int] = 1) -> str:
        # TODO(Paul): Change this so that it logs only the most recent state.
        hdbg.dassert(log_dir, "Must specify `log_dir` to log state.")
        #
        wall_clock_time = self._get_wall_clock_time()
        wall_clock_time_str = wall_clock_time.strftime("%Y%m%d_%H%M%S")
        file_name = f"{wall_clock_time_str}.csv"
        #
        holdings_df = self.get_historical_holdings(num_periods)
        AbstractPortfolio._write_df(holdings_df, log_dir, "holdings", file_name)
        #
        holdings_mtm_df = self.get_historical_holdings_marked_to_market(
            num_periods
        )
        AbstractPortfolio._write_df(
            holdings_mtm_df, log_dir, "holdings_marked_to_market", file_name
        )
        #
        flows_df = self.get_historical_flows(num_periods)
        AbstractPortfolio._write_df(flows_df, log_dir, "flows", file_name)
        #
        stats_df = self.get_historical_statistics(num_periods)
        AbstractPortfolio._write_df(stats_df, log_dir, "statistics", file_name)
        return file_name

    def price_assets(self, asset_ids: List[int]) -> pd.Series:
        """
        Wrap `portfolio.market_data` and packages output.

        :param asset_ids: as in `market_data.get_data_at_timestamp()`
        :return: series of prices at `as_of_timestamp` indexed by asset_id
        """
        if self._pricing_type == "last":
            prices = self.market_data.get_last_price(
                self._mark_to_market_col, asset_ids
            )
        elif self._pricing_type == "twap":
            prices = self.market_data.get_last_twap_price(
                self._bar_duration,
                self._timestamp_col,
                asset_ids,
                self._mark_to_market_col,
            )
        else:
            raise NotImplementedError
        hdbg.dassert_eq(self._mark_to_market_col, prices.name)
        prices.index.name = "asset_id"
        prices.name = "price"
        hdbg.dassert(not prices.index.has_duplicates)
        return prices

    @staticmethod
    def _load_df_from_files(
        log_dir: str,
        name: str,
        tz: str,
    ) -> pd.DataFrame:
        dir_name = os.path.join(log_dir, name)
        pattern = "*"
        only_files = True
        use_relative_paths = True
        files = hio.listdir(dir_name, pattern, only_files, use_relative_paths)
        files.sort()
        dfs = []
        for file_name in tqdm(files, desc=f"Loading `{name}` files..."):
            df = AbstractPortfolio._read_df(log_dir, name, file_name, tz)
            dfs.append(df)
        df = pd.concat(dfs)
        hdbg.dassert(
            not df.index.has_duplicates,
            "Duplicated indices for `%s`=\n%s",
            name,
            df.index[df.index.duplicated()],
        )
        return df

    @staticmethod
    def _parse_pricing_method(pricing_method: str) -> Tuple[str, Optional[str]]:
        hdbg.dassert_isinstance(pricing_method, str)
        if pricing_method == "last":
            pricing_type = "last"
            bar_duration = None
        else:
            split_str = pricing_method.split(".")
            hdbg.dassert_eq(len(split_str), 2)
            pricing_type = split_str[0]
            hdbg.dassert_in(pricing_type, ["twap", "vwap"])
            pricing_type = pricing_type
            bar_duration = split_str[1]
            hdbg.dassert(
                pd.Timedelta(bar_duration),
                "Cannot convert %s to `pd.Timedelta`" % bar_duration,
            )
        return pricing_type, bar_duration

    @staticmethod
    def _compute_pnl(
        holdings_marked_to_market: pd.DataFrame,
        flows: pd.DataFrame,
    ) -> pd.DataFrame:
        # Drop the cash balance.
        holdings_marked_to_market = holdings_marked_to_market.drop(
            columns=AbstractPortfolio.CASH_ID
        )
        cols = holdings_marked_to_market.columns.union(flows.columns)
        holdings_marked_to_market = holdings_marked_to_market.reindex(
            columns=cols, fill_value=0
        )
        flows = flows.reindex(columns=cols, fill_value=0)
        # Get per-bar flows and compute PnL.
        pnl = holdings_marked_to_market.diff().add(flows)
        return pnl

    @staticmethod
    def _write_df(
        df: pd.DataFrame,
        log_dir: str,
        name: str,
        file_name: str,
    ) -> None:
        path = os.path.join(log_dir, name, file_name)
        hio.create_enclosing_dir(path, incremental=True)
        df.to_csv(path)

    @staticmethod
    def _read_df(
        log_dir: str,
        name: str,
        file_name: str,
        tz: str,
    ) -> pd.DataFrame:
        path = os.path.join(log_dir, name, file_name)
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        # TODO(Paul): Add better checks. The first `flows` dataframe has no
        # rows, and so when parsed it does not have a DatetimeIndex.
        if isinstance(df.index, pd.DatetimeIndex):
            df.index = df.index.tz_convert(tz)
        return df

    @staticmethod
    def _create_holdings_df_from_cash(
        cash: float, timestamp: pd.Timestamp
    ) -> pd.DataFrame:
        # The holdings_df has a single row about cash.
        row = [AbstractPortfolio.CASH_ID, cash]
        holdings_df = pd.DataFrame(
            [row],
            index=[timestamp],
            columns=AbstractPortfolio.HOLDINGS_COLS,
        )
        return holdings_df

    @staticmethod
    def _validate_mark_to_market_df(df: pd.DataFrame) -> None:
        """
        Ensure that `df` passes basic sanity checks for `mark_to_market()`.
        """
        # The input should be a dataframe satisfying the following column
        # constraints.
        hdbg.dassert_isinstance(df, pd.DataFrame)
        hdbg.dassert_is_subset(AbstractPortfolio.HOLDINGS_COLS, df.columns)
        hdbg.dassert_not_in("price", df.columns)
        hdbg.dassert_not_in("value", df.columns)
        # The columns should be of the correct types. Skip if the dataframe is
        # empty (since the correct types are not inferred in that case).
        if not df.empty:
            hpandas.dassert_series_type_is(
                df["asset_id"],
                np.int64,
                "The column `asset_id` should only contain integer ids.",
            )
            hpandas.dassert_series_type_in(
                df["curr_num_shares"],
                [np.float64, np.int64],
                "The column `curr_num_shares` should be a float column.",
            )
        # There should be no more than one row per asset.
        hdbg.dassert_no_duplicates(df["asset_id"].to_list())
        # All share values should be finite.
        hdbg.dassert(
            np.isfinite(df["curr_num_shares"]).all(),
            "All share values must be finite.",
        )

    @staticmethod
    def _validate_initial_holdings(initial_holdings: pd.Series) -> None:
        """
        Ensure that `initial_holdings` passes basic sanity checks.
        """
        idx = initial_holdings.index
        # The index must be an Int64Index (asset_id must be an int) with no
        # duplicates. The ID for cash must be present.
        hdbg.dassert(not idx.has_duplicates)
        hdbg.dassert_isinstance(idx, pd.Int64Index)
        hdbg.dassert_in(
            AbstractPortfolio.CASH_ID,
            idx,
            "No cash holdings available.",
        )
        # The holdings must be numerical. They represent number of shares.
        hdbg.dassert_in(
            initial_holdings.dtype.type,
            [np.float64, np.int64],
            "The initial holdings should consist of numerical values (in shares).",
        )
        # All share values should be finite.
        hdbg.dassert(
            np.isfinite(initial_holdings).all(),
            "All share values must be finite.",
        )
        initial_cash = initial_holdings.iloc[AbstractPortfolio.CASH_ID]
        if initial_cash < 0:
            _LOG.warning("Initial cash balance=%0.2f", initial_cash)

    def _set_holdings(self, holdings: pd.Series) -> None:
        """
        Set portfolio holdings in shares and price the portfolio.

        This covers two main use cases:
        - Lazy initialization of the portfolio holdings
        - Resetting portfolio holdings overnight to account for splits/dividends
        """
        # Get the timestamp from the wall clock.
        timestamp = self._get_wall_clock_time()
        _LOG.debug("timestamp=%s", timestamp)
        _LOG.debug("Setting asset_holdings...")
        asset_holdings = holdings[holdings.index != AbstractPortfolio.CASH_ID]
        asset_holdings.name = timestamp
        hdbg.dassert_isinstance(asset_holdings, pd.Series)
        _LOG.debug("`asset_holdings`=\n%s", hpandas.df_to_str(asset_holdings))
        hdbg.dassert(not asset_holdings.index.has_duplicates)
        self._asset_holdings[timestamp] = asset_holdings
        _LOG.debug("asset_holdings set.")
        _LOG.debug("Setting cash...")
        cash = holdings.loc[AbstractPortfolio.CASH_ID]
        _LOG.debug("`cash`=%0.2f", cash)
        self._cash[timestamp] = cash
        _LOG.debug("cash set.")
        _LOG.debug("Setting assets_marked_to_market...")
        # Price the assets at the initial timestamp.
        self._price_assets(asset_holdings)
        _LOG.debug("assets_marked_to_market set.")
        _LOG.debug("Calculating statistics...")
        # Compute the initial portfolio statistics.
        self._compute_statistics()
        _LOG.debug("statistics calculated.")

    @abc.abstractmethod
    def _observe_holdings(self) -> None:
        """
        Update holdings and cash at the current wall clock time.
        """
        ...

    @abc.abstractmethod
    def _initialize_holdings_from_db(
        self, initial_holdings: pd.Series
    ) -> pd.Series:
        """
        Initialize holdings after querying an external db.

        Use for:
        - BOD initialization
        - Intraday recovery
        """
        ...

    def _price_assets(self, asset_ids: pd.Series) -> None:
        """
        Access the underlying market_data to price assets.

        :param asset_ids: series of share counts indexed by asset id
        :return: series of asset values
        """
        as_of_timestamp, _ = self._asset_holdings.peek()
        _LOG.debug("_price_assets `as_of_timestamp`=%s", as_of_timestamp)
        hdbg.dassert_isinstance(asset_ids, pd.Series)
        asset_ids_list = asset_ids.index.to_list()
        # TODO(*): Get the market as-of timestamp.
        if not asset_ids_list:
            # prices = pd.Series(name=as_of_timestamp)
            assets_marked_to_market = pd.DataFrame(
                columns=AbstractPortfolio.PRICE_COLS
            )
        else:
            # TODO(gp): A bit weird that we are calling the public method from the
            # private.
            prices = self.price_assets(asset_ids_list)
            assets_marked_to_market = asset_ids * prices
            assets_marked_to_market.name = "value"
            assets_marked_to_market = pd.concat(
                [prices, assets_marked_to_market], axis=1
            )
            assets_marked_to_market.columns = AbstractPortfolio.PRICE_COLS
        hdbg.dassert_isinstance(assets_marked_to_market, pd.DataFrame)
        hdbg.dassert(not assets_marked_to_market.index.has_duplicates)
        self._assets_marked_to_market[as_of_timestamp] = assets_marked_to_market

    def _compute_statistics(self) -> None:
        """
        Compute various portfolio statistics using latest holdings and prices.

        Return asset/cash values, net wealth, exposure, and leverage for
        the portfolio at a given timestamp.
        """
        cash_ts, _ = self._cash.peek()
        assets_ts, assets_marked_to_market = self._assets_marked_to_market.peek()
        hdbg.dassert_eq(cash_ts, assets_ts)
        hdbg.dassert_not_in(cash_ts, self._statistics)
        # Compute value of holdings.
        asset_holdings = assets_marked_to_market["value"]
        is_finite = asset_holdings.apply(np.isfinite)
        holdings_net_value = asset_holdings[is_finite].sum()
        hdbg.dassert(
            np.isfinite(holdings_net_value),
            "holdings_net_value=%s",
            holdings_net_value,
        )
        # Get the cash available.
        cash = self._cash[cash_ts]
        hdbg.dassert(np.isfinite(cash), "cash=%s", cash)
        # Compute the net wealth (AKA "total value" AKA "NAV").
        net_wealth = holdings_net_value + cash
        hdbg.dassert(np.isfinite(net_wealth), "net_value=%s", net_wealth)
        # Compute the gross exposure.
        gross_exposure = asset_holdings[is_finite].abs().sum()
        # Compute the portfolio leverage.
        leverage = gross_exposure / net_wealth
        # Compute the gross and net volume.
        if assets_ts in self._flows:
            traded_volume = -1 * self._flows[assets_ts]
            gross_volume = traded_volume.abs().sum()
            net_volume = traded_volume.sum()
        else:
            gross_volume = 0
            net_volume = 0
        dict_ = {
            "gross_volume": gross_volume,
            "net_volume": net_volume,
            "gmv": gross_exposure,
            "nmv": holdings_net_value,
            "cash": cash,
            "net_wealth": net_wealth,
            "leverage": leverage,
        }
        statistics = pd.Series(dict_, name=cash_ts)
        self._statistics[cash_ts] = statistics


# #############################################################################
# DataFramePortfolio
# #############################################################################


class DataFramePortfolio(AbstractPortfolio):

    # A `fills_df` represents orders that have been executed (e.g., how many shares,
    # at how much).
    # Columns required in a `fills_df`.
    FILLS_COLS = [
        "asset_id",
        "timestamp",
        "num_shares",
        "price",
    ]

    @staticmethod
    def _validate_fills_df(fills_df: pd.DataFrame) -> None:
        """
        Ensure that `fills_df` passes basic sanity checks.
        """
        # The input should be a nonempty dataframe.
        hdbg.dassert_isinstance(fills_df, pd.DataFrame)
        hdbg.dassert(not fills_df.empty, "The dataframe must be nonempty.")
        # The dataframe must have the correct columns.
        hdbg.dassert_is_subset(
            DataFramePortfolio.FILLS_COLS,
            fills_df.columns.to_list(),
            "Columns do not conform to requirements.",
        )
        # The columns should be of the correct types.
        hdbg.dassert_eq(
            fills_df["asset_id"].dtype.type,
            np.int64,
            "The column `asset_id` should only contain integer ids.",
        )
        hdbg.dassert_eq(
            fills_df["num_shares"].dtype.type,
            np.float64,
            "The column `curr_num_shares` should be a float column.",
        )
        #
        hdbg.dassert_eq(fills_df["timestamp"].dtype.type, pd.Timestamp)
        hdbg.dassert(hasattr(fills_df["timestamp"].dtype, "tz"))
        # The dataframe must not contain a row for cash.
        hdbg.dassert_not_in(
            DataFramePortfolio.CASH_ID,
            fills_df["asset_id"].to_list(),
            "Order for cash detected.",
        )
        # There should be no more than one row per asset.
        hdbg.dassert_no_duplicates(fills_df["asset_id"].to_list())
        # All share values should be finite.
        _LOG.debug("fills_df=%s", fills_df)
        hdbg.dassert(
            np.isfinite(fills_df["num_shares"]).all(),
            "All share values must be finite.",
        )

    def _observe_holdings(self) -> None:
        """
        Update holdings at the current wall clock time using fills information.
        """
        # Get fills.
        fills_df = self._get_fills()
        # Get latest holdings
        prev_asset_holdings_ts, prev_asset_holdings = self._asset_holdings.peek()
        prev_cash_holdings_ts, prev_cash = self._cash.peek()
        wall_clock_timestamp = self._get_wall_clock_time()
        hdbg.dassert_lt(prev_cash_holdings_ts, wall_clock_timestamp)
        new_cash = prev_cash
        # Update holdings using the `fills_df`.
        new_asset_holdings_srs = prev_asset_holdings.copy()
        flows = pd.Series([], dtype="float64")
        if fills_df is not None:
            DataFramePortfolio._validate_fills_df(fills_df)
            # last_timestamp <= fills_df.index <= timestamp
            hdbg.dassert_lte(prev_asset_holdings_ts, fills_df["timestamp"].min())
            hdbg.dassert_lte(fills_df["timestamp"].max(), wall_clock_timestamp)
            holdings_diff = fills_df.set_index("asset_id")["num_shares"]
            transaction_price = fills_df.set_index("asset_id")["price"]
            flows = -1 * transaction_price * holdings_diff
            cash_diff = flows.sum()
            hdbg.dassert(np.isfinite(cash_diff))
            new_asset_holdings_srs = new_asset_holdings_srs.add(
                holdings_diff, fill_value=0
            )
            new_cash += cash_diff
        hdbg.dassert(not flows.index.has_duplicates)
        self._flows[wall_clock_timestamp] = flows
        hdbg.dassert(not new_asset_holdings_srs.index.has_duplicates)
        self._asset_holdings[wall_clock_timestamp] = new_asset_holdings_srs
        self._cash[wall_clock_timestamp] = new_cash

    def _initialize_holdings_from_db(self) -> pd.Series:
        # For now we don't assume that `DataFramePortfolio` is saved on permanent
        # storage, since it is typically used for simulations.
        # TODO(Paul, GP): Consider allowing saving and retrieval of
        #  `DataFramePortfolio`.
        raise NotImplementedError

    def _get_fills(self) -> pd.DataFrame:
        """
        Get the fills from the broker and convert it into a `fills_df`.

        :return: fills_df
        """
        _LOG.debug("")
        # Get the fills from the broker.
        fills = self.broker.get_fills()
        # Convert the fills into a `fills_df`.
        fill_rows = []
        for fill in fills:
            _LOG.debug("# Processing fill=%s", fill)
            fill_row: Dict[str, Any] = collections.OrderedDict()
            # Copy contents of the fill.
            fill_row.update(fill.to_dict())
            fill_rows.append(pd.Series(fill_row))
        if fill_rows:
            fills_df = pd.concat(fill_rows, axis=1).transpose()
            fills_df = fills_df.convert_dtypes()
        else:
            fills_df = None
        _LOG.debug(
            "fills_df=\n%s",
            hpandas.df_to_str(fills_df, num_rows=None, precision=2),
        )
        return fills_df


# #############################################################################
# MockedPortfolio
# #############################################################################


# TODO(gp): -> DatabasePortfolio, DbPortfolio, DbPortfolio
#  The important characteristic is how it's implemented rather than that it's a
#  mocked version of the implemented system.
#  In fact the implemented Portfolio and Broker descend from this class because
#  they implement the logic talking to the DB.
class MockedPortfolio(AbstractPortfolio):
    """
    Portfolio class using a DB to store the state of the holdings.

    A `snapshot_df` contains the image of the current holdings (excluding cash)
    in the account and it is maintained by an external DB, e.g.,

    ```
     tradedate           published_dt   curr_pos
               asset_id         target_pos       net_cost
    2021-12-09    10005  ... 11:54:28  0.0     0      ...
    2021-12-09    10006  ... 11:54:28  0.0     0      ...
    2021-12-09    10009  ... 00:00:00  0.0     0      ...
    ```
    """

    def __init__(
        self,
        *args: Any,
        db_connection: hsql.DbConnection,
        table_name: str,
        poll_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        """
        Constructor.
        """
        super().__init__(*args, **kwargs)
        #
        self._db_connection = db_connection
        self._table_name = table_name
        #
        if poll_kwargs is None:
            poll_kwargs = hasynci.get_poll_kwargs(self._get_wall_clock_time)
        self._poll_kwargs = poll_kwargs
        # wall clock timestamp -> snapshot_df (i.e., the image of the holdings in
        # the account, without cash).
        self._timestamp_to_snapshot_df = collections.OrderedDict()
        # wall clock timestamp -> total net cost of transactions since the BOD.
        self._net_cost = cksoordi.KeySortedOrderedDict(pd.Timestamp)
        if self._retrieve_initial_holdings_from_db:
            self._initial_holdings = self._initialize_holdings_from_db(
                self._initial_holdings
            )
            self._validate_initial_holdings(self._initial_holdings)

    def _observe_holdings(self) -> None:
        # The current positions table has the following fields:
        # - tradedate (e.g., 2021-10-28)
        # - id (e.g., 10005)
        # - strategyid (e.g., SAU1)
        # - account (e.g., SAU1_CAND)
        # - published_dt (e.g., 2021-10-28 12:01:49.41)
        # - target_position (e.g., 300)
        #   = the fully realized portfolio position
        #   - E.g., the value is 300 if we own 100 AAPL and we want get 200 more so
        #     we send an order for 200
        # - current_position (e.g., 100)
        #   = what we own (e.g., 100 AAPL)
        # - open_quantity (e.g., 200)
        #   = how many shares we have orders open in the market. In other words,
        #     open quantity reflects how much is out getting executed in the market
        #   - Note that it's not always
        #     `open_quantity = target_position - current_position`
        #     since orders might have been cancelled. If `open_quantity = 0` it
        #     means that there are no order in the market
        #   - E.g., if we send orders for 200 AAPL, then current_position = 200, but
        #     if we cancel the orders, current_position = 0, even if target_position
        #     reports what we were targeting 200 shares
        # - net_cost (e.g., 0.0)
        #   = fill-quantity * signed fill_price with respect to the BOD price
        #   - In practice it is the average price paid
        # - currency (e.g., USD)
        # - fx (e.g., 1.0)
        # - action (e.g., BOD)
        #   - TODO(gp): Find out what it means and if interesting
        # - bod_position (e.g., 0)
        #   - = number of shares at BOD
        # - bod_price (e.g., 0.0)
        #   - = price of a share at BOD
        # Wait until the portfolio is stable.
        # TODO(gp): We can either assert or wait until the portfolio is stable.
        # Get the current positions.
        query = []
        query.append(f"SELECT * FROM {self._table_name}")
        wall_clock_timestamp = self._get_wall_clock_time()
        _LOG.debug("wall_clock_timestamp=%s", wall_clock_timestamp)
        trade_date = wall_clock_timestamp.date()
        # Restrict query to portfolio universe.
        hdbg.dassert(self.universe, "Universe is empty.")
        _LOG.debug("universe=\n%s", self.universe)
        universe = tuple(self.universe)
        if len(universe) == 1:
            universe = str(universe)[:-2] + ")"
        else:
            universe = universe
        # TODO(Paul): Make sure that we do not exclude IDs with a nonzero current /
        #  target position (ensure that these are included in the universe).
        where_clause = [f"WHERE tradedate='{trade_date}'"]
        where_clause.append(f"AND {self._asset_id_col} IN {universe}")
        if self._account is not None:
            where_clause.append(f"AND account='{self._account}'")
        query.append(" ".join(where_clause))
        #
        query.append(f"ORDER BY {self._asset_id_col}")
        query = "\n".join(query)
        _LOG.debug("query=%s", query)
        # Retrieve the data from the DB.
        snapshot_df = hsql.execute_query_to_df(self._db_connection, query)
        snapshot_df.rename(columns={self._asset_id_col: "asset_id"}, inplace=True)
        # `snapshot_df` looks like:
        # ```
        #  tradedate asset_id        published_dt  target_position  current_position
        # 2021-12-09    10005 2021-12-09 11:54:28  0.0              0
        # 2021-12-09    10006 2021-12-09 11:54:28  0.0              0
        # 2021-12-09    10009 1970-01-01 00:00:00  0.0              0
        # ```
        _LOG.debug(
            "snapshot_df=\n%s",
            hpandas.df_to_str(snapshot_df, num_rows=None, precision=2),
        )
        if not snapshot_df.empty:
            hdbg.dassert_no_duplicates(
                snapshot_df["asset_id"],
                "Each asset_id should be unique in a snapshot_df",
            )
        if False:
            file_name = f"snapshot_df.{wall_clock_timestamp}.csv"
            file_name = file_name.replace(" ", "_")
            _LOG.debug("Saving %s", file_name)
            snapshot_df.to_csv(file_name)
        # Update cash from snapshot_df.
        self._update_cash(snapshot_df, wall_clock_timestamp)
        # Update asset holdings from snapshot_df.
        asset_holdings = snapshot_df[["asset_id", "current_position"]].set_index(
            "asset_id"
        )["current_position"]
        hdbg.dassert_isinstance(asset_holdings, pd.Series)
        _LOG.debug("asset_holdings=%s", asset_holdings)
        asset_holdings = asset_holdings.reindex(
            index=self._initial_universe, copy=False
        )
        asset_holdings.name = wall_clock_timestamp
        # If the database does not have an entry for an asset (e.g., as in
        # a mock database without universe initialization), then a NaN is
        # returned.
        _LOG.debug("Number of NaN asset_holdings=%d", asset_holdings.isna().sum())
        asset_holdings.fillna(0, inplace=True)
        hdbg.dassert(not asset_holdings.index.has_duplicates)
        self._asset_holdings[wall_clock_timestamp] = asset_holdings
        # Update snapshot_df.
        hdbg.dassert(not snapshot_df.index.has_duplicates)
        self._timestamp_to_snapshot_df[wall_clock_timestamp] = snapshot_df

    def _initialize_holdings_from_db(
        self, initial_holdings: pd.Series
    ) -> pd.Series:
        # TODO(*): Factor out the query.
        # All initial holdings must be NaN when we invoke this method.
        hdbg.dassert_eq(initial_holdings.count(), 0)
        #
        query = []
        query.append(f"SELECT * FROM {self._table_name}")
        wall_clock_timestamp = self._get_wall_clock_time()
        _LOG.debug("wall_clock_timestamp=%s", wall_clock_timestamp)
        trade_date = wall_clock_timestamp.date()
        where_clause = [f"WHERE tradedate='{trade_date}'"]
        if self._account is not None:
            where_clause.append(f"AND account='{self._account}'")
        query.append(" ".join(where_clause))
        #
        query.append(f"ORDER BY {self._asset_id_col}")
        query = "\n".join(query)
        _LOG.debug("query=%s", query)
        # Retrieve the data from the DB.
        snapshot_df = hsql.execute_query_to_df(self._db_connection, query)
        snapshot_df.rename(columns={self._asset_id_col: "asset_id"}, inplace=True)
        _LOG.debug(
            "snapshot_df=\n%s",
            hpandas.df_to_str(snapshot_df, num_rows=None, precision=2),
        )
        if not snapshot_df.empty:
            hdbg.dassert_no_duplicates(
                snapshot_df["asset_id"],
                "Each asset_id should be unique in a snapshot_df",
            )
        # Get current nonzero positions.
        holdings = snapshot_df[["asset_id", "current_position"]].set_index(
            "asset_id"
        )["current_position"]
        hdbg.dassert_isinstance(holdings, pd.Series)
        nonzero_holdings = holdings[holdings != 0]
        hdbg.dassert_isinstance(nonzero_holdings, pd.Series)
        # Ensure that the nonzero positions are a subset of the universe
        # specified by the index of `initial_holdings`.
        hdbg.dassert_is_subset(
            nonzero_holdings.index.to_list(), initial_holdings.index.to_list()
        )
        # Set zero holdings and combine with nonzero initial holdings.
        initial_holdings = initial_holdings.fillna(0)
        hdbg.dassert_isinstance(initial_holdings, pd.Series)
        initial_holdings = initial_holdings.add(nonzero_holdings, fill_value=0)
        return initial_holdings

    def _convert_to_holdings_df(
        self, snapshot_df: pd.DataFrame, as_of_timestamp: pd.Timestamp
    ) -> pd.DataFrame:
        """
        Convert a snapshot_df from SQL query into a `holdings_df`.
        """
        holdings_df = snapshot_df[["asset_id", "current_position"]]
        holdings_df.columns = AbstractPortfolio.HOLDINGS_COLS
        holdings_df.index = [as_of_timestamp] * snapshot_df.shape[0]
        holdings_df = holdings_df.convert_dtypes()
        return holdings_df

    def _get_net_cost(self, snapshot_df: pd.DataFrame) -> pd.Series:
        """
        Return the `net_cost` of assets stored in a `snapshot_df`.

        This is a helper for `_update_cash()`.
        """
        if snapshot_df.empty:
            return pd.Series([], dtype="float64")
        hdbg.dassert_in("net_cost", snapshot_df.columns)
        # A long position has negative net cost.
        net_cost = -1 * snapshot_df.set_index("asset_id")["net_cost"]
        _LOG.debug("net_cost (cumulative)=%f", net_cost.sum())
        return net_cost

    def _update_cash(
        self,
        snapshot_df: pd.DataFrame,
        wall_clock_timestamp: pd.Timestamp,
    ) -> None:
        """
        Compute the current cash amount using:

        - the net cost information from `snapshot_df`; and
        - the previous cash amount
        """
        # `snapshot_df` should not have CASH_ID.
        hdbg.dassert_not_in(AbstractPortfolio.CASH_ID, snapshot_df["asset_id"])
        # Get the cash at the previous timestamp.
        prev_cash_ts, prev_cash = self._cash.peek()
        hdbg.dassert_lt(prev_cash_ts, wall_clock_timestamp)
        _LOG.debug("prev_cash=%s", prev_cash)
        hdbg.dassert(np.isfinite(prev_cash), "prev_cash=%s", prev_cash)
        # Get the net cost at the previous timestamp.
        if self._net_cost:
            prev_net_cost_ts, prev_net_costs = self._net_cost.peek()
            hdbg.dassert_eq(prev_net_cost_ts, prev_cash_ts)
        else:
            _, asset_holdings = self._asset_holdings.peek()
            idx = asset_holdings.index
            prev_net_costs = pd.Series(0, idx)
        prev_net_cost = prev_net_costs.sum()
        # Get the current net cost.
        current_net_costs = self._get_net_cost(snapshot_df)
        current_net_cost = current_net_costs.sum()
        hdbg.dassert(
            np.isfinite(current_net_cost),
            "current_net_cost (cumulative)=%f",
            current_net_cost,
        )
        # The cost of the previous transactions is the difference of net cost.
        cost = current_net_cost - prev_net_cost
        _LOG.debug("cost (net_cost diff)=%f", cost)
        hdbg.dassert(np.isfinite(cost))
        # The current cash is given by the previous cash and the cash spent in the
        # previous transactions.
        updated_cash = prev_cash - cost
        _LOG.debug("updated_cash=%s", updated_cash)
        # Update the cash and net cost.
        self._cash[wall_clock_timestamp] = updated_cash
        self._net_cost[wall_clock_timestamp] = current_net_costs
        flows = -1 * current_net_costs.subtract(prev_net_costs, fill_value=0.0)
        hdbg.dassert(not flows.index.has_duplicates)
        self._flows[wall_clock_timestamp] = flows
