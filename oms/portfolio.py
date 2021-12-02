"""
Import as:

import oms.portfolio as omportfo
"""
import collections
import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

import core.dataflow.price_interface as cdtfprint
import helpers.dbg as hdbg
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)


# TODO(gp): Add an abstract interface and other implementations.
class Portfolio:
    """
    Store tables with the following info:

    - holdings over time
        - how many shares of each asset are owned at any time
        - cash is treated as any other asset to keep code uniform
    - orders executed over time
        - what orders were executed, for how many shares, and at how much

    The tables are indexed by knowledge time, i.e., when this information became
    known by this object.
    """

    # ID of asset representing cash.
    CASH_ID: int = -1

    HOLDINGS_COLS = ["asset_id", "curr_num_shares"]
    ORDER_COLS = [
        "asset_id",
        "start_timestamp",
        "end_timestamp",
        "num_shares_filled",
        "execution_price",
    ]
    PRICE_COLS = ["asset_id", "price"]

    def __init__(
        self,
        # TODO(GP): Add docstrings for these.
        strategy_id: str,
        account: str,
        #
        price_interface: cdtfprint.AbstractPriceInterface,
        asset_id_col: str,
        mark_to_market_col: str,
        timestamp_col: str,
        holdings_df: pd.DataFrame,
    ):
        """
        Constructor.

        :param asset_id_col: column name in the output df of `price_interface`
            storing the asset id
        :param mark_to_market_col: column name used to mark holdings to market
        """
        _LOG.debug(
            hprint.to_str("strategy_id account asset_id_col mark_to_market_col")
        )
        self._strategy_id = strategy_id
        self._account = account
        #
        hdbg.dassert_issubclass(price_interface, cdtfprint.AbstractPriceInterface)
        self._price_interface = price_interface
        self._asset_id_col = asset_id_col
        self._mark_to_market_col = mark_to_market_col
        self._timestamp_col = timestamp_col
        self._validate_initial_holdings_df(holdings_df)
        self._holdings = holdings_df
        self._characteristics = collections.OrderedDict()

    def __str__(self) -> str:
        act = []
        act.append("# holdings=\n%s" % hprint.dataframe_to_str(self.holdings))
        act = "\n".join(act)
        return act

    @classmethod
    def from_cash(
        cls,
        strategy_id: str,
        account: str,
        price_interface: cdtfprint.AbstractPriceInterface,
        asset_id_col: str,
        mark_to_market_col: str,
        timestamp_col: str,
        initial_cash: float,
        initial_timestamp: pd.Timestamp,
    ) -> "Portfolio":
        """
        Initialize a portfolio with no non-cash assets.
        """
        hdbg.dassert_lt(0, initial_cash)
        row = [Portfolio.CASH_ID, initial_cash]
        holdings_df = pd.DataFrame(
            [row], index=[initial_timestamp], columns=Portfolio.HOLDINGS_COLS
        )
        portfolio = cls(
            strategy_id,
            account,
            price_interface,
            asset_id_col,
            mark_to_market_col,
            timestamp_col,
            holdings_df,
        )
        return portfolio

    @classmethod
    def from_dict(
        cls,
        strategy_id: str,
        account: str,
        price_interface: cdtfprint.AbstractPriceInterface,
        asset_id_col: str,
        mark_to_market_col: str,
        timestamp_col: str,
        holdings_dict: Dict[int, float],
        initial_timestamp: pd.Timestamp,
    ) -> "Portfolio":
        """
        Initialize from a dict of holdings and initial timestamp.
        """
        hdbg.dassert_isinstance(holdings_dict, dict)
        df = pd.Series(holdings_dict).to_frame().reset_index()
        df.columns = Portfolio.HOLDINGS_COLS
        hdbg.dassert_isinstance(initial_timestamp, pd.Timestamp)
        size = len(holdings_dict)
        df.index = [initial_timestamp] * size
        portfolio = cls(
            strategy_id,
            account,
            price_interface,
            asset_id_col,
            mark_to_market_col,
            timestamp_col,
            df,
        )
        return portfolio

    @property
    def holdings(self) -> pd.DataFrame:
        return self._holdings

    @property
    def orders(self) -> pd.DataFrame:
        return self._orders

    def get_last_timestamp(self) -> pd.Timestamp:
        """
        Get the last time known to Portfolio (holdings).
        """
        return self._holdings.index.max()

    def get_holdings(
        self,
        timestamp: pd.Timestamp,
        asset_id: Optional[Any],
        *,
        exclude_cash: bool = False,
    ) -> pd.DataFrame:
        """
        Return the holdings in shares at `timestamp` for one or all the assets.

        :param timestamp: as-of timestamp
        :param asset_id: consider only a specific asset. `None` means all holdings.
        :param exclude_cash: exclude cash from the holdings
        :return: a dataframe with asset id and num_shares
            ```
                                       asset_id  curr_num_shares
            2000-01-01 09:35:00-05:00      -1.0        1000000.0
            ```

        - empty dataframe if there are no holdings for the requested timestamp
        `timestamp`
        """
        return Portfolio._get_holdings(
            self._holdings, timestamp, asset_id, exclude_cash=exclude_cash
        )

    def get_asset_price_at_timestamp(
        self,
        timestamp: pd.Timestamp,
        asset_ids: List[int],
    ) -> pd.DataFrame:
        """
        Use `price_interface` to price assets at `timestamp`.

        `CASH_ID` may be included among `asset_ids`. If it is, it is not
        priced using `price_interface`, but rather is priced at `1.0`.

        :return: df with columns `Portfolio.PRICE_COLS`
        """
        hdbg.dassert_isinstance(asset_ids, list)
        asset_ids = sorted(asset_ids)
        hdbg.dassert_no_duplicates(asset_ids)
        non_cash_asset_ids = list(
            filter(lambda x: x != Portfolio.CASH_ID, asset_ids)
        )
        price_df = self._price_interface.get_data_at_timestamp(
            timestamp, self._timestamp_col, non_cash_asset_ids
        )
        hdbg.dassert_eq(
            price_df.shape[0],
            len(non_cash_asset_ids),
            "Some assets have no price. Attempted to access price for"
            "`non_cash_asset_ids=%s` and found prices for `asset_ids=%s`",
            non_cash_asset_ids,
            price_df["asset_id"].to_list(),
        )
        _LOG.debug("price_df=\n%s", hprint.dataframe_to_str(price_df))
        # Extract subset of price information.
        columns = [self._asset_id_col, self._mark_to_market_col]
        hdbg.dassert_is_subset(columns, price_df.columns)
        price_df = price_df[columns]
        price_df.columns = Portfolio.PRICE_COLS
        price_df.index = [timestamp] * price_df.index.size
        if len(non_cash_asset_ids) < len(asset_ids):
            cash_df = pd.DataFrame(
                {Portfolio.PRICE_COLS[0]: -1, Portfolio.PRICE_COLS[1]: 1},
                index=[timestamp],
            )
            price_df = pd.concat([cash_df, price_df])
        return price_df

    def mark_to_market(
        self, timestamp: pd.Timestamp, df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Mark holdings to market prices available at `timestamp`.

        :param timestamp: as-of time for holdings and price
        :param df: a dataframe that
            - has columns HOLDINGS_COLS
            - does not have columns PRICE_COLS
            - does not have duplicate asset_ids
            - does not have NaN `curr_num_shares`
        :return: `df` merged with asset prices (valued at `timestamp`) and
            valued according to `price` and `curr_num_shares`
        """
        _LOG.debug(
            "\n%s",
            hprint.frame("mark_to_market: timestamp=%s" % timestamp, char1="<"),
        )
        Portfolio._validate_mark_to_market_df(df)
        if df.empty:
            cols = Portfolio.HOLDINGS_COLS + ["value", "price"]
            result_df = pd.DataFrame(columns=cols)
            return result_df
        # Get asset ids.
        asset_ids = df["asset_id"].to_list()
        hdbg.dassert_lt(0, len(asset_ids))
        _LOG.debug("asset_ids=%s", asset_ids)
        price_df = self.get_asset_price_at_timestamp(timestamp, asset_ids)
        hdbg.dassert(not price_df.empty)
        # Merge the holdings with the prices.
        result_df = pd.merge(
            df,
            price_df,
            how="outer",
            left_on="asset_id",
            right_on=self._asset_id_col,
        )
        # Compute the value.
        result_df["value"] = result_df["curr_num_shares"] * result_df["price"]
        result_df.index = df.index
        # TODO(gp): We should have no nans in res_df["value"].
        return result_df

    def get_characteristics(self, timestamp: pd.Timestamp) -> pd.Series:
        """
        Return asset/cash values, net wealth, exposure, and leverage.
        """
        hdbg.dassert_in(
            timestamp,
            self._holdings.index,
            "No record of holdings at timestamp=`%s`",
            timestamp,
        )
        # Mark the current holdings to market.
        holdings = self.get_holdings(timestamp, asset_id=None, exclude_cash=True)
        holdings = self.mark_to_market(timestamp, holdings)
        # Compute value of holdings.
        holdings_net_value = holdings["value"].sum()
        hdbg.dassert(
            np.isfinite(holdings_net_value),
            "holdings_net_value=%s",
            holdings_net_value,
        )
        # Get the cash available.
        cash = self._get_holdings_as_scalar(timestamp, asset_id=self.CASH_ID)
        hdbg.dassert(np.isfinite(cash), "cash=%s", cash)
        # Cash should always be positive.
        hdbg.dassert_lte(0.0, cash, "cash=%s", cash)
        # Compute the net wealth (AKA "total value" AKA "NAV").
        net_wealth = holdings_net_value + cash
        hdbg.dassert(np.isfinite(net_wealth), "net_value=%s", net_wealth)
        # Compute the gross exposure.
        gross_exposure = holdings["value"].abs().sum()
        # Compute the portfolio leverage.
        leverage = gross_exposure / net_wealth
        dict_ = {
            "net_asset_holdings": holdings_net_value,
            "cash": cash,
            "net_wealth": net_wealth,
            "gross_exposure": gross_exposure,
            "leverage": leverage,
        }
        characteristics = pd.Series(dict_, name=timestamp)
        return characteristics

    def advance_portfolio_state(
        self,
        timestamp: pd.Timestamp,
        order_df: Optional[pd.DataFrame],
    ) -> None:
        """
        Update holdings using `order_df`.

        :param timestamp:
        :param order_df:
        :return:
        """
        _LOG.debug(
            "\n%s",
            hprint.frame(
                "advance_portfolio_state: timestamp=%s" % timestamp, char1="<"
            ),
        )
        # TODO(gp): Check that orders are all for different asset_ids.
        # TODO: Ensure that we are moving forward in time.
        hdbg.dassert_isinstance(timestamp, pd.Timestamp)
        hdbg.dassert(timestamp.tz is not None)
        hdbg.dassert_eq(timestamp.tz.zone, self._holdings.index.dtype.tz.zone)
        # Check that latest holdings are timestamped prior to `timestamp`.
        last_timestamp = self.get_last_timestamp()
        hdbg.dassert_lte(last_timestamp, timestamp)
        # Log portfolio characteristics at `last_timestamp` if not done already.
        # This could happen if there is a bar with no orders (or overnight).
        if last_timestamp not in self._characteristics:
            _LOG.debug(
                "Computing portfolio characteristics for timestamp=`%s`",
                last_timestamp,
            )
            val = self.get_characteristics(last_timestamp)
            self._characteristics[last_timestamp] = val
        # Get latest holdings
        last_holdings = self.get_holdings(last_timestamp, asset_id=None)
        last_holdings.index.name = "last_timestamp"
        last_holdings.reset_index(inplace=True)
        last_holdings_srs = last_holdings.set_index("asset_id")["curr_num_shares"]
        new_holdings_srs = last_holdings_srs.copy()
        if order_df is not None:
            Portfolio._validate_order_df(order_df)
            hdbg.dassert_lte(last_timestamp, order_df["start_timestamp"].min())
            hdbg.dassert_lte(order_df["end_timestamp"].max(), timestamp)
            holdings_diff = order_df.set_index("asset_id")["num_shares_filled"]
            cash_diff = (
                -1
                * (
                    order_df["execution_price"] * order_df["num_shares_filled"]
                ).sum()
            )
            hdbg.dassert(np.isfinite(cash_diff))
            new_holdings_srs = new_holdings_srs.add(holdings_diff, fill_value=0)
            new_holdings_srs.loc[Portfolio.CASH_ID] += cash_diff
        new_holdings_srs.name = "curr_num_shares"
        new_holdings = new_holdings_srs.to_frame().reset_index()
        new_holdings.index = [timestamp] * len(new_holdings)
        new_holdings = new_holdings.convert_dtypes()
        Portfolio._validate_holdings_df(new_holdings)
        # Add the information to the holdings.
        updated_state = pd.concat([new_holdings, self._holdings])
        updated_state = updated_state.convert_dtypes()
        self._holdings = updated_state
        # Log portfolio characteristics at `last_timestamp` if not done already.
        _LOG.debug(
            "Computing portfolio characteristics for timestamp=`%s`", timestamp
        )
        val = self.get_characteristics(timestamp)
        self._characteristics[timestamp] = val

    def get_pnl(self):
        """
        Return the time series of positions, PnL.
        """
        # strategyid                                  SAU1
        # tradedate                             2021-10-28
        # account                                SAU1_CAND
        # id                                         10005
        # cost_basis                                   0.0  ?
        # mark                                  795.946716  ?
        # avg_buy_px                                   0.0
        # avg_sell_px                                  0.0
        # qty_bought                                     0
        # qty_sold                                       0
        # quantity                                       0
        # position_pnl                                 0.0
        # trading_pnl                                  0.0
        # total_pnl                                    0.0
        # local_currency                               USD
        # portfolio_currency                           USD
        # fx                                           1.0
        # to_usd                                       1.0
        # timestamp_db          2021-10-28 20:40:05.517649
        return

    def get_bod_holdings(
        self, timestamp: pd.Timestamp, id_: Optional[Any]
    ) -> pd.DataFrame:
        """
        Return the holdings at the beginning of a day.
        """
        # tradedate       2021-10-28
        # id                   10005
        # strategyid            SAU1
        # account          SAU1_CAND
        # bod_position             0
        # bod_price              0.0
        # currency               USD
        # fx                     1.0
        # TODO(gp): Just implement from get_current_holdings.
        return

    def _get_holdings_as_scalar(
        self,
        timestamp: pd.Timestamp,
        asset_id: int,
    ) -> float:
        """
        Return the number of shares held for a given asset.

        For cash, this is the same as the cash value.
        """
        holdings = self.get_holdings(timestamp, asset_id=asset_id)
        _LOG.debug("holdings=\n%s", holdings)
        if holdings.empty:
            value = 0
        else:
            value = holdings["curr_num_shares"].values[0]
        _LOG.debug("value=%s for asset_id=%s", value, asset_id)
        return value

    @staticmethod
    def _get_holdings(
        holdings_df,
        timestamp: pd.Timestamp,
        asset_id: Optional[Any],
        *,
        exclude_cash: bool = False,
    ) -> pd.DataFrame:
        Portfolio._validate_holdings_df(holdings_df)
        _LOG.debug(hprint.to_str("timestamp asset_id exclude_cash"))
        # _LOG.debug("holdings=\n%s", hprint.dataframe_to_str(holdings_df))
        if timestamp not in holdings_df.index:
            _LOG.debug("Timestamp=`%s` not found in holdings index", timestamp)
            empty_holdings = pd.DataFrame(
                [], index=[], columns=Portfolio.HOLDINGS_COLS
            )
            return empty_holdings
        holdings = holdings_df.loc[timestamp]
        if isinstance(holdings, pd.Series):
            holdings = holdings.to_frame().T
        # _LOG.debug("holdings=%s\n%s", str(holdings.shape), holdings)
        hdbg.dassert(
            not holdings.empty,
            "Timestamp=`%s` found in index but filtered holdings are empty!",
            timestamp,
        )
        hdbg.dassert_no_duplicates(holdings["asset_id"])
        # Filter by asset_id, if needed.
        if asset_id is not None:
            _LOG.debug("Filtering by asset_id: holdings=\n%s", holdings)
            mask = holdings["asset_id"] == asset_id
            # _LOG.debug("mask=\n%s", mask)
            holdings = holdings[mask]
            if holdings.empty:
                _LOG.debug(
                    "No holdings available at timestamp=`%s` for asset_id=`%s`",
                    timestamp,
                    asset_id,
                )
        # Exclude cash, if needed.
        if exclude_cash:
            # _LOG.debug("exclude_cash: holdings=\n%s", holdings)
            hdbg.dassert_ne(
                asset_id,
                Portfolio.CASH_ID,
                "You cannot request cash (asset_id=`%s`) and simultaneously request to exclude it",
                asset_id,
            )
            mask = holdings["asset_id"] != Portfolio.CASH_ID
            # _LOG.debug("mask=\n%s", mask)
            holdings = holdings[mask]
            if holdings.empty:
                _LOG.debug(
                    "No non-cash holdings available at timestamp=`%s`", timestamp
                )
        return holdings

    @staticmethod
    def _validate_mark_to_market_df(
        df: pd.DataFrame,
    ) -> None:
        """
        Ensure that `df` passes basic sanity checks for `mark_to_market()`.
        """
        # The input should be a dataframe satisfying the following column
        # constraints.
        hdbg.dassert_isinstance(df, pd.DataFrame)
        hdbg.dassert_is_subset(Portfolio.HOLDINGS_COLS, df.columns)
        hdbg.dassert_not_in("price", df.columns)
        hdbg.dassert_not_in("value", df.columns)
        # The columns should be of the correct types. Skip if the dataframe is
        # empty (since the correct types are not inferred in that case).
        if not df.empty:
            hdbg.dassert_eq(
                df["asset_id"].dtype.type,
                np.int64,
                "The column `asset_id` should only contain integer ids.",
            )
            hdbg.dassert_eq(
                df["curr_num_shares"].dtype.type,
                np.float64,
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
    def _validate_holdings_df(
        holdings_df: pd.DataFrame,
    ) -> None:
        """
        Ensure that `holdings_df` passes basic sanity checks.
        """
        # The input should be a nonempty dataframe with a datetime index.
        hdbg.dassert_isinstance(holdings_df, pd.DataFrame)
        hdbg.dassert_isinstance(holdings_df.index, pd.DatetimeIndex)
        hdbg.dassert(hasattr(holdings_df.index.dtype, "tz"))
        hdbg.dassert(not holdings_df.empty, "The dataframe must be nonempty.")
        # The dataframe must have the correct columns.
        hdbg.dassert_eq_all(
            holdings_df.columns.to_list(),
            Portfolio.HOLDINGS_COLS,
            "Columns do not conform to requirements.",
        )
        # The columns should be of the correct types.
        hdbg.dassert_eq(
            holdings_df["asset_id"].dtype.type,
            np.int64,
            "The column `asset_id` should only contain integer ids.",
        )
        hdbg.dassert_eq(
            holdings_df["curr_num_shares"].dtype.type,
            np.float64,
            "The column `curr_num_shares` should be a float column.",
        )
        # The dataframe must contain a row for cash.
        hdbg.dassert_in(
            Portfolio.CASH_ID,
            holdings_df["asset_id"].to_list(),
            "No cash holdings available.",
        )
        # All share values should be finite.
        hdbg.dassert(
            np.isfinite(holdings_df["curr_num_shares"]).all(),
            "All share values must be finite.",
        )

    @staticmethod
    def _validate_initial_holdings_df(holdings_df: pd.DataFrame) -> None:
        """
        Ensure that `holdings_df` qualifies as an initial holdings df.

        The dataframe should specify asset holdings (in shares) at a
        single point in time. Cash must be included and be nonnegative.
        """
        Portfolio._validate_holdings_df(holdings_df)
        # Initialization should be at a single point in time.
        hdbg.dassert_eq(
            holdings_df.index.nunique(),
            1,
            "Encountered `%s` unique index values in initial holdings dataframe",
            holdings_df.index.nunique(),
        )
        # At initialization there should be no more than one row per asset.
        hdbg.dassert_no_duplicates(holdings_df["asset_id"].to_list())
        #
        initialization_timestamp = holdings_df.index[0]
        cash_holdings = Portfolio._get_holdings(
            holdings_df,
            initialization_timestamp,
            Portfolio.CASH_ID,
            exclude_cash=False,
        )
        # Initial cash must be nonnegative.
        cash = cash_holdings["curr_num_shares"].values[0]
        hdbg.dassert_lte(0, cash)

    @staticmethod
    def _validate_order_df(
        order_df: pd.DataFrame,
    ) -> None:
        """
        Ensure that `holdings_df` passes basic sanity checks.
        """
        # The input should be a nonempty dataframe.
        hdbg.dassert_isinstance(order_df, pd.DataFrame)
        hdbg.dassert(not order_df.empty, "The dataframe must be nonempty.")
        # The dataframe must have the correct columns.
        hdbg.dassert_is_subset(
            Portfolio.ORDER_COLS,
            order_df.columns.to_list(),
            "Columns do not conform to requirements.",
        )
        # The columns should be of the correct types.
        hdbg.dassert_eq(
            order_df["asset_id"].dtype.type,
            np.int64,
            "The column `asset_id` should only contain integer ids.",
        )
        hdbg.dassert_eq(
            order_df["num_shares_filled"].dtype.type,
            np.float64,
            "The column `curr_num_shares` should be a float column.",
        )
        #
        hdbg.dassert_eq(order_df["start_timestamp"].dtype.type, pd.Timestamp)
        hdbg.dassert_eq(order_df["end_timestamp"].dtype.type, pd.Timestamp)
        hdbg.dassert(hasattr(order_df["start_timestamp"].dtype, "tz"))
        hdbg.dassert(hasattr(order_df["end_timestamp"].dtype, "tz"))
        # The dataframe must not contain a row for cash.
        hdbg.dassert_not_in(
            Portfolio.CASH_ID,
            order_df["asset_id"].to_list(),
            "Order for cash detected.",
        )
        # There should be no more than one row per asset.
        hdbg.dassert_no_duplicates(order_df["asset_id"].to_list())
        # All share values should be finite.
        hdbg.dassert(
            np.isfinite(order_df["num_shares_filled"]).all(),
            "All share values must be finite.",
        )
