"""
Import as:

import oms.mocked_portfolio as omocport
"""

# class MockedPortfolio(AbstractPortfolio):
class MockedPortfolio:
    """
    Implement an object that mocks a real OMS / portfolio backed by a DB where
    updates to the state representing portfolio holdings are asynchronous.

    The DB contains the following tables:
    - `positions`
        - current_position
        - open_quantity
    """

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
        #
        db_connection_info: Any,
        event_loop: Any,
        get_wall_clock: Any,
    ):
        """
        Same interface as Portfolio but no holdings_df.

        Instead a pointer to the OMS DB.
        """
        # INV: the DB contains holdings and orders in the same format that the
        # OMS would do.

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
        # Can't pass this since the time is kept by the external clock.
        # initial_timestamp: pd.Timestamp,
    ) -> "Portfolio":
        pass
