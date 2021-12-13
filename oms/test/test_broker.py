import logging

import pytest

import helpers.unit_test as hunitest
import oms.broker_example as obroexam
import oms.order_example as oordexam
import oms.test.test_oms_db as ottb

_LOG = logging.getLogger(__name__)


class TestSimulatedBroker1(hunitest.TestCase):
    def test1(self) -> None:
        """
        Submit orders to a SimulatedBroker.
        """
        # Build a SimulatedBroker.
        event_loop = None
        broker = obroexam.get_simulated_broker_example1(event_loop)
        # Submit an order.
        order = oordexam.get_order_example1()
        orders = [order]
        broker.submit_orders(orders)
        # Check fills.
        # TODO(gp): Implement this.


# TODO(gp): Finish this.
@pytest.mark.skip(reason="Need to be finished")
class TestMockedBroker1(ottb.TestOmsDbHelper):
    def test1(self) -> None:
        """
        Test submitting orders to a MockedBroker.
        """
        event_loop = None
        broker = obroexam.get_mocked_broker_example1(event_loop, self.connection)
        #
        order = oordexam.get_order_example1()
        orders = [order]
        broker.submit_orders(orders)
        # Check fills.
        # TODO(gp): Implement this.
