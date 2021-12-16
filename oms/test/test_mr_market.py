import asyncio

import helpers.hasyncio as hasynci
import oms.broker_example as obroexam
import oms.mr_market as omrmark
import oms.oms_db as oomsdb
import oms.order_example as oordexam
import oms.test.oms_db_helper as omtodh


class TestMrMarketOrderProcessor1(omtodh.TestOmsDbHelper):
    """
    Test operations on the submitted orders table.
    """

    def test1(self) -> None:
        """
        Test creating the table.
        """
        # Create OMS tables.
        oomsdb.create_oms_tables(self.connection, incremental=False)

        #
        with hasynci.solipsism_context() as event_loop:
            # Build MockedBroker.
            broker = obroexam.get_mocked_broker_example1(
                event_loop, self.connection
            )
            #
            async_broker = self._broker_thread(event_loop, broker)
            # Build OrderProcessor.
            get_wall_clock_time = broker.market_data_interface.get_wall_clock_time
            poll_kwargs = hasynci.get_poll_kwargs(get_wall_clock_time)
            delay_to_accept_in_secs = 3
            delay_to_fill_in_secs = 10
            order_processor = omrmark.order_processor(
                self.connection,
                poll_kwargs,
                delay_to_accept_in_secs,
                delay_to_fill_in_secs,
                broker,
            )
            #
            coroutines = [order_processor, async_broker]
            hasynci.run(asyncio.gather(*coroutines), event_loop=event_loop)

    async def _order_processor_thread(
        self,
        event_loop: asyncio.AbstractEventLoop,
        broker,
    ) -> None:
        # Kick off the OrderProcessor.
        # Wait.
        pass

    async def _broker_thread(
        self,
        event_loop: asyncio.AbstractEventLoop,
        broker,
    ) -> None:
        _ = event_loop
        await asyncio.sleep(1)
        # Create an order.
        order = oordexam.get_order_example1()
        # Submit the order to the broker.
        await broker.submit_orders([order])
