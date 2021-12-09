"""
Import as:

import oms.broker_example as obroexam
"""

import asyncio

import core.dataflow.price_interface_example as cdtfprinex
import oms.broker as ombroker


def get_broker_example1(event_loop: asyncio.AbstractEventLoop) -> ombroker.Broker:
    # Build the price interface.
    (
        price_interface,
        get_wall_clock_time,
    ) = cdtfprinex.get_replayed_time_price_interface_example2(event_loop)
    # Build the broker.
    broker = ombroker.Broker(price_interface, get_wall_clock_time)
    # TODO(gp): Instead of returning all the objects we could return only `broker`
    #  and allow clients to extract the objects from inside, if needed.
    return broker
