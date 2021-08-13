<!--ts-->
   * [Real-time dataflow execution](#real-time-dataflow-execution)



<!--te-->
# Real-time dataflow execution

- We have several semantics for "real-time" depending if it's simulated or real,
  and if it's current wall-clock time or replayed wall-clock time
  - This is used to achieve different levels of accuracy and speed when testing
    real-time systems
  - See `core/dataflow/real_time.py` for detailed definitions

- As usual, we use an approach where there are multiple layers exposing more and
  more complex functionalities
  - `helpers/hdatetime.py`: contains some functions for getting wall-clock time
    and handling timestamps with time zone
  - `helpers/hasyncio.py`: contains adapter to run `asyncio` together with
    `async_solipsism` for achieving a simulated real-time semantic
  - `core/dataflow/real_time.py`: general-purpose functions for real-time
    execution, e.g,.
    - `ReplayedTime`: for replaying time in real-time for testing purposes
    - `execute_with_real_time_loop`: real-time loop based on `asyncio`
    - `get_data_as_of_datetime`: to extract real-time view of data based on
      as-of timestamps
  - `core/dataflow/nodes/sources.py`: dataflow nodes with a real-time semantic
    for production and testing, e.g.,
    - `SimulatedTimeDataSource`, `RealTimeDataSource`, `ReplayedTimeDataSource`
    - Nodes for specific data source can be built customizing real-time nodes by
      injecting functions inside or inheriting from them
  - `core/dataflow/runners.py`: a DagRunner based on `asyncio` that runs DAGs
    with real-time semantic

- To run a real-time DAG, one needs to:
  - Build a DAG whose data source is a real-time node
    - E.g., inject a `RealTimeDataSource` inside `ReturnsPipeline`
  - Create a configuration for the real-time loop (e.g., the period of time to
    sleep between DAG executions)
  - Run the DAG through the `RealTimeDagRunner`
