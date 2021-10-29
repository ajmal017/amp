"""
Import as:

import core.dataflow.nodes.sinks as cdtfnosin
"""

import collections
import logging
import os
from typing import Dict

import pandas as pd

import core.dataflow.core as cdtfcor
import core.dataflow.nodes.base as cdtfnb
import core.dataflow.utils as cdtfuti
import core.finance as cfin
import helpers.dbg as hdbg
import helpers.io_ as hio
import helpers.hparquet as hparquet

_LOG = logging.getLogger(__name__)


class WriteDf(cdtfnb.FitPredictNode):
    def __init__(
        self,
        nid: cdtfcor.NodeId,
        dir_name: str,
    ) -> None:
        super().__init__(nid)
        hdbg.dassert_isinstance(dir_name, str)
        self._dir_name = dir_name

    def fit(self, df_in) -> Dict[str, pd.DataFrame]:
        return self._write_df(df_in, fit=True)

    def predict(self, df_in) -> Dict[str, pd.DataFrame]:
        return self._write_df(df_in, fit=False)

    def _write_df(self, df, fit: True) -> Dict[str, pd.DataFrame]:
        if self._dir_name:
            hdbg.dassert_lt(1, df.index.size)
            # Create the directory if it does not already exist.
            hio.create_dir(self._dir_name, incremental=True)
            if isinstance(df.index, pd.DatetimeIndex):
                # NOTE: If needed, we can pass in only the last two elements.
                epochs = cfin.compute_epoch(df)
                epoch = epochs.iloc[-1].values[0]
            else:
                raise NotImplemented
            # Get the latest `df` index value.
            #
            file_name = f"{epoch}.pq"
            file_name = os.path.join(self._dir_name, file_name)
            hdbg.dassert_not_exists(file_name)
            # Write the file.
            # TODO(Paul): Maybe allow the node to configure the log level.
            hparquet.to_parquet(df, file_name, log_level=logging.DEBUG)
        # Collect info.
        info = collections.OrderedDict()
        info["df_out_info"] = cdtfuti.get_df_info_as_string(df)
        mode = "fit" if fit else "predict"
        self._set_info(mode, info)
        # Pass the dataframe though.
        return {"df_out": df}


#
def read_data(dir_name) -> Dict[str, pd.DataFrame]:
    # Glob.
    # Read all the stuff in a format.
    ...
    # if validate:
    # P1, Check some invariants: they are always increasing.
