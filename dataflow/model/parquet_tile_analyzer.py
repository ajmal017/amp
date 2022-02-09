import collections
import logging
import os
from typing import Tuple

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hintrospection as hintros

_LOG = logging.getLogger(__name__)


class ParquetTileAnalyzer:
    """
    A tool for analyzing parquet file metadata.
    """

    @staticmethod
    def collate_parquet_tile_metadata(
        path: str,
    ) -> pd.DataFrame:
        hdbg.dassert(os.path.isdir(path))
        if path.endswith("/"):
            path = path[:-1]
        dict_ = collections.OrderedDict()
        start_depth = len(path.split("/"))
        walk = os.walk(path)
        headers_set = set()
        for triple in walk:
            lhs, rhs = ParquetTileAnalyzer._process_walk_triple(
                triple, start_depth
            )
            hdbg.dassert_eq(len(lhs), len(rhs))
            if not lhs:
                continue
            headers_set.add(lhs)
            file_name = os.path.join(triple[0], triple[2][0])
            size_in_bytes = os.path.getsize(file_name)
            dict_[rhs] = size_in_bytes
        hdbg.dassert_eq(len(headers_set), 1)
        # Convert to a multiindexed dataframe.
        df = pd.DataFrame(
            dict_.values(),
            index=dict_.keys()
        )
        df.rename(columns={0: "file_size_in_bytes"}, inplace=True)
        headers = headers_set.pop()
        df.index.names = headers
        df.sort_index(inplace=True)
        file_size = df["file_size_in_bytes"].apply(hintros.format_size)
        df["file_size"] = file_size
        return df

    @staticmethod
    def compute_metadata_stats_by_asset_id(
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        srs = df["file_size_in_bytes"]
        hdbg.dassert_isinstance(srs, pd.Series)
        n_years = srs.groupby(level=[0, 1]).count().groupby(level=0).count()
        n_unique_months = srs.groupby(level=[0, 2]).count().groupby(level=0).count()
        n_files = srs.groupby(level=0).count()
        size_in_bytes = srs.groupby(level=0).sum()
        size = size_in_bytes.apply(hintros.format_size)
        stats_df = pd.DataFrame({
            "n_years": n_years,
            "n_unique_months": n_unique_months,
            "n_files": n_files,
            "size": size,
        })
        return stats_df

    @staticmethod
    def compute_universe_size_by_time(
        df: pd.DataFrame,
    ):
        srs = df["file_size_in_bytes"]
        hdbg.dassert_isinstance(srs, pd.Series)
        n_asset_ids = srs.groupby(level=[1, 2]).count()
        size_in_bytes = srs.groupby(level=[1, 2]).sum()
        size = size_in_bytes.apply(hintros.format_size)
        stats_df = pd.DataFrame({
            "n_asset_ids": n_asset_ids,
            "size": size,
        })
        return stats_df

    @staticmethod
    def _process_walk_triple(triple: tuple, start_depth) -> Tuple[Tuple[str], Tuple[int]]:
        lhs_vals = []
        rhs_vals = []
        # If there are subdirectories, do not process.
        if triple[1]:
            return tuple(lhs_vals), tuple(rhs_vals)
        depth = len(triple[0].split("/"))
        rel_depth = depth - start_depth
        key = tuple(triple[0].split("/")[start_depth:])
        if len(key) == 0:
            return tuple(lhs_vals), tuple(rhs_vals)
        hdbg.dassert_eq(len(key), rel_depth)
        lhs_vals = []
        rhs_vals = []
        for string in key:
            lhs, rhs = string.split("=")
            lhs_vals.append(lhs)
            rhs_vals.append(int(rhs))
        return tuple(lhs_vals), tuple(rhs_vals)