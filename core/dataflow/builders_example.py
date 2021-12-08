"""
Import as:

import core.dataflow.builders_example as cdtfbuexa
"""

import datetime
import logging

import core.config as cconfig
import core.dataflow.builders as cdtfbuil
import core.dataflow.core as cdtfcore
import core.dataflow.nodes.sources as cdtfnosou
import core.dataflow.nodes.transformers as cdtfnotra
import core.dataflow.nodes.volatility_models as cdtfnovomo
import core.finance as cofinanc

_LOG = logging.getLogger(__name__)


class ArmaReturnsBuilder(cdtfbuil.DagBuilder):
    """
    Pipeline for generating filtered returns from an ARMA process.
    """

    def get_config_template(self) -> cconfig.Config:
        """
        Return a reference configuration.

        :return: reference config
        """
        config = cconfig.get_config_from_nested_dict(
            {
                # Load prices.
                self._get_nid("rets/read_data"): {
                    "frequency": "T",
                    "start_date": "2010-01-04 09:00:00",
                    "end_date": "2010-01-04 16:30:00",
                    "ar_coeffs": [0],
                    "ma_coeffs": [0],
                    "scale": 0.1,
                    "burnin": 0,
                    "seed": 0,
                },
                # Filter ATH.
                self._get_nid("rets/filter_ath"): {
                    "col_mode": "replace_all",
                    "transformer_kwargs": {
                        "start_time": datetime.time(9, 30),
                        "end_time": datetime.time(16, 00),
                    },
                },
                # Resample returns.
                self._get_nid("rets/resample"): {
                    "rule": "1T",
                    "price_cols": ["close"],
                    "volume_cols": ["volume"],
                },
                # Compute TWAP and VWAP.
                self._get_nid("rets/compute_wap"): {
                    "rule": "5T",
                    "price_col": "close",
                    "volume_col": "volume",
                },
                # Calculate rets.
                self._get_nid("rets/compute_ret_0"): {
                    "cols": ["twap", "vwap"],
                    "col_mode": "merge_all",
                    "transformer_kwargs": {
                        "mode": "pct_change",
                    },
                },
                # Model volatility.
                self._get_nid("rets/model_volatility"): {
                    "cols": ["vwap_ret_0"],
                    "steps_ahead": 2,
                    "nan_mode": "leave_unchanged",
                },
                # Clip rets.
                self._get_nid("rets/clip"): {
                    "cols": ["vwap_ret_0_vol_adj"],
                    "col_mode": "replace_selected",
                },
            }
        )
        return config

    def _get_dag(
        self, config: cconfig.Config, mode: str = "strict"
    ) -> cdtfcore.DAG:
        """
        Generate pipeline DAG.

        :param config: config object used to configure DAG
        :param mode: "strict" (e.g., for production) or "loose" (e.g., for
            interactive jupyter notebooks)
        :return: initialized DAG
        """
        dag = cdtfcore.DAG(mode=mode)
        _LOG.debug("%s", config)
        # Read data.
        stage = "rets/read_data"
        nid = self._get_nid(stage)
        node = cdtfnosou.ArmaGenerator(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, None, node)
        # Set weekends to Nan.
        stage = "rets/filter_weekends"
        nid = self._get_nid(stage)
        node = cdtfnotra.ColumnTransformer(
            nid,
            transformer_func=cofinanc.set_weekends_to_nan,
            col_mode="replace_all",
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Set non-ATH to NaN.
        stage = "rets/filter_ath"
        nid = self._get_nid(stage)
        node = cdtfnotra.ColumnTransformer(
            nid,
            transformer_func=cofinanc.set_non_ath_to_nan,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Resample.
        stage = "rets/resample"
        nid = self._get_nid(stage)
        node = cdtfnotra.TimeBarResampler(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, tail_nid, node)
        # Compute TWAP and VWAP.
        stage = "rets/compute_wap"
        nid = self._get_nid(stage)
        node = cdtfnotra.TwapVwapComputer(
            nid,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Compute returns.
        stage = "rets/compute_ret_0"
        nid = self._get_nid(stage)
        node = cdtfnotra.ColumnTransformer(
            nid,
            transformer_func=cofinanc.compute_ret_0,
            col_rename_func=lambda x: x + "_ret_0",
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Model volatility.
        stage = "rets/model_volatility"
        nid = self._get_nid(stage)
        node = cdtfnovomo.VolatilityModel(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, tail_nid, node)
        # Clip rets.
        stage = "rets/clip"
        nid = self._get_nid(stage)
        node = cdtfnotra.ColumnTransformer(
            nid,
            transformer_func=lambda x: x.clip(lower=-3, upper=3),
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        _ = tail_nid
        return dag
