import logging

import core.dataflow.builders_example as cdtfbuexa
import core.dataflow.runners as cdtfrunn
import helpers.printing as hprint
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


# #############################################################################


class TestArmaReturnsBuilder(hunitest.TestCase):
    """
    Test the ArmaReturnsBuilder pipeline.
    """

    def test1(self) -> None:
        dag_builder = cdtfbuexa.ArmaReturnsBuilder()
        config = dag_builder.get_config_template()
        dag_runner = cdtfrunn.FitPredictDagRunner(config, dag_builder)
        result_bundle = dag_runner.fit()
        df_out = result_bundle.result_df
        str_output = (
            f"{hprint.frame('config')}\n{config}\n"
            f"{hprint.frame('df_out')}\n{hunitest.convert_df_to_string(df_out, index=True)}\n"
        )
        self.check_string(str_output)
