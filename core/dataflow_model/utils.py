"""
Import as:

import core.dataflow_model.utils as cdtfut
"""

def add_experiment_arg(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    """
    Add common command line options to run the experiments.
    """
    parser.add_argument(
        "--dst_dir",
        action="store",
        required=True,
        help="Directory storing the results",
    )
    parser.add_argument(
        "--no_incremental",
        action="store_true",
        help="Delete the dir before running or skip experiments already performed",
    )
    parser.add_argument(
        "--function",
        action="store",
        required=True,
        help="Full invocation of Python function to create configs, e.g., "
             "`nlp.build_configs.build_PTask1297_configs(random_seed_variants="
             "[911,2,42,0])`",
    )
    parser.add_argument(
        "--skip_on_error",
        action="store_true",
        help="Continue execution of experiments after encountering an error",
    )
    parser.add_argument(
        "--index",
        action="store",
        help="Run a single experiment corresponding to the i-th config",
    )
    parser.add_argument(
        "--start_from_index",
        action="store",
        help="Run experiments starting from a specified index",
    )
    parser.add_argument(
        "--only_print_configs",
        action="store_true",
        help="Print the configs and exit",
    )
    # parser.add_argument(
    #     "--dry_run",
    #     action="store_true",
    #     help="Run a short experiment to sanity check the flow",
    # )
    parser.add_argument(
        "--num_attempts",
        default=1,
        type=int,
        help="Repeat running the experiment up to `num_attempts` times",
        required=False,
    )
    parser.add_argument(
        "--num_threads",
        action="store",
        help="Number of threads to use (-1 to use all CPUs)",
        required=True,
    )


def setup_experiment(config, dst_dir, i):
    """
    Set up the directory and the book-keeping artifacts for the experiment running
    `config`.
    """
    dbg.dassert_isinstance(config, cfg.Config)
    # TODO(gp): Can we just create instead of asserting?
    dbg.dassert_dir_exists(dst_dir)

    # Create subdirectory structure for experiment results.
    result_subdir = "result_%s" % i
    experiment_result_dir = os.path.join(dst_dir, result_subdir)
    _LOG.info("experiment_result_dir=%s", experiment_result_dir)
    # If there is already a success file in the dir, skip the experiment.
    file_name = os.path.join(experiment_result_dir, "success.txt")
    if os.path.exists(file_name):
        _LOG.warning("Found file '%s': skipping run %d", file_name, i)
        return
    io_.create_dir(experiment_result_dir, incremental=True)

    # Inject the experiment result dir inside the config.
    # TODO(gp): This operation is also performed on the notebook side
    #  in `get_config_from_env()`. Find a better way to achieve this.
    config = cfgb.set_experiment_result_dir(experiment_result_dir,
                                            config)
    # Prepare book-keeping files.
    file_name = os.path.join(experiment_result_dir, "config.pkl")
    _LOG.info("file_name=%s", file_name)
    hpickle.to_pickle(config, file_name)
    #
    file_name = os.path.join(experiment_result_dir, "config.txt")
    _LOG.info("file_name=%s", file_name)
    io_.to_file(file_name, str(config))
    #
    file_name = os.path.join(experiment_result_dir, "config_builder.txt")
    _LOG.info("file_name=%s", file_name)
    io_.to_file(
        file_name,
        "Config builder: %s\nConfig index: %s" % (config_builder, str(i)),
        )
