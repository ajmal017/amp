## run_pipeline.py

- Separation of responsibilities
    - `run_pipeline.py`
      - run all the experiments
      - save config for each experiment
      - parallelize the experiments
      - implement the retry logic
    
    - `run_pipeline_stub.py`
      - runs a single experiment
      - It has the same interface as a notebook
        - pipeline_builder
        - config_builder
        - index
        - dst_dir
    
- The invariant is that 

- There are two interfaces to materialize configs
    - One on the command line side
    
    - One on the run_pipeline, notebook side
        - The params to reconstruct the configs 
          are passed through env vars or params of the
          script
    
```
> run_pipeline.py \
  --dst_dir experiment1 \
  --pipeline ./core/dataflow_model/notebooks/Master_pipeline_runner.py \
  --function "dataflow_lemonade.RH1E.task89_config_builder.build_15min_ar_model_configs()" \
  --num_threads 2
```