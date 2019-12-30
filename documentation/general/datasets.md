## Where to save files for general consumption

- In general we prefer to separate data that is:
    -   used only by one person: you can keep it in your _wd dir or in /scratch
    -   used by multiple people: you should move it to /data

When you change shared data:

-   Send an email to the team broadcasting the update
-   Try to keep things back compatible (e.g., make a copy of the old file adding a timestamp, instead of overwriting it)

Ideally we would like to have accessors that abstract an interface, so that we can make changes without client code being affected

-   E.g., instead of having in all notebooks pd.read_hd5(‘/data/xyz_data/…’), one can implement a function with a (default) parameter pointing to the file

If you see that your code that used to work doesn’t work anymore, check the git log on that file or related files to see if somebody made a change but did not propagate it everywhere.

## Releasing data sets


### Guidelines for official datasets

-   Datasets are versioned
    -   E.g.,` v0.3` (not` v 0.3`, not` v_0.3`, not `V0.3`)
-   Dataset reader
    -   Each version has a reader in the Data Encyclopedia, which should be the only way to access the data
    -   It is ok to add a version field to the reader to make it back compatible
-   Dataset file needs to:
    -   be saved in the proper directory, which should be modeled after the code base
        -   e.g.,` edg/form_8, edg/form_10, edg/timestamp`
    -   have a reference to the task that was used to generate it
    -   have information about their version
    -   have reference to the interval of time used
        -   in general we like [a, b) intervals
    -   have an extension of the format (even if they are directory)
        -   e.g.,` .h5` (not` .hdf5`), `.pkl` (not` .pickle`),` .pq` (not` .parquet`)
    -   Separate chunks of information with _ and not -
        -   The char - is not Linux friendly and should be avoided when possible
    -   E.g., `/scratch/edg/timestamp/Task777_timestamp_v0.7_20180101_20181201.pq`
-   Each dataset has a gdoc that explains:
    -   where the raw data is
    -   how it was generated (e.g., exact command line, options, an indication of how long it took, commit hash)
    -   a changelog with the changes with respect to the previous version
    -   a description of the fields
    -   ...
    -   See Data Manual (aka DM)
-   Generating a dataset needs to be simple and automatic
    -   E.g., it’s not ok to run too many scripts, notebooks, reading and saving the data, and so on
    -   Ideally we want something that is “fire-and-forget”, i.e., we start a script and the data generation, checks and so on, are run without any human intervention


### Patching a dataset



-   Sometimes we “patch up” a data set, reading the old version of the data, making some changes, and then saving it back
-   In these cases
    -   We still bump the revision probably a minor one (e.g., from `v0.3` to `v0.3.1`)
    -   We also need to follow the release process (e.g., update gdoc, reader in data encyclopedia)
    -   Also we need to make sure that the change is also applied to the official release flow, kick off the process of generating the dataset from scratch, and make sure that the dataset matches the patched version


### Lifecycle of a dataset task



-   Each data set should have an umbrella issue with a list of all the desired features for that specific dataset
    -   EDG: Wishlist for timestamp data set #529
    -   Ideally we want to put a query that finds all the bugs related to this umbrella issue, so that we are forced to use a regular naming scheme
    -   Any time we find a bug or we want to improve the dataset we add something to the wishlist
    -   The wishlist is used to prioritize the work
-   From the wishlist we create bugs, ideally one bug per feature as usual
    -   Once the work for the bug is done we re-run the entire pipeline to generate the data set
    -   Everytime we change the data set, even to fix a bug in the previous release, we should bump up the version
    -   We can use minor version e.g., v0.4.1 for bug fixes and minor improvements (e.g., fix the name of a column) and major revisions (v0.4 -> v0.5) for a new feature


### Expose a dataset reader

Provide library functions for reading the data creating a nice interface for the data (e.g., ravenpack/project_helpers/rp_sql_pickle.py)

Use a variable in the code to point to the official version of the data, that is passed to the reader function (which is better than using an evil default parameter)



-   Pros
-   we don’t have to change all the code when we release a new version of the data
-   old code can still still use an older version of the data by specifying a previous path

Add all the conversion functions that are needed so that we can add a level of indirection from the raw data (e.g., ET conversion, float to int conversion, renaming of columns…)

The reader also makes sure that the data is not broken.


### Reproducibility of any datasets

We should be able to regenerate any dataset from scratch in a deterministic way.

Often it’s a good idea to pre-compute some data structures, e.g., running a long SQL query, and then saving it for later use in a notebook.

We refer to these datasets as “unofficial” datasets, since they are meant only for internal consumption, 

and we not necessarily distribute to people outside our group

Cons:



-   these datasets end up being used by multiple people, with misunderstandings on how data was generated and under which assumptions
-   the data is filtered several times in contradiction with DRY (Don’t Repeat Yourself) principle, making difficult to understand how each data subset was generated
-   when it’s time to put it in production, nobody remembers how the data was generated


### Creating datasets



-   Shared datasets should be written from HEAD. Do not write shared datasets with code that is not checked in.
-   Datasets should have meaningful filenames with meaningful metadata (e.g., the data period that is covered in the usual “YYYYMMDD” format and a version number such as “v0.1”)
    -   E.g.,` TaskXYZ_edg_forms4_v0.1_20180923_20181201.pq`
-   Document how the data was generated
    -   You can refer to the bug in the name of the file
        -   E.g., `TaskXYZ_ravenpack100pct_v0.1_20180923`
        -   In this way one can go to the bug and retrieve all the information on used scripts and assumptions
    -   Otherwise a file with the same name and the extension “.txt” with the following data is helpful, like a README
        -   The command line (if applicable) used for generating the data
        -   The git commit at which you were synced when writing the data
        -   Pointers to a useful Task
-   Save the shared dataset under /data/xyz_data
-   Provide library functions for creating and saving the data
-   If you are processing data sets to generate other data sets, somehow document this in the name
    -   E.g., if you are using the file Task93_ravenpack100pct.pq, filtering it further using 
    -   Ids, add this information in the name Task204_ravenpack100pct_filteredByIds.pq
-   Even if the data is just for your own consumption, at least document something in the notebook code, like:

    ```
%%time

if False:
    # Generate the data.
    file_name = '/data/xyz_data/ravenpack/rp_filtered_numrows=None_sample=0.2.pkl'
    if "data" not in locals():
        data = get_event_table_df(file_name)
    else:
        _log.warning("Using cached %s", file_name)
    data.sort_values(by='timestamp_ET', inplace=True)
    data_ar = data[data['topic_group'] == 'analyst-ratings']
    data_ar.to_pickle('/scratch/julia/rp_filtered_numrows=None_sample=0.2-analyst_ratings.pkl')
else:
    # Read the cached data.
    file_name_ar = '/scratch/julia/rp_filtered_numrows=None_sample=0.2-analyst_ratings.pkl'
    if "data_ar" not in locals():
        data_ar = pd.read_pickle(file_name_ar)
    else:
        _log.warning("Using cached %s", file_name_ar)
```




### Suggested data formats

Best formats to store tabular data are, in order:



-   parquet
    -   [ParquetDataset](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html) are a good idea to keep things organized
    -   (although we are still learning the limitations with it)
    -   Please use “.pq” (not .pqt or .parquet as extensions)
-   h5 fixed format
    -   [+] super fast
    -   [-] can’t be sliced
-   h5 table format
    -   [+] can be sliced on created indices and columns
    -   [-] slower, larger footprint
-   pickle
    -   [-] not portable across versions of pandas, python, and potentially different OS

For unstructured data:



-   pickle
    -   [+] preserve python types 
    -   [+] more general
    -   [+] faster
    -   [-] incompatibility issues
-   json
    -   [+] easy to inspect (‘The Power of Plain Text’)

One can save the same data both as pickle and as json, using the same name and different suffixes (.pkl, .json), to allow for both computer and human consumption.


### Releasing a new dataset

When you change the data reader interface, or you create a new version of the data set, do a quick grep looking for code that relies on that data set



-   Fix the simple places (e.g., when it’s a function rename or adding a parameter)
-   File a bug for people indicating the code / notebooks that need to be fixed by the author
-   Try to do as much as possible to lower the burden for the team to transition to the new API


### Data encyclopedia guidelines

Each section should show:



1. Use a reader to read the data
    -   The wrapper should use a constant to point to the data location
    -   The wrapper does common transformations needed by each client
2. Show some basic stats about the data (e.g., timespan, number of tickers)
3. Print a snippet of the data
4. Map id to other representation and vice versa

Before each commit, do "Restart -> Restart & Run" to make sure nothing is broken (if so please file a bug)

## Architectural and design pattern


### Organize scripts as pipelines

One can organize complex computations in stages of a pipeline



-   E.g., to parse EDGAR forms
    -   download -> (raw data) -> header parser -> (pq data) -> XBLR / XML / XLS parser -> (pq data) -> custom transformation

One should be able to run the entire pipeline or just a piece



-   E.g., one can run the header parser from the raw data, save the result to file, then read this file back, and run the XBLR parser

Ideally one would always prefer to run the pipeline from scratch, but sometimes the stages are too expensive to compute over and over, so using chunks of the pipeline is better

This can also mixed with the “incremental mode”, so that if one stage has already been run and the intermediate data has been generated, that stage is skipped



-   Each stage can save files in a tmp_dir/stage_name

The code should be organized to allow these different modes of operations, but there is not always need to be super exhaustive in terms of command line options



-   E.g., I implement the various chunks of the pipeline in a library, separating functions that read / save data after a stage and then assemble the pieces into a throw-away script where I hardwire the file names and so on


### Incremental behavior



-   Often we need to run the same code over and over
    -   E.g., because the code fails on an unexpected point and then we need to re-run from the beginning
-   We use options like:

        ```
        --incremental
        --force
        --start_date
        --end_date
        --output_file
        ```


-   Check existence output file before start function (or a thread when using parallelism) which handle data of the corresponding period
    -   if `--incremental` is set and output file already exists then skip the computation and report
        -   log.info(“Skipping processing file %s as requested”, …)
    -   if `--incremental `is not set
        -   if output file exists then we issue a log.warn and abort the process
        -   if output file exists and param `--force`, then report a log.warn and rewrite output file


### Run end-to-end



-   Try to run things end-to-end (and from scratch) so we can catch these unexpected issues and code defensively
    -   E.g., we found out that TR data is malformed sometimes and only running end-to-end we can catch all the weird cases
    -   This also helps with scalability issues, since if takes 1 hr for 1 month of data and we have 10 years of data is going to take 120 hours (=5 days) to run on the entire data set


### Think about scalability



-   Do experiments to try to understand if a code solution can scale to the dimension of the data we have to deal with
    -   E.g., inserting data by doing SQL inserts of single rows are not scalable for pushing 100GB of data \

-   Remember that typically we need to run the same scripts multiple times (e.g., for debug and / or production)


### Use command line for reproducibility



-   Try to pass params through command line options when possible
    -   In this way a command line contains all the set-up to run an experiment


### Structure the code in terms of filters



-   Focus on build a set of "filters" split into different functions, rather than a monolithic flow
-   Organize the code in terms of a sequence of transformations that can be run in sequence, e.g.,
1. create SQL tables
2. convert json data to csv
3. normalize tables
4. load csv files into SQL
5. sanity check the SQL (e.g., mismatching TR codes, missing dates)
6. patch up SQL (e.g., inserting missing TR codes and reporting them to us so we can check with TR)


## Pipeline design pattern

Many of what said here applies also to notebooks, since a notebook is already structured as a pipeline


### Config contains everything

Centralize all the options into a config that is defined at the beginning of the notebook, instead of having parameters interspersed in the entire notebook.

The config controls what needs to be done (i.e., control flow) and how it's done.

E.g,, the SPY adjustment becomes a single function and the cell is:


```
    if config["market_adjustment"]:
        market_adjust_from_config(config)
```



### Make the pipeline think only in terms of config

Each function gets its params from the config. There is an adapter that adapts the config to the specific function allowing to keep the interfaces flexible

E.g.,


```
   def market_adjust(in_sample_start, in_sample_end):
      ...
```


The corresponding adapter is like:


```
   def market_adjust_from_config(config):
       return market_adjust(config["in_sample_start"], config["in_sample_end"])
```


in this way the pipeline (i.e., notebooks) just passes the config to all the functions and it's easy to maintain flexibility at the beginning. Once the code is more stable, one can remove the adapters.

This can be controversial and I usually do it only for functions whose interface I expect to change

### Repeat the structure but not the implementation

In other words, the “structure”, i.e., the control flow, is difficult to factor out across different notebooks, so it’s ok to repeat the structure of the code (e.g., cut and paste the same invocation of functions in different notebooks) as long as the implementation of the pieces doing the actual work is factored out.


### Have a way to compare and pretty-print configs

Self-explanatory.

pprint() is awesome


### Save the config together with the results

In this way one can know exactly how the results were generated. Also saving info about, user, server, time, git version and conda env can help.


### Use functions to allow total flexibility

Sometimes one piece of the pipeline becomes super complicated because one wants to do completely different things in different situations, so one ends up doing:


```
def stage(mode):
   if mode == "mode1":
      f(...)
   elif mode == "mode2":
      f(...)
      g(...)
   else:
      raise ValueError(...)
```


or tries to factor it out like


```
def stage(mode):
   if mode in ("mode1", "mode2"):
     f(...)
   if mode in ("mode2", ):
    g(...)
```


or creates a mini language passing strings that represents ways, e.g.,


```
{ name:my_filter_namecolumn_name:name_of_matching_feature_column_in_dataframebucket: { bucket_name:my_bucket_namebucket_spec: (>, threshold_1) | (<, threshold_2) } bucket: { bucket_name:my_2nd_bucket_namebucket_spec: (==, special_value) } ... }
```


The problem with the mini-language is that no matter how complex it is, one ends up always wanting to do something beyond the limits of the mini-language (e.g., “I want if-then-else”, “I want loops”, …)

Another approach is to use python directly instead of inventing a new “mini-language” by passing the name of a function and then evaluate it dynamically


```
config["stage_func"] = {
    "function_name": "foo_bar",
    "params": …
}

def stage(mode):
    f = eval(config["stage_func"]["function_name"])
    f(config["stage_func"]["params"])
```


For instance one can compose masks / filtering in any possible way by creating functions that combine the filters in different ways and then passing the name of the function to the pipeline.

One can also pass a pointer to the function directly instead of the name to be evaluated at later stage.


### Each function validates its own params

This is basic separation of responsibility, but sometimes one wants to do some checks at the beginning to avoid to get an assertion too late. Classical example: the name of the file to save the result is invalid and a long run crashes at the very end.

In this case an approach is to separate function and param validation like:


```
def f(...):
   check_f_params(...)
```


and then call check_f_params() also at the beginning when validating the config.


### Bend but not break

Always bend and try not to break.

E.g., to save a file, create the enclosing dir if missing, instead of having the OS throw an error.


### Make configs hierarchical

As soon as stages of the pipeline become hierarchical also the config becomes hierarchical (instead of flat) so that


```
def stage1(config):
   ...
   stage2(config["stage2"])
   …
```


E.g., generate_sentiment_mask()


```
config = {
   "generate_sentiment_mask": {
      "min": -0.5,
      "max": -0.5,
   }
}
```



### Add verbose mode

One problem is that to keep the notebook simple, one ends up having very complex that in the notebook were originals many cells with a lot of debug, plots in the middle. One approach is to have a "verbose" mode:


```
   def f1(..., verbose=False):
       # work work
       if verbose:
          # plot, print, display ...
       f2(..., verbose=verbose)
```


In this way when one wants to "debug" in the notebook one function (that corresponds to a lot of cells that have been collapsed to encapsulate / reuse), one can set the verbose mode and get all the debug spew, although one can’t easily break and inspect after one “cell”.

Global and function-specific verbose mode can even become config params.

Sometimes logging is better, although it’s not very notebook friendly.


### Multi-process + memoization > optimize one function

IMO it is better to keep functions simple even if slow, and then get speed-up by avoiding to recompute the same result and use coarse grain parallelism. Maybe obvious, maybe not.

