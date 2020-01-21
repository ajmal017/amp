# Definitions and principles

## `DataSource`

- A `DataSource` is a collection of datasets from a specific origin (e.g., a
  website like `eia.org`, the WIND terminal)

## Data in a `DataSource`

- Each `DataSource` typically contains:

  1. Metadata (i.e., information about the data, e.g., a description of each
     time series)
     - Some `DataSource` might not have metadata and contain just payload data
  2. Payload data (e.g., time series, point in time data, tables, PDFs with
     text)

- This data comes in "raw form"
  - E.g., the schema for both 1. and 2. is typically different among different
    data sources, irregular, and incomplete
- We want to convert any raw data into our internal data representation

## Time series

- Each `DataSource` typically is composed of many time series
  - Time series may be univariate or multivariate

## Raw metadata and payload

- We define as "raw" any data (both metadata and payload) in the form it
  originally existed in the data source, e.g.,
  - Raw metadata in case there was a file with a directory of the data
  - Zipped CSV files containing timeseries data

- The raw data is stored in the ETL2 layer
  - The ETL2 layer stores both the raw data and the P1 data

## P1 metadata and payload

- This is data that has been transformed in our internal format

- This is an example of raw metadata:
  ```
  ;updates;pub_date;document_type;organisation;part_of_a_collection;short_desc;title;updated;page_url;name;doc_url;doc_type;size;frequency
  0;['2020-01-14T15:33:56.000+00:00', '2019-10-10T09:30:00.000+01:00'];Published 10 October 2019;National Statistics;Department for Business, Energy & Industrial Strategy;Business Population Estimates;Annual business population estimates for the UK and regions in 2019.;Business population estimates 2019;14 January 2020;/government/statistics/business-population-estimates-2019;Business population estimates for the UK and regions 2019: Statistical Release (PDF);https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/852919/Business_Population_Estimates_for_the_UK_and_regions_-_2019_Statistical_Release.pdf;PDF;636KB;[]
  ```

## Conventions

- Every piece of information downloaded or inserted manually should be traceable
  - Where did it come from?
    - E.g., `xyz.org` website, a paper, a book
  - Who added that information and when?
    - Note that who made the modification to a data structure (e.g., Max) might
      be different from whom made the change in the db (e.g., Paul committed the
      change)
    - Git is tracking the second part, but we want to track the first part
    - Context about data should be available (e.g., GitHub task might be the
      best way)
- All metadata should be described in this document
  - The field names should
    - have a name as long as needed to be clear, although concise
    - have underscores and not spaces
    - be capitalized
    - have a type associated
    - a description
- We should qualify if something is an estimate (e.g., ~\$1000) or not (e.g.,
  \$725 / month)
- How much do we believe in this information?
  - Is it a wild guess?
  - Is it an informed guess?
  - Is it what we were told by somebody on the street?

# Our flow for ingesting data

- The process we follow is:

## 1. Idea generation

- We come up with ideas from papers, books, ...
- Currently this is done informally in GitHub tasks

## 2. Data sets collection

- It is informed by a modeling idea or just pre-emptively (e.g., "this data
  makes us come up with a modeling ideas")
  - E.g., see GitHub tasks under the `Datasets` milestone

- Currently, the result of this activity should go in the `MonsterDataSource`
  (see below), and currently is in the Monster Spreadsheet

## 3. Exploratory analysis of data sets

- We:
  - look for data needed by a model; or
  - browse data and come up with modeling ideas

- Currently the result of this activity is in the GitHub tasks / Monster
  Spreadsheet

## 4. Prioritize data sources downloading

- We decide which data set to download:
  - Based on business objective (e.g., a source for oil vs one for ags)
    - Amount of models that can be built out of the data
  - Complexity of downloading
  - Uniqueness
  - Cost
  - ...

- Currently we:
  - Track these activities in the Monster Spreadsheet and
  - File issues against ETL2

## 5. Data download

- Download the raw data (both metadata and payload) and put it into a suitable
  form inside ETL2

- Ideally we would like to have each data source to be available both
  historically and in real-time
  - On the one side, only the real-time data can inform us on publication delay,
    reliability of the downloading process, delay to acquire the data on our
    side, throttling, ...
  - On the other side, we would prefer to do the additional work of putting data
    in production (with all the on-going maintenance effort) only when we know
    this data is useful for our models or can be sold
  - We need to strike a balance between these two needs

- Currently we track these activities into GH tasks
  - We use the `DataEncyclopedia` and `etl_guides` to track data sources
    available and APIs

## 6. Transform data into our internal P1 representation

- See below

## 7. Sanity check of the data

- We want to check that the downloaded data is sane, e.g.,
  - Did we miss anything we wanted to download?
  - Does the data look good?
  - Compute statistics of the time series (e.g., using our timeseries stats
    flow)

## 8. Expose data to researchers

- Researchers access the data from ETL2

- Ideally we would like to share the access mechanisms with customers as much as
  possible (of course with the proper access control)
  - E.g., we could build REST APIs that call into our internal APIs

# Complexities in the design

### How to handle data already in relational form?

- Some data is already in a relational form, e.g.,
  - Information about from which data source a time series come from
    - We don't want to replicate information about a data source (e.g., its
      `URL`)
  - The source that informed a certain data source or relationship

- We want to store information about our internal process, e.g.,
  - What is the priority of having a certain data source / time series available
    internally
  - What is the status of a data source (e.g., "downloaded", "only-historical
    data downloaded", "real-time")
  - What is the source of a data source (e.g., "WIND", ..., scraping website)

- It can be argued that information about the infra should not be mixed with
  research ones
  - The issue is that the process of discovering data sources and on-boarding
    data sources moves at different speed
    - E.g., one researcher (or potentially even a customer!) might want to know:
      - "what are the sources about oil that are available?"
      - "what are the next sources to download?"
      - "do we have only historical data or real-time of a data source?"
      - "what are the models built in production from a data source?"
  - Thus inevitably we will need to "join multiple tables" from research and
    infra
    - At this point let's just make it simpler to do instead of maintaining
      different data structures

### Successive approximations of data

- It can happen that for a data source some of the fields are filled manually
  initially and then automatically updated
  - E.g., we can have an analyst fill out the duration of the data (e.g., "from
    2000 to today") and then have automatic processes populate this data
    automatically

### Access control

- We need to have policies to expose some of the data only internally; or to
  certain customers

- We can group fields into different "tables"
  - Shared: fields
  - Internal
  - Customer

# Internal representations

## `MonsterDataSource`

- TODO(*): Ok to come up with better names, but we might need to have names for
  these data structures so it's easier to understand what we are referring to
  (e.g., the Monster Spreadsheet)

- Tracked
  [PartTask583](https://github.com/ParticleDev/commodity_research/issues/578)

- The `MonsterDataSource` stores all the data sources we are aware of
  - In practice it is a machine readable form of the Monster Spreadsheet
  - It is represented by a single CSV file
  - It is checked in the repo under the `//p1/metadata`
- There is a notebook that loads the CSV as pandas dataframe
- There is a library that allows to query, compute stats, and manipulate this
  data structure, e.g.,
  - What data sources are available already?
  - How many data sources do we know?
  - How many data sources are available in ETL2?
  - There are sanity checks to make sure the representation is consistent (e.g.,
    make sure that the values in special columns have the right type and values)

- This is the result of "Data sets collection" step
  - Typically analysts are in charge of manipulating it

- Probably this will evolve into a full blown database table at some point
  - For now we want to keep it as a CSV so we can:
    - Version control
    - Review the changes before commit

### P1 fields

- `ID`
  - P1 data source internal name
  - E.g., `EIA_001`
- `DATA_SOURCE`
  - The symbolic name of the data source
  - E.g., "USDA"
- `DATASET`
  - Optional
  - Represents the fact that one data set can be organized in multiple data
    sets, each with many time series
  - E.g., For USDA there are several data sets "Agricultural Transportation Open
    Data Platform", "U.S. Agricultural Trade Data"
- `SUMMARY`
  - Human readable summary
    - What does this dataset contain?
    - This is a free form description with links to make easier for a human to
      understand what the data set is about
  - E.g., "The U.S. Energy Information Administration (EIA) collects, analyzes,
    and disseminates independent and impartial energy information to promote
    sound policymaking, efficient markets, and public understanding of energy
    and its interaction with the economy and the environment."
- `SUMMARY_SOURCE`
  - Where did we know about this data source
  - E.g., it can be an URL, a paper, a book
- `DATASOURCE_URL`
  - E.g., `www.eia.gov`
- `DATASET_URL`
  - Links to the webpage about the specific dataset
- `DESCRIPTION_URL`
  - Links to the webpage with some description of the data
  - E.g., `https://agtransport.usda.gov/`
- `COLLECTION_TYPE`
  - What is the predominant source of the data
    - Survey: data that is collected by 'survey' methodology
    - First-hand: closest source of the data
    - Aggregation: the source just present the information which comes from
      other party
    - Search engine
- `DOWNLOAD_STATUS`
  - Represents whether we have:
    - Historical downloaded: the raw historical data is in ETL2
    - Historical metadata processed: the metadata has been processed and it's
      available
    - Historical payload data processed: the payload data is available through
      ETL2
    - ...
- `SUBSCRIPTION_TYPE`
  - Free
  - Subscription
  - Both: source has open data and paid services simultaneously
- `COST`
  - Indicative cost, if subscription
- `HIGHEST_FREQUENCY`
  - Highest frequency available from a exploratory inspection, e.g.,
    - Annual
    - Daily
    - Hourly
    - Monthly
    - Quarterly
    - Unspecified
- `RELEASE`
  - When the data is released, e.g.,
    - Different releases
    - End of month
    - Third Friday of the month
    - Unspecified
- `COMMODITY_TARGETS`
  - What target commodity it can be used for (from exploratory analysis), e.g.,
    - Agriculture
    - Climate
    - Coal
    - Commodity: contains info about agricultural, metal, energy commodities as
      a whole
    - Copper
    - Corn
    - Energy: contains oil + gas or other oil products
    - Gold
    - Macroeconomic data
    - Market: contains data about market indicators
    - Metals
    - Natural gas
    - Oil
    - Other
    - Palladium
    - Platinum
    - Silver
    - Soybean
    - Steel
    - Sugar
    - Trade: trade data, freight data etc.
- `GEO`
  - Geographical location that this data is mainly about, e.g.,
    - Global
    - US
    - China
    - Europe
- `GITHUB_ISSUE`
  - Number (or link) for the GitHub issue tracking this specific data set
- `GITHUB_ETL2_ISSUE`
  - Number (or link) for the GitHub issue tracking the downloading of this
    specific data sets
- `TAGS`
  - Wind: WIND terminal data sources
  - Chinagov: Chinese government sources of data
  - Baidu: data sources found using Baidu
  - Shf: sources from data vendors of Shanghai Futures Exchange
  - Papers that referred to this
  - Edgar: EDGAR equivalents in a given country
  - Wind+: sources from WIND Commodity DB
  - 600: sources from Task 600 from Github issues
  - TODO(gp): To reorg
- `NOTES`
  - This is a free-form field which also incubates data that can become a field
    in the future
    - Why and how is this data relevant to our work?
    - Is there an API? Do we need to scrape?
    - Do we need to parse HTML, PDFs?
    - How complex do we believe it is to download?
- `PRIORITY`
  - Our subjective belief on how important a data source is. This information
    can help us prioritize data source properly
  - E..g, P0

## `MonsterMetaData`

- For each data source in the `MonsterDataSource` there is a dataframe
  with information about all the data contained in the data source

- Each metadata for a timeseries contains a unique P1 `ID` that can be used to
  retrieve the data from ETL2

- The KnowledgeGraph contains pointers to metadata of timeseries

### Fields

Task 921 - KG: Generate spreadsheet with time series info

- `ID`
  - Internal P1 ID
- `NAME`
  - 
- `ALIASES`
- `URL`
- `SHORT DESCRIPTION`
- `LONG DESCRIPTION`
- `SAMPLING FREQUENCY`
- `RELEASE FREQUENCY`
- `RELEASE DELAY`
- `START DATE`
- `END DATE`
- `UNITS OF MEASURE`
- `TARGET COMMODITIES`
- `SUPPLY / DEMAND / INVENTORY`
- `GEO`
- `RELATED PAPERS`
- `INTERNAL DATA POINTER`

## `MonsterPayloadData`

- Tracked in
  [PartTask951: ETL2: Uniform access to ETL2 data](https://github.com/ParticleDev/commodity_research/issues/951)

- ETL2 has interfaces to access data from each data source that we have
  downloaded
- We want to have a single interface sitting on top of the data source specific
  API
- This Uniform API should be able to return a timeseries given a unique ID
  - The format of this data is fixed, e.g., it is a `pd.DataFrame` or
    `pd.Series` indexed by datet imes with one or multiple columns

## `KnowledgeGraph`

- This graph represents relationships between economic entities and data in ETL2
  - E.g., what predicts crude oil demand, which timeseries are related to crude
    oil demand

- This is described in detail in the document `knowledge_graph_example.md`

# Flow of data among representations

- Download raw historical ETL2 data
  - Data is added to ETL2

- Download raw real-time ETL2 data
  - Same as above but for the real-time loop

- Transform raw data into our internal representation
  - E.g., extract raw metadata and convert it into P1 metadata
    - This consists in mapping fields from the raw metadata into our P1 internal
      representation
    - Convert the values into Python types
  - E.g., extract raw payload data and convert it into P1 data, if needed
    - Note that if the data is in a suitable format (e.g., CSV form) we might be
      able to convert it on the flight to our internal `pandas` representation
    - If it's in a PDF or other unstructured data format we want to extract the
      data and save it

# Principles

## P1 data and raw data
- P1 data

It's ok if we decide not to process the data, if we don't think it's high priority. So it's ok to stop here, but we can use it to implement the rest of the KG / ETL2 flow.
Taking a look the CSV file in the zip file is compatible to our metadata statistics flow, which we started but not finished. We should complete it at some point.
I would still import the metadata in our system (#578, #921) even if we don't have the payload data available in accessible form through the Uniform access (#951)
Let's start using some standard names
#578 -> MonsterDataSourceDb
#921 -> MonsterTimeSeriesDb
#951 -> UniformETL
I propose as next immediate steps to use this data source as running example to implement the entire system
Save the csv file with the metadata in ETL2 as "raw" data
We should be able to access this in the same way we can access "raw" data
Map the columns of this specific metadata csv file to our general metadata flow
Finish the metadata statistics flow
Run the statistics flow on this data
Import the metadata about this data source into the MonsterDataSourceDb
We should have an entry about this data source reporting the state as "raw data downloaded, metadata processed, data not exposed through UniformETL"
Import all the metadata about the time series into the MonsterTimeSeriesDb


## Knowledge base

- There is an ontology for economic phenomena
- Each time series relates to nodes in the ontology
