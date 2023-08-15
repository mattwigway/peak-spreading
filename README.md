# Peak spreading

## Getting the data

The exact data used is available from the [UNC Dataverse](https://doi.org/10.15139/S3/OUIPOT). This code is available from there as well, but the original code is hosted [on Github](https://github.com/mattwigway/peak-spreading). The version used in the PLoS paper is tagged `plos-final`.

The original data source for the data is [Caltrans PeMS](https://pems.dot.ca.gov/) through a web interface that allows downloading data for a single district and day at a time. We have several thousand district-day combinations, so downloading them all would be tedious. The `scrape_data.py` script will download the data from PeMS, if you do not wish to get it from Dataverse. It expects `PEMS_USER` and `PEMS_PASSWORD` to be environment variables. It expects a data folder as an argument. This should be on a disk with plenty of space---the full dataset is several hundred gigabytes.

What years and districts to download are specified at the top of the script. The script is smart enough to not re-download files that already exist in the output directory, but even retrieving metadata can take some time, so if you know a year is already downloaded it can be wise to specify the years you wish to download.

The script artificially stops downloading 2022 data after August 18th to match the paper. Delete those lines if you wish to download additional data.

The PeMS database sometimes returns partial files. If you re-run the script, it will tell you (with the text `WARNING`) when file sizes on disk do not match the sizes expected by PeMS. I recommend investigating any warnings.

## Running the analysis

Most data prepation code is in scripts and is run at the command line.

### `calculate_peaks.jl`

This file calculates the peak-hour statistics for every day, in parallel using Julia Distributed (not multithreading). Run it with the path to the folder where you downloaded the files.

## `combine_peaks.jl`

The `calculate_peaks.jl` script processes all of the downloaded files in parallel, and writes one output file per input file. The `combine_peaks.jl` script combines all of these files together. Run it with the path to folder with the data, and the name of the file you want to save the combined results to.

## `Sensor shifts.ipynb`

This notebook reads in all the metadata (download all the metadata files that were active during the analysis period with `scrape_data.py` with the `--type meta` option), and finds sensors that moved during the analysis period, or that changed freeways, directions, or numbers of lanes, so they can be excluded from the analysis. It creates `good_sensors.csv`

## `Urban rural sensors.ipynb`

This notebook reads the output of `Sensor shifts.ipynb` and further categorizes sensors into urban or rural, based on Census Bureau Urbanized Area definitions.

## `geo_data.jl`

This script takes the output of `combine_peaks` and merges it with metadata from `sensor_meta_geo.csv`. It does not take any arguments, but assumes it will find the files in `data/{all_district_peaks.parquet, sensor_meta_geo.csv}`

## `Peak spreading analysis.ipynb`

This is the main peak spreading code. It processes the peak data and produces the main results table, the heterogeneity figure, the cumulative distribution figures, and statistics on exclusion by region.

## `Missing data patterns.ipynb`

This produces the missing data figure (Figure 3).

## `Sensor descriptive statistics.ipynb`

This produces basic descriptive statistics on the used sensors (proportion in urban areas).

## `Total flow plot.ipynb`

This produces Figure 4.

## `Missing data robustness model.ipynb`

This produces the models in the appendix.

## `Fundamental diagrams.ipynb`

This produces the fundamental diagrams in Figure 2.

## Automated tests

Most key aspects of the analysis have automated tests in the test/ directory. Run them by typing `]test` at the Julia prompt. Tests are run on each commit by Github Actions.



Notebooks code is in notebooks. In addition to jupyterlab, install [Julia](https://julialang.org) and IJulia to run them.

