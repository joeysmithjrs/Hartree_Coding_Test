# HARTREE CODING TEST

This repository contains two Python solutions for data processing tasks provided by Hartree Partners as part of a coding test. The code has been authored by Joseph Smith to fulfill specific data manipulation and aggregation requirements using the Pandas library and Apache Beam.

## Overview

The solutions are designed to perform the following operations:

- **Merge two datasets**: Datasets `dataset1.csv` and `dataset2.csv` are merged on the `counter_party` column.
- **Calculate totals**: Totals for the legal entity, counterparty, and tier are computed.

The output files `{framework}_output_1.csv` and `{framework}_output_2.csv` correspond to the first and second requirements, respectively. The term `{framework}` is a placeholder and represents either `pandas` or `apache`, depending on which framework was used for processing.

## Files Description

- `pandas_solution.py`: This script processes the input data using the Pandas library. The output is saved as `pandas_output_1.csv` and `pandas_output_2.csv`.
- `apache_solution.py`: This script processes the input data using the Apache Beam framework. The output is saved as `apache_output_1.csv` and `apache_output_2.csv`.
- `assets/`: A directory containing input data files `dataset1.csv` and `dataset2.csv`.
- `output/`: A directory where all output CSV files are stored after script execution.

## How to Run

Ensure that you have Python 3.9 and all required dependencies installed. Dependencies can be installed using the following command:

```bash
pip install -r requirements.txt
```
To execute the scripts, run:
```bash
python pandas_solution.py
```
or 
```bash
python apache_solution.py
```
The scripts are to be executed independently, and upon completion, the output CSV files will be generated in the `output` directory.

## Assumptions

- The order of the rows in the output files is not significant.
- The calculations and formatting within the code are correct and meet the test specifications.

## Methodology

### Pandas Solution

The pandas-based solution consists of two main steps:

#### Merging Datasets

- **Data Loading**: Each dataset (`dataset1.csv` and `dataset2.csv`) is loaded into a pandas DataFrame.
- **Data Merging**: The two DataFrames are merged on the `counter_party` column to create a combined dataset.
- **Column Renaming**: Columns are renamed for consistency with the expected output. For instance, `rating` becomes `max(rating by counterparty)` to indicate the subsequent aggregation operation.

#### Data Aggregation and Calculation

- **Conditional Sums**: Conditional sums are computed for `ARAP` and `ACCR` statuses by applying functions across all rows.
- **Aggregation**: The merged DataFrame is grouped by `legal_entity`, `counterparty`, and `tier`. Aggregations are then performed based on the predefined operations in `agg_dict`.
- **Total Calculations**:
  - **By Legal Entity**: Totals are calculated for each `legal_entity`.
  - **By Legal Entity and Counterparty**: Totals are computed for each combination of `legal_entity` and `counterparty`.
  - **By Counterparty**: Totals are determined for each `counterparty`.
  - **By Tier**: Totals are found for each `tier`.

#### Output Generation

- The aggregated and calculated results are saved into CSV files (`pandas_output_1.csv` for merged datasets, `pandas_output_2.csv` for total calculations) in the output directory.

### Apache Beam Solution

The Apache Beam solution utilizes a robust data processing pipeline to transform and aggregate data from input datasets. Below is a detailed methodology:

#### Pipeline Setup

- **Pipeline Initialization**: The pipeline is initialized with the necessary configuration options to set up the execution environment.
- **Input Reading**: Two distinct CSV datasets are read, creating two separate PCollections for processing.

#### Data Preparation

- **Data Merging**: Datasets are keyed by 'counter_party' and then merged using `CoGroupByKey` which groups values from both datasets having the same key.
- **Joining Datasets**: The `ParDo` transform is used to join these datasets based on the 'counter_party' key. This step leverages the `Precursor` type.

#### Custom Data Types

- **Precursor**: A named tuple, `Precursor`, is defined to hold intermediate data after the join but before aggregation. This allows for handling complex data structures in a way that is both type-safe and clear.
- **Aggregated**: After the grouping and aggregation, the `Aggregated` named tuple is used to represent the results in a structured format. It encapsulates the aggregated fields such as `legal_entity`, `counter_party`, `tier`, `rating`, `ARAP`, and `ACCR`.

#### Aggregation Strategies

- **Strategy Definition**: Various grouping and aggregation strategies are defined using tuples that guide the data grouping and specify the aggregation functions.
- **Group and Aggregate**: The pipeline groups data according to the defined strategies and applies the corresponding custom aggregation function.

#### Data Aggregation

- **Aggregation Logic**: Within the `aggregate` function, custom logic is implemented to compute the maximum rating and conditional sums for 'ARAP' and 'ACCR' based on the status.

#### Output Generation

- **CSV Formatting**: The aggregated data, structured as `Aggregated` named tuples, is converted to CSV format lines using a custom function, `convert_to_csv_line`.
- **File Writing**: The formatted data is written to CSV files, with headers included. There's a distinct file for the first aggregation strategy and another for the combined output of subsequent strategies.

Using `Precursor` and `Aggregated` not only makes the pipeline steps clear and ensures the correct data structure is passed along, but also makes the code self-describing. The data flow through the pipeline is easier to understand and maintain, and other developers can quickly grasp the structure of data being processed at each stage.

## Author

- **Name**: Joseph Smith
- **Email**: [joeysmithjrs@gmail.com](mailto:joeysmithjrs@gmail.com)
- **Phone**: +1-508-768-7901

