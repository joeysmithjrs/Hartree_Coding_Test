import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from typing import NamedTuple

# Define various grouping and aggregation strategies as tuples.
strategies = [
    ("Group by Legal Entity, Counter Party, and Tier", 0, ['legal_entity', 'counter_party', 'tier']),
    ('Group by Legal Entity', 1, ['legal_entity']),
    ('Group by Legal Entity and Counter Party', 2, ['legal_entity', 'counter_party']),
    ('Group by Counter Party', 3, ['counter_party']),
    ('Group by Tier', 4, ['tier']),
]


# Function to join two datasets on a common key.
def join_datasets(data):
    _, (rows, tier_info) = data  # Unpack the input data into rows and tier information.
    tier = tier_info[0] if tier_info else None  # Select the first tier info if available.

    # Return an empty list if there is no tier info to join on.
    if tier is None:
        return []

    # Iterate through rows and combine information into a new Row.
    return [(Precursor(
        legal_entity=row.legal_entity,
        counter_party=row.counter_party,
        rating=row.rating,
        status=row.status,
        value=row.value,
        tier=tier,
    )) for row in rows]


# Function to aggregate values by different keys based on aggregation type.
def aggregate(data, strategy):
    key, rows = data
    agg = {'ARAP': 0, 'ACCR': 0, 'rating': 0}

    # Iterating through rows to compute max rating and sums of ARAP and ACCR statuses.
    for row in rows:
        agg['rating'] = max(agg['rating'], row.rating)
        agg['ARAP'] += row.value if row.status == "ARAP" else 0
        agg['ACCR'] += row.value if row.status == "ACCR" else 0

    # Initial aggregation returns a dictionary.
    if strategy == 0:
        return key, {
            'max(rating by counterparty)': agg['rating'],
            'sum(value where status=ARAP)': agg['ARAP'],
            'sum(value where status=ACCR)': agg['ACCR']
        }

    # Subsequent aggregations return Aggregated.
    # Select fields based on the aggregation type.
    legal_entity = rows[0].legal_entity if strategy in (1, 2) else "Total"
    counter_party = rows[0].counter_party if strategy in (2, 3) else "Total"
    tier = rows[0].tier if strategy == 4 else "Total"

    return Aggregated(
        legal_entity=legal_entity,
        counter_party=counter_party,
        tier=tier,
        rating=agg['rating'],
        ARAP=agg['ARAP'],
        ACCR=agg['ACCR'],
    )


# Convert data to a CSV formatted string.
def convert_to_csv_line(data_item):
    # Check if data item is a tuple containing a named tuple and a dictionary.
    if isinstance(data_item, tuple) and len(data_item) == 2 and isinstance(data_item[1], dict):
        named_tuple, result_dict = data_item
        # Concatenate the named tuple values and dictionary values into a CSV string.
        return ','.join(str(value) for value in named_tuple) + ',' + ','.join(
            str(value) for value in result_dict.values())
    else:
        # For a single named tuple, join its values to form a CSV line.
        return ','.join(str(value) for value in data_item)


def read_inputs(p):
    # Read input CSV files and create PCollection instances for further transformations.
    reader_1 = beam.io.ReadFromCsv("assets/dataset1.csv", splittable=False)
    reader_2 = beam.io.ReadFromCsv("assets/dataset2.csv", splittable=False)

    reader_1.label, reader_2.label = "Reader1", "Reader2"  # Label readers for identification.
    # Key rows by 'counter_party' to facilitate joining with the second dataset.
    return p | reader_1 | beam.Map(lambda x: (x.counter_party, x)), p | reader_2


def apply_group_and_aggregate(joined_datasets, strategy):
    (strategy_name, aggregation_type, fields) = strategy
    return (
            joined_datasets
            | f'{strategy_name}' >> beam.GroupBy(*fields)  # Group data based on the specified fields.
            | f'Aggregate {strategy_name}' >> beam.Map(aggregate, aggregation_type)  # Apply the aggregate function.
            .with_output_types(Aggregated)  # Define the output type of the PCollection.
    )


def write_to_file(data, file_prefix):
    # Format the PCollection to CSV lines.
    data = data | f"Format {file_prefix} as CSV" >> beam.Map(convert_to_csv_line)
    return (
            data | f"Write {file_prefix} to CSV" >> WriteToText(
        file_path_prefix=file_prefix,  # Set the output file prefix.
        file_name_suffix='.csv',  # Specify the file extension.
        shard_name_template='',  # Avoid creating additional shards.
        header='legal_entity,counterparty,tier,max(rating by counterparty),sum(value where status=ARAP),sum(value '
               'where status=ACCR) '
    )
    )


# Named tuple for representing the joined precursor data.
class Precursor(NamedTuple):
    legal_entity: str
    counter_party: str
    rating: int
    status: str
    value: int
    tier: int


# Named tuple for the aggregated output.
class Aggregated(NamedTuple):
    legal_entity: str
    counter_party: str
    tier: int
    rating: int
    ARAP: int
    ACCR: int


# Entry point for the Beam pipeline execution.
def run_pipeline():
    pipeline_options = PipelineOptions()  # Initialize pipeline options.
    with beam.Pipeline(options=pipeline_options) as p:
        dataset1, dataset2 = read_inputs(p)  # Read input datasets.

        # Merge the datasets by 'counter_party' and prepare them for aggregation.
        merged_datasets = (
                [dataset1, dataset2]
                | "Merge datasets by counter_party" >> beam.CoGroupByKey()
                | "Join datasets" >> beam.ParDo(join_datasets).with_output_types(Precursor)
        )

        # Apply the group and aggregate strategy to the joined datasets.
        universe = [
            apply_group_and_aggregate(merged_datasets, strategy)
            for strategy in strategies
        ]

        # Write the first strategy's result to a CSV file.
        write_to_file(universe[0], 'output/apache_output_1')
        # Combine the remaining strategies' results, flatten, and write to another CSV file.
        write_to_file(universe[1:] | "Combine all Outputs" >> beam.Flatten(), 'output/apache_output_2')

        # Execute the pipeline and wait until it completes.
        pipeline_result = p.run()
        pipeline_result.wait_until_finish()


if __name__ == "__main__":
    run_pipeline()
