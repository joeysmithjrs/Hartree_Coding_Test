import pandas as pd

# Define a dictionary that specifies the aggregation operation for each column
agg_dict = {
    'max(rating by counterparty)': 'max',  # Define the operation for max rating
    'sum(value where status=ARAP)': 'sum',  # Define the operation for summing ARAP values
    'sum(value where status=ACCR)': 'sum'   # Define the operation for summing ACCR values
}


def merge_datasets():
    # Load the data from CSV files into DataFrames
    df1 = pd.read_csv('assets/dataset1.csv')
    df2 = pd.read_csv('assets/dataset2.csv')

    # Merge the two DataFrames on the 'counter_party' column, which is common to both
    merged_df = pd.merge(df1, df2, on='counter_party')

    # Rename columns for consistency and to match the expected output structure
    merged_df.rename(columns={
        'counter_party': 'counterparty',  # Rename for consistency
        'rating': 'max(rating by counterparty)'  # Rename to specify the aggregation
    }, inplace=True)

    # Calculate conditional sums for ARAP and ACCR status by applying a function across all rows
    merged_df['sum(value where status=ARAP)'] = merged_df.apply(
        lambda x: x['value'] if x['status'] == 'ARAP' else 0, axis=1)
    merged_df['sum(value where status=ACCR)'] = merged_df.apply(
        lambda x: x['value'] if x['status'] == 'ACCR' else 0, axis=1)

    # Group the merged DataFrame by 'legal_entity', 'counterparty', and 'tier' and perform specified aggregations
    group_list = ['legal_entity', 'counterparty', 'tier']
    grouped = merged_df.groupby(group_list)
    aggregated = grouped.agg(agg_dict).reset_index()

    return aggregated


def calculate_totals(aggregated):
    # Aggregate data to compute totals for each group level: legal_entity, legal_entity & counter_party, etc.
    # Each total is computed separately and then combined into one DataFrame

    # Calculate totals for each legal_entity
    legal_entity_totals = aggregated.groupby('legal_entity').agg(agg_dict).reset_index()
    legal_entity_totals['counterparty'] = 'Total'  
    legal_entity_totals['tier'] = 'Total'  

    # Calculate totals for each combination of legal_entity and counter_party
    legal_entity_counter_party_totals = aggregated.groupby(['legal_entity', 'counterparty']).agg(agg_dict).reset_index()
    legal_entity_counter_party_totals['tier'] = 'Total'  

    # Calculate totals for each counter_party
    counter_party_totals = aggregated.groupby('counterparty').agg(agg_dict).reset_index()
    counter_party_totals['legal_entity'] = 'Total' 
    counter_party_totals['tier'] = 'Total'  

    # Calculate totals for each tier
    tier_totals = aggregated.groupby('tier').agg(agg_dict).reset_index()
    tier_totals['legal_entity'] = 'Total'  
    tier_totals['counterparty'] = 'Total'  

    # Combine all the calculated totals into a single DataFrame
    combined_results = pd.concat(
        [legal_entity_totals, legal_entity_counter_party_totals, counter_party_totals, tier_totals], ignore_index=True)

    # Rearrange the columns to match the expected output format
    final_results = combined_results[
        ["legal_entity", "counterparty", "tier", "max(rating by counterparty)", "sum(value where status=ARAP)",
         "sum(value where status=ACCR)"]]

    return final_results


if __name__ == '__main__':
    # Call the merge_datasets function and save to CSV
    aggregated_df = merge_datasets()
    aggregated_df.to_csv('output/pandas_output_1.csv', index=False)

    # Call the calculate_totals function to get the totals and save to CSV
    total_calculation_df = calculate_totals(aggregated_df)
    total_calculation_df.to_csv('output/pandas_output_2.csv', index=False)
