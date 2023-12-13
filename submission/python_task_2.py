import pandas as pd

def calculate_distance_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate a distance matrix based on the dataframe, df.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Distance matrix
    """
    # Create an empty matrix with unique IDs
    unique_ids = sorted(set(df['id_start'].unique()) | set(df['id_end'].unique()))
    distance_matrix = pd.DataFrame(0, index=unique_ids, columns=unique_ids)

    # Fill the matrix with cumulative distances along known routes
    for _, row in df.iterrows():
        distance_matrix.at[row['id_start'], row['id_end']] += row['distance']
        distance_matrix.at[row['id_end'], row['id_start']] += row['distance']

    return distance_matrix

def unroll_distance_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Unroll a distance matrix to a DataFrame in the style of the initial dataset.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Unrolled DataFrame containing columns 'id_start', 'id_end', and 'distance'.
    """
    # Create an empty DataFrame to store unrolled distances
    unrolled_df = pd.DataFrame(columns=['id_start', 'id_end', 'distance'])

    # Iterate through the distance matrix
    for i in df.index:
        for j in df.columns:
            if i != j:
                unrolled_df = unrolled_df.append({'id_start': i, 'id_end': j, 'distance': df.at[i, j]}, ignore_index=True)

    return unrolled_df

def find_ids_within_ten_percentage_threshold(df: pd.DataFrame, reference_id: int) -> pd.DataFrame:
    """
    Find all IDs whose average distance lies within 10% of the average distance of the reference ID.

    Args:
        df (pandas.DataFrame)
        reference_id (int)

    Returns:
        pandas.DataFrame: DataFrame with IDs whose average distance is within the specified percentage threshold
                          of the reference ID's average distance.
    """
    # Filter rows where id_start is equal to the reference_id
    reference_rows = df[df['id_start'] == reference_id]

    # Check if reference_id is present in the DataFrame
    if reference_rows.empty:
        raise ValueError(f"Reference ID {reference_id} not found in the DataFrame.")

    # Calculate the average distance for the reference_id
    reference_average_distance = reference_rows['distance'].mean()

    # Calculate the threshold range
    lower_threshold = reference_average_distance - (reference_average_distance * 0.1)
    upper_threshold = reference_average_distance + (reference_average_distance * 0.1)

    # Filter rows where average distance is within the threshold range
    result_df = df.groupby('id_start')['distance'].mean().reset_index()
    result_df = result_df[(result_df['distance'] >= lower_threshold) & (result_df['distance'] <= upper_threshold)]

    return result_df

def calculate_toll_rate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate toll rates for each vehicle type based on the unrolled DataFrame.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Create new columns for toll rates
    df['moto'] = df['distance'] * 0.8
    df['car'] = df['distance'] * 1.2
    df['rv'] = df['distance'] * 1.5
    df['bus'] = df['distance'] * 2.2
    df['truck'] = df['distance'] * 3.6

    return df

import pandas as pd
from datetime import time

def calculate_time_based_toll_rates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates toll rates for different time intervals within a day.

    Args:
        df (pandas.DataFrame): Input DataFrame with columns 'startDay', 'startTime', 'endDay', 'endTime'.

    Returns:
        pandas.DataFrame: Original DataFrame with additional columns for start_day, start_time, end_day, and end_time.
                          Vehicle columns are modified based on the specified time ranges and discount factors.
    """
    # Define time ranges and discount factors
    weekday_discounts = [(time(0, 0, 0), time(10, 0, 0), 0.8),
                         (time(10, 0, 0), time(18, 0, 0), 1.2),
                         (time(18, 0, 0), time(23, 59, 59), 0.8)]

    weekend_discount = 0.7

    # Create new columns for start_day, start_time, end_day, and end_time
    df['start_datetime'] = pd.to_datetime(df['startDay'] + ' ' + df['startTime'])
    df['end_datetime'] = pd.to_datetime(df['endDay'] + ' ' + df['endTime'])

    df['start_day'] = df['start_datetime'].dt.day_name()
    df['start_time'] = df['start_datetime'].dt.time
    df['end_day'] = df['end_datetime'].dt.day_name()
    df['end_time'] = df['end_datetime'].dt.time

    # Apply discount factors based on time ranges
    for start_time, end_time, discount_factor in weekday_discounts:
        mask = (df['start_time'] >= start_time) & (df['start_time'] <= end_time) & (df['start_day'].isin(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']))
        df.loc[mask, ['car', 'bus', 'truck']] *= discount_factor

    # Apply constant discount factor for weekends
    mask_weekend = df['start_day'].isin(['Saturday', 'Sunday'])
    df.loc[mask_weekend, ['car', 'bus', 'truck']] *= weekend_discount
    
   result_df = calculate_time_based_toll_rates(df)
    
   print(result_df)



