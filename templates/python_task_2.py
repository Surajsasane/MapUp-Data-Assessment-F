import pandas as pd


def calculate_distance_matrix(df)->pd.DataFrame():
    """
    Calculate a distance matrix based on the dataframe, df.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Distance matrix
    """
    # Write your logic here
    # Create an empty matrix with unique IDs
    unique_ids = sorted(set(df['id_start'].unique()) | set(df['id_end'].unique()))
    distance_matrix = pd.DataFrame(0, index=unique_ids, columns=unique_ids)

    # Fill the matrix with cumulative distances along known routes
    for _, row in df.iterrows():
        distance_matrix.at[row['id_start'], row['id_end']] += row['distance']
        distance_matrix.at[row['id_end'], row['id_start']] += row['distance']

    return distance_matrix

    return df


def unroll_distance_matrix(df)->pd.DataFrame():
    """
    Unroll a distance matrix to a DataFrame in the style of the initial dataset.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Unrolled DataFrame containing columns 'id_start', 'id_end', and 'distance'.
    """
    # Write your logic here
    # Create an empty DataFrame to store unrolled distances
    unrolled_df = pd.DataFrame(columns=['id_start', 'id_end', 'distance'])

    # Iterate through the distance matrix
    for i in df.index:
        for j in df.columns:
            if i != j:
                unrolled_df = unrolled_df.append({'id_start': i, 'id_end': j, 'distance': df.at[i, j]}, ignore_index=True)

    return unrolled_df

    return df


def find_ids_within_ten_percentage_threshold(df, reference_id)->pd.DataFrame():
    """
    Find all IDs whose average distance lies within 10% of the average distance of the reference ID.

    Args:
        df (pandas.DataFrame)
        reference_id (int)

    Returns:
        pandas.DataFrame: DataFrame with IDs whose average distance is within the specified percentage threshold
                          of the reference ID's average distance.
    """
    # Write your logic here
    
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

    return df


def calculate_toll_rate(df)->pd.DataFrame():
    """
    Calculate toll rates for each vehicle type based on the unrolled DataFrame.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Wrie your logic here
    # Create new columns for toll rates
    df['moto'] = df['distance'] * 0.8
    df['car'] = df['distance'] * 1.2
    df['rv'] = df['distance'] * 1.5
    df['bus'] = df['distance'] * 2.2
    df['truck'] = df['distance'] * 3.6

    return df

  


def calculate_time_based_toll_rates(df)->pd.DataFrame():
    """
    Calculate time-based toll rates for different time intervals within a day.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Write your logic here
        # Convert start_time and end_time to datetime.time()
    df['start_time'] = pd.to_datetime(df['start_time']).dt.time
    df['end_time'] = pd.to_datetime(df['end_time']).dt.time

    # Create new columns for start_day, end_day, and time-based toll rates
    df['start_day'] = df['start_datetime'].dt.strftime('%A')
    df['end_day'] = df['end_datetime'].dt.strftime('%A')

    # Define time intervals for weekdays and weekends
    weekday_intervals = [
        (pd.to_datetime('00:00:00').time(), pd.to_datetime('10:00:00').time(), 0.8),
        (pd.to_datetime('10:00:00').time(), pd.to_datetime('18:00:00').time(), 1.2),
        (pd.to_datetime('18:00:00').time(), pd.to_datetime('23:59:59').time(), 0.8)
    ]

    weekend_intervals = [
        (pd.to_datetime('00:00:00').time(), pd.to_datetime('23:59:59').time(), 0.7)
    ]

    # Apply time-based toll rates for each row
    for _, row in df.iterrows():
        if row['start_day'] in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
            intervals = weekday_intervals
        else:
            intervals = weekend_intervals

        for start, end, discount_factor in intervals:
            if start <= row['start_time'] <= end and start <= row['end_time'] <= end:
                df.at[_, 'moto'] = row['moto'] * discount_factor
                df.at[_, 'car'] = row['car'] * discount_factor
                df.at[_, 'rv'] = row['rv'] * discount_factor
                df.at[_, 'bus'] = row['bus'] * discount_factor
                df.at[_, 'truck'] = row['truck'] * discount_factor
                break

    return df

    
