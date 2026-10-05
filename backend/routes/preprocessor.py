import numpy as np

def build_features(df):
    df = df.sort_values(by=['timestamp'])

    # 1. Cyclical Time Encoding (The Rhythm)
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)

    # 2. Service Divergence Ratios (Avoid division by zero using np.where)
    df['data_to_voice_ratio'] = np.where(
        (df['call_in'] + df['call_out']) > 0, 
        df['internet'] / (df['call_in'] + df['call_out']), 
        0
    )

    # 3. Trailing Window Features (Ending at time 't')
    # We group by grid_id so the rolling windows do not bleed across different grids
    grouped_activity = df.groupby('grid_id')['total_activity']

    # avg_activity: Mean over the last 24 hours
    df['avg_activity'] = grouped_activity.transform(
        lambda x: x.rolling(24, min_periods=1).mean().shift(1)
    )

    # active_hours: Count of hours > 0 in the last 24 hours
    df['active_hours'] = grouped_activity.transform(
        lambda x: (x > 0).rolling(24, min_periods=1).sum().shift(1)
    )

    # peak_ratio: Max / Mean over the last 24 hours
    df['peak_ratio'] = grouped_activity.transform(
        lambda x: (x.rolling(24, min_periods=1).max() / 
                x.rolling(24, min_periods=1).mean()).shift(1)
    )

    # variability: Coefficient of Variation (Std / Mean)
    df['variability'] = grouped_activity.transform(
        lambda x: (x.rolling(24, min_periods=1).std() / 
                x.rolling(24, min_periods=1).mean()).shift(1)
    )

    # activity_growth: 12 hours vs previous 12 hours (Fits within 24h window)
    # Corrected: Removed ['total_activity'] since grouped_activity already includes it
    recent_12h_avg = grouped_activity.transform(lambda x: x.rolling(12, min_periods=1).mean().shift(1))
    past_12h_avg = grouped_activity.transform(lambda x: x.rolling(12, min_periods=1).mean().shift(13))
    df['activity_growth'] = recent_12h_avg / past_12h_avg

    # internet_share: Shifted by 1 to represent the share up to time 't'
    df['internet_share'] = (df['internet'] / df['total_activity']).fillna(0)
    df['internet_share_at_t'] = df.groupby('grid_id')['internet_share'].shift(1)

    return df
