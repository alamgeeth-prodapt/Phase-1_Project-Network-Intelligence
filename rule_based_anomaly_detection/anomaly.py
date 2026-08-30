import pandas as pd
import numpy as np
import logging


class AnomalyDetector:

    HIGH_ACTIVITY_MULTIPLIER = 2.0
    DROP_ACTIVITY_MULTIPLIER = 0.5
    SPIKE_MULTIPLIER = 3.0

    def __init__(self, file_path=None, dataframe=None):

        self.file_path = file_path
        self.df = dataframe

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

        self.logger = logging.getLogger(__name__)

    def load_data(self):

        if self.df is not None:
            self.logger.info(
                f"Using provided DataFrame with {len(self.df)} rows"
            )
            return self.df

        if self.file_path is None:
            raise ValueError(
                "Either file_path or dataframe must be provided."
            )

        self.df = pd.read_csv(self.file_path)

        self.logger.info(
            f"Loaded {len(self.df)} rows from {self.file_path}"
        )

        return self.df

    def calculate_baseline(self):

    required_columns = [
        "date",
        "grid_id",
        "hour",
        "total_activity"
    ]

    missing_columns = [
        col for col in required_columns
        if col not in self.df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    # Check that every grid/day has 24 distinct hourly intervals
    hour_counts = (
        self.df
        .groupby(["date", "grid_id"])["hour"]
        .nunique()
    )

    invalid_groups = hour_counts[hour_counts != 24]

    if not invalid_groups.empty:
        self.logger.warning(
            f"{len(invalid_groups)} grid/day groups "
            "do not contain exactly 24 distinct hours."
        )

    def leave_one_out_median(values):

        n = values.shape[0]

        result = np.full(n, np.nan)

        if n < 2:
            return result

        # Sort the values
        order = np.argsort(values,kind="mergesort")

        sorted_vals = values[order]

        # rank[i] = position of values[i]
        # in the sorted array
        rank = np.empty(
            n,
            dtype=np.int64
        )

        rank[order] = np.arange(n)

        # Number of values after removing
        # the current observation
        m = n - 1

        if m % 2 == 1:

            # Odd number of remaining values
            r = m // 2

            pos = np.where(
                rank <= r,
                r + 1,
                r
            )

            result = sorted_vals[pos]

        else:

            # Even number of remaining values
            r1 = m // 2 - 1
            r2 = m // 2

            pos1 = np.where(
                rank <= r1,
                r1 + 1,
                r1
            )

            pos2 = np.where(
                rank <= r2,
                r2 + 1,
                r2
            )

            result = (
                sorted_vals[pos1]
                + sorted_vals[pos2]
            ) / 2.0

        return result

        # Calculate the baseline independently
        # for every grid/day group
        self.df["baseline_activity"] = (
            self.df
            .groupby(["date", "grid_id"])["total_activity"]
            .transform(
                lambda values:
                leave_one_out_median(
                    values.to_numpy()
                )
            )
        )

        self.logger.info(
            "Calculated within-day leave-one-out "
            "median baselines."
        )

        return self.df