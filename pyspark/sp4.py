"""
SP4 — Joins at scale: enrich telecom activity with Milan grid geometry.

Pipeline:
    1. Load + inspect milano-grid.geojson
    2. Build a grid_id -> geometry lookup (grid_id sourced from properties.cellId)
    3. Broadcast-join it onto the cleaned/aggregated activity data on grid_id
    4. Validate the join numerically AND geographically
    5. Produce grid_activity_geo_df + a coverage report + top-activity grids with geometry

Depends on sp2.py's SparkCleaning output (clean_network_df / hourly_grid_summary),
which already use canonical column names (grid_id, timestamp, sms_in, sms_out,
call_in, call_out, internet, total_activity, ...).
"""

import json
import math
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    col,
    broadcast,
    countDistinct,
    lit,
    sum as spark_sum,
    desc,
)


# =====================================================================
# GEOMETRY HELPERS (plain Python — runs once on the driver, not on
# Spark executors, since the grid geojson is small: 10,000 features)
# =====================================================================

def polygon_centroid(exterior_ring):
    """
    Shoelace-formula centroid of a single polygon ring.

    exterior_ring: list of [lon, lat] pairs (GeoJSON coordinate order is
    [longitude, latitude], NOT [lat, lon] — this trips a lot of people up).

    Returns (centroid_lon, centroid_lat).

    NOTE: this only uses the exterior ring (ignores holes) and, for a
    MultiPolygon, the caller should pass the first polygon's exterior
    ring — sufficient for the small, near-square Milan grid cells used
    here, where holes/multi-parts don't occur in practice.
    """
    area_acc = 0.0
    cx_acc = 0.0
    cy_acc = 0.0

    n = len(exterior_ring)
    for i in range(n - 1):  # GeoJSON rings repeat the first point as the last
        x0, y0 = exterior_ring[i]
        x1, y1 = exterior_ring[i + 1]
        cross = (x0 * y1) - (x1 * y0)
        area_acc += cross
        cx_acc += (x0 + x1) * cross
        cy_acc += (y0 + y1) * cross

    area = area_acc / 2.0

    if area == 0:
        # Degenerate polygon (all points collinear/identical) — fall back
        # to a simple average of vertices rather than dividing by zero.
        xs = [p[0] for p in exterior_ring]
        ys = [p[1] for p in exterior_ring]
        return (sum(xs) / len(xs), sum(ys) / len(ys))

    cx = cx_acc / (6.0 * area)
    cy = cy_acc / (6.0 * area)
    return (cx, cy)


def geometry_centroid(geometry):
    """
    Compute a centroid for a GeoJSON geometry dict.
    Handles Polygon and MultiPolygon (uses the first polygon for
    MultiPolygon, which is fine for near-uniform grid cells).
    """
    geom_type = geometry["type"]

    if geom_type == "Polygon":
        exterior_ring = geometry["coordinates"][0]
        return polygon_centroid(exterior_ring)

    if geom_type == "MultiPolygon":
        exterior_ring = geometry["coordinates"][0][0]
        return polygon_centroid(exterior_ring)

    raise ValueError(f"Unsupported geometry type for centroid: {geom_type}")


def haversine_meters(lon1, lat1, lon2, lat2):
    """Great-circle distance between two lon/lat points, in meters."""
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(d_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


# =====================================================================
# GEO ENRICHMENT PIPELINE
# =====================================================================

class GeoEnrichment:

    def __init__(self, spark, geojson_path):
        self.spark = spark
        self.geojson_path = geojson_path

        self.raw_geojson = None
        self.grid_lookup_df = None

        self.coverage_report = {}
        self.unmatched_grid_ids = []

    # -----------------------------------------------------------------
    # 1. LOAD + INSPECT milano-grid.geojson
    # -----------------------------------------------------------------

    def load_and_inspect_geojson(self):

        with open(self.geojson_path, "r", encoding="utf-8") as f:
            self.raw_geojson = json.load(f)

        top_level_type = self.raw_geojson.get("type")
        features = self.raw_geojson.get("features", [])

        assert top_level_type == "FeatureCollection", (
            f"Expected top-level type 'FeatureCollection', got {top_level_type!r}"
        )
        assert len(features) > 0, "GeoJSON has no features."

        sample_feature = features[0]
        sample_geometry_type = sample_feature["geometry"]["type"]

        print("\n--- GEOJSON STRUCTURE ---")
        print(f"Top-level type      : {top_level_type}")
        print(f"Feature count        : {len(features)}")
        print(f"Grid id stored at    : properties.cellId")
        print(f"Sample geometry type : {sample_geometry_type}")
        print(f"Sample properties    : {sample_feature['properties']}")

        return self.raw_geojson

    # -----------------------------------------------------------------
    # 2 & 3. IDENTIFY COMMON KEY + NORMALIZE INTO A grid_id LOOKUP
    # -----------------------------------------------------------------

    def build_grid_lookup_df(self):
        """
        Flattens features[] into a lookup keyed by grid_id.

        ACCEPTANCE CRITERION: the lookup is built from properties.cellId.
        --> grid_id is explicitly read from feature["properties"]["cellId"]
            below (NOT from feature["id"] or any other field), which is
            the common key with the activity dataset's grid_id column.
        """

        assert self.raw_geojson is not None, (
            "Call load_and_inspect_geojson() before build_grid_lookup_df()."
        )

        rows = []

        for feature in self.raw_geojson["features"]:

            # ACCEPTANCE CRITERION: grid_id sourced from properties.cellId.
            grid_id = feature["properties"]["cellId"]

            geometry = feature["geometry"]
            centroid_lon, centroid_lat = geometry_centroid(geometry)

            rows.append(
                Row(
                    grid_id=int(grid_id),
                    geometry_type=geometry["type"],
                    # Keep the full geometry as GeoJSON text so the
                    # enriched output is directly usable by any GeoJSON-
                    # aware mapping tool downstream (Steps 9-10).
                    geometry=json.dumps(geometry),
                    centroid_lon=float(centroid_lon),
                    centroid_lat=float(centroid_lat),
                )
            )

        self.grid_lookup_df = self.spark.createDataFrame(rows)

        lookup_count = self.grid_lookup_df.count()

        print("\n--- GRID LOOKUP BUILT ---")
        print(f"Lookup rows (grid cells): {lookup_count}")

        return self.grid_lookup_df

    # -----------------------------------------------------------------
    # 4. SIZE COMPARISON: LOOKUP VS ACTIVITY DATAFRAME
    # -----------------------------------------------------------------

    def compare_sizes(self, activity_df):

        lookup_count = self.grid_lookup_df.count()
        activity_count = activity_df.count()

        print("\n--- SIZE COMPARISON ---")
        print(f"Grid lookup rows   : {lookup_count:,}")
        print(f"Activity df rows   : {activity_count:,}")
        print(
            f"Ratio              : activity is "
            f"~{activity_count / max(lookup_count, 1):,.0f}x the lookup size"
        )

        return lookup_count, activity_count

    # -----------------------------------------------------------------
    # 5. LEFT JOIN activity_df TO grid_lookup_df ON grid_id
    # -----------------------------------------------------------------

    def join_activity_to_grid(self, activity_df):
        """
        LEFT join (not inner): an inner join would silently DROP any
        activity rows whose grid_id has no matching geometry, which
        would hide a data-quality problem rather than surface it. A
        left join keeps every activity row and lets missing geometry
        show up explicitly as a null — which is exactly what
        validate_join() below checks for.
        """

        assert self.grid_lookup_df is not None, (
            "Call build_grid_lookup_df() before join_activity_to_grid()."
        )

        pre_join_row_count = activity_df.count()

        # Broadcast the small grid lookup (10k rows) to every executor so
        # the join needs no shuffle of the (much larger) activity data.
        # See explain_join_plans() below for the concrete execution-plan
        # comparison and the size-asymmetry rationale.
        joined_df = activity_df.join(
            broadcast(self.grid_lookup_df),
            on="grid_id",
            how="left",
        )

        post_join_row_count = joined_df.count()

        # ACCEPTANCE CRITERION: row count after the left join must equal
        # row count before it. If this fails, the lookup has duplicate
        # grid_id keys (fanning the join out into extra rows).
        assert post_join_row_count == pre_join_row_count, (
            f"Row count changed after left join: "
            f"before={pre_join_row_count}, after={post_join_row_count}. "
            f"This means grid_lookup_df has duplicate grid_id keys — "
            f"check for duplicate cellId values in the source GeoJSON."
        )

        print("\n--- JOIN ROW COUNT CHECK ---")
        print(f"Before join: {pre_join_row_count:,}")
        print(f"After join : {post_join_row_count:,}")
        print("PASS: row count preserved by the left join")

        return joined_df

    # -----------------------------------------------------------------
    # 6. VALIDATE THE JOIN NUMERICALLY
    # -----------------------------------------------------------------

    def validate_join(self, activity_df, joined_df):

        distinct_grids_before = (
            activity_df.select("grid_id").distinct().count()
        )

        distinct_grids_after = (
            joined_df.filter(col("geometry").isNotNull())
            .select("grid_id")
            .distinct()
            .count()
        )

        missing_geometry_count = (
            joined_df.filter(col("geometry").isNull())
            .select("grid_id")
            .distinct()
            .count()
        )

        coverage_pct = (
            100.0 * distinct_grids_after / distinct_grids_before
            if distinct_grids_before > 0 else 0.0
        )

        self.coverage_report = {
            "distinct_grids_before_join": distinct_grids_before,
            "distinct_grids_enriched": distinct_grids_after,
            "distinct_grids_missing_geometry": missing_geometry_count,
            "coverage_percent": round(coverage_pct, 4),
        }

        unmatched_rows = (
            joined_df.filter(col("geometry").isNull())
            .select("grid_id")
            .distinct()
            .orderBy("grid_id")
            .collect()
        )
        self.unmatched_grid_ids = [r["grid_id"] for r in unmatched_rows]

        print("\n--- GRID ENRICHMENT COVERAGE REPORT ---")
        for k, v in self.coverage_report.items():
            print(f"{k}: {v}")

        print(f"\nUnmatched grid_id count: {len(self.unmatched_grid_ids)}")
        if self.unmatched_grid_ids:
            print(f"Unmatched grid_id sample: {self.unmatched_grid_ids[:20]}")

        # ACCEPTANCE CRITERIA
        assert coverage_pct == 100.0, (
            f"Enrichment coverage is {coverage_pct:.4f}%, not 100%. "
            f"{missing_geometry_count} grid_id(s) have no matching geometry."
        )
        print("PASS: enrichment coverage is 100%")

        assert len(self.unmatched_grid_ids) == 0, (
            f"Unmatched grid_id list is not empty: {self.unmatched_grid_ids}"
        )
        print("PASS: unmatched-grid list is empty")

        return self.coverage_report, self.unmatched_grid_ids

    # -----------------------------------------------------------------
    # 7. VALIDATE THE JOIN GEOGRAPHICALLY (not just numerically)
    # -----------------------------------------------------------------

    def geographic_spot_check(
        self,
        named_grid_id,
        named_grid_label,
        expected_lon_range,
        expected_lat_range,
        adjacent_grid_id_a=1,
        adjacent_grid_id_b=2,
    ):
        """
        A 100% coverage number only proves every grid_id got SOME
        geometry attached — it says nothing about whether that geometry
        is actually correct. Two independent geographic sanity checks:

          (a) a named, recognizable grid cell's centroid falls inside
              the expected lon/lat bounding box for that part of Milan.
          (b) grid_id 1 and grid_id 2's centroids are close to each
              other (adjacent grid cells) — not identical (which would
              mean the join fanned out or geometry got duplicated) and
              not far apart (which would mean grid_id numbering doesn't
              correspond to spatial adjacency, or geometry got
              mismatched during the cellId -> grid_id mapping).
        """

        named_row = (
            self.grid_lookup_df
            .filter(col("grid_id") == named_grid_id)
            .select("centroid_lon", "centroid_lat")
            .collect()
        )
        assert len(named_row) == 1, (
            f"grid_id {named_grid_id} not found in lookup for spot-check."
        )
        named_lon = named_row[0]["centroid_lon"]
        named_lat = named_row[0]["centroid_lat"]

        lon_ok = expected_lon_range[0] <= named_lon <= expected_lon_range[1]
        lat_ok = expected_lat_range[0] <= named_lat <= expected_lat_range[1]

        print("\n--- GEOGRAPHIC SPOT-CHECK ---")
        print(
            f"{named_grid_label} (grid_id={named_grid_id}) centroid: "
            f"lon={named_lon:.6f}, lat={named_lat:.6f}"
        )
        assert lon_ok and lat_ok, (
            f"Centroid for {named_grid_label} (grid_id={named_grid_id}) "
            f"at ({named_lon}, {named_lat}) falls outside the expected "
            f"Milan bounding box lon={expected_lon_range}, "
            f"lat={expected_lat_range}."
        )
        print(f"PASS: centroid lands within expected bounding box for {named_grid_label}")

        # Adjacency check between two grid ids.
        pair_rows = (
            self.grid_lookup_df
            .filter(col("grid_id").isin([adjacent_grid_id_a, adjacent_grid_id_b]))
            .select("grid_id", "centroid_lon", "centroid_lat")
            .collect()
        )
        assert len(pair_rows) == 2, (
            f"Expected both grid_id {adjacent_grid_id_a} and "
            f"{adjacent_grid_id_b} in lookup for adjacency check."
        )
        by_id = {r["grid_id"]: r for r in pair_rows}
        a = by_id[adjacent_grid_id_a]
        b = by_id[adjacent_grid_id_b]

        distance_m = haversine_meters(
            a["centroid_lon"], a["centroid_lat"],
            b["centroid_lon"], b["centroid_lat"],
        )

        print(
            f"Distance between grid_id {adjacent_grid_id_a} and "
            f"{adjacent_grid_id_b} centroids: {distance_m:.1f} m"
        )

        # Not identical (< 5m would suggest duplicated/misassigned
        # geometry) and not far apart (> 1000m would suggest grid_id
        # numbering isn't spatially adjacent for this dataset).
        MIN_ADJACENT_M = 5.0
        MAX_ADJACENT_M = 1000.0

        assert MIN_ADJACENT_M < distance_m < MAX_ADJACENT_M, (
            f"grid_id {adjacent_grid_id_a} and {adjacent_grid_id_b} "
            f"centroids are {distance_m:.1f}m apart — expected an "
            f"adjacent-cell distance between {MIN_ADJACENT_M}m and "
            f"{MAX_ADJACENT_M}m. Check the cellId -> grid_id mapping."
        )
        print(
            f"PASS: grid_id {adjacent_grid_id_a} and {adjacent_grid_id_b} "
            f"are adjacent (neither identical nor far apart)"
        )

    # -----------------------------------------------------------------
    # 8. COMPARE STANDARD JOIN VS BROADCAST JOIN EXECUTION PLANS
    # -----------------------------------------------------------------

    def explain_join_plans(self, activity_df):

        print("\n--- STANDARD (SORT-MERGE) JOIN PLAN ---")
        standard_joined = activity_df.join(
            self.grid_lookup_df, on="grid_id", how="left"
        )
        standard_joined.explain(mode="formatted")

        print("\n--- BROADCAST JOIN PLAN ---")
        broadcast_joined = activity_df.join(
            broadcast(self.grid_lookup_df), on="grid_id", how="left"
        )
        broadcast_joined.explain(mode="formatted")

    # -----------------------------------------------------------------
    # 9. CREATE THE ENRICHED DATASET
    # -----------------------------------------------------------------

    def create_enriched_dataset(self, joined_df):
        """
        grid_activity_geo_df — matches the Core Dataset Contract
        (grid_id, timestamp) plus the activity measures and geometry.
        """

        grid_activity_geo_df = joined_df.select(
            col("timestamp"),
            col("grid_id"),
            col("sms_in"),
            col("sms_out"),
            col("call_in"),
            col("call_out"),
            col("internet_activity"),
            col("total_activity"),
            col("geometry"),
        )

        return grid_activity_geo_df

    # -----------------------------------------------------------------
    # 10. TOP HIGH-ACTIVITY GRIDS, WITH GEOMETRY RETAINED
    # -----------------------------------------------------------------

    def top_high_activity_grids_with_geo(
        self,
        grid_activity_geo_df,
        top_n=10,
        start_timestamp=None,
        end_timestamp=None,
    ):

        df = grid_activity_geo_df

        if start_timestamp is not None:
            df = df.filter(col("timestamp") >= lit(start_timestamp))
        if end_timestamp is not None:
            df = df.filter(col("timestamp") <= lit(end_timestamp))

        top_grids = (
            df.groupBy("grid_id", "geometry")
            .agg(spark_sum("total_activity").alias("window_total_activity"))
            .orderBy(desc("window_total_activity"))
            .limit(top_n)
        )

        print(f"\n--- TOP {top_n} HIGH-ACTIVITY GRIDS (with geometry) ---")
        top_grids.select("grid_id", "window_total_activity").show(top_n, truncate=False)

        return top_grids

    # -----------------------------------------------------------------
    # 11. (OPTIONAL) CENTROIDS — already computed in build_grid_lookup_df
    # -----------------------------------------------------------------

    def grid_centroids_df(self):
        """
        Returns a lightweight grid_id + centroid_lon + centroid_lat
        DataFrame, useful for simple map plotting or API responses
        where you don't want to ship full polygon geometry.
        """
        return self.grid_lookup_df.select(
            "grid_id", "centroid_lon", "centroid_lat"
        )


# =====================================================================
# MAIN — wire this up to your sp2/sp3 outputs
# =====================================================================

if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("SP4_GeoEnrichment")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    # -------------------------------------------------------------
    # Bring in the already-cleaned activity data from SP2/SP3.
    # If clean_network_df / hourly_grid_summary aren't already in
    # scope from an earlier stage in the same run, load them here,
    # e.g. from the parquet SP2 wrote out:
    #
    #   activity_df = spark.read.parquet(
    #       r"D:\phase_1_project\pyspark\results\clean_network"
    #   )
    #
    # For the aggregated (one row per grid+hour) version used below,
    # substitute hourly_grid_summary from sp2_sp3.py's run() output,
    # renaming its "internet_activity" column if needed to match the
    # Core Dataset Contract column names used throughout this file.
    # -------------------------------------------------------------

    activity_df = spark.read.parquet(
        r"D:\phase_1_project\pyspark\results\clean_network"
    )

    GEOJSON_PATH = r"D:\phase_1_project\pyspark\data_set\milano-grid.geojson"

    geo = GeoEnrichment(spark, GEOJSON_PATH)

    # 1-3: load, inspect, build lookup from properties.cellId
    geo.load_and_inspect_geojson()
    geo.build_grid_lookup_df()

    # 4: size comparison
    geo.compare_sizes(activity_df)

    # 5: left join
    joined_df = geo.join_activity_to_grid(activity_df)

    # 6: numeric validation (coverage, unmatched grids)
    geo.validate_join(activity_df, joined_df)

    # 7: geographic validation
    # NOTE: adjust expected_lon_range / expected_lat_range and the
    # named grid_id/label below to a cell you can independently verify
    # (e.g. Milan city center, ~lon 9.19, lat 45.46).
    geo.geographic_spot_check(
        named_grid_id=5060,
        named_grid_label="Milan city center area",
        expected_lon_range=(9.15, 9.23),
        expected_lat_range=(45.44, 45.48),
        adjacent_grid_id_a=1,
        adjacent_grid_id_b=2,
    )

    # 8: execution plan comparison
    geo.explain_join_plans(activity_df)

    # 9: enriched dataset
    grid_activity_geo_df = geo.create_enriched_dataset(joined_df)
    grid_activity_geo_df.show(5, truncate=False)

    # 10: top high-activity grids with geometry
    geo.top_high_activity_grids_with_geo(grid_activity_geo_df, top_n=10)

    spark.stop()