from datetime import date, timedelta, datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, text
from sqlalchemy.orm import Session
from .auth import get_current_user
from database import get_db
from models import (
    NetworkSummaryResponse,
    GridActivityResponse,
    GridActivityPoint,
    GridFeaturesResponse,
    GridLocationResponse,
    HourlyProfilePoint,
    DayOfWeekProfilePoint,
    TrafficMixPoint,
    TopGridResponse,
    AlertTimelinePoint,
    FeatureMetric,
    FeatureSummaryResponse,
    FeatureAllResponse,
)
from database import DaySummary
from schemas import (
    DimGrid,
    FactNetworkActivity,
    DimTime,
    GridDailySummary,
    GridFeatures,
)
router = APIRouter(
    prefix="/network",
    tags=["Network"]
)

@router.get(
    "/summary",
    response_model=list[NetworkSummaryResponse]
)
def get_network_summary(
    summary_date: date | None = Query(
        default=None,
        description="Calculate summary up to this timestamp"
    ),
    db: Session = Depends(get_db),
    current_user: str = Depends(get_current_user)
):

    if summary_date is not None:

        summary = (
            db.query(
                DaySummary
            ).filter(
                DaySummary.summary_date == summary_date
            ).first()
        )

        if summary is None:
            raise HTTPException(404)

        return summary

    summary = db.query(DaySummary).order_by(DaySummary.summary_date).all()

    if summary is None:
        raise HTTPException(404)

    return summary

@router.get(
    "/summary/hourly-profile",
    response_model=list[HourlyProfilePoint]
)
def get_hourly_profile(
    date: date | None = Query(
        default=None,
        description="Optional date in YYYY-MM-DD format"
    ),
    db: Session = Depends(get_db),
    current_user: str = Depends(get_current_user),
):
    query = (
        db.query(
            DimTime.hour.label("hour"),
            func.avg(FactNetworkActivity.total_activity).label("avg_activity"),
        )
        .join(
            FactNetworkActivity,
            FactNetworkActivity.time_key == DimTime.time_key
        )
    )

    if date is not None:
        query = query.filter(DimTime.date == date)

    rows = (
        query
        .group_by(DimTime.hour)
        .order_by(DimTime.hour)
        .all()
    )

    return [
        HourlyProfilePoint(
            hour=row.hour,
            avg_activity=float(row.avg_activity or 0),
        )
        for row in rows
    ]


@router.get(
    "/summary/day-of-week-profile",
    response_model=list[DayOfWeekProfilePoint]
)
def get_day_of_week_profile(
    db: Session = Depends(get_db),
    current_user: str = Depends(get_current_user),
):
    rows = (
        db.query(
            DimTime.day_of_week.label("day_of_week"),
            DimTime.day_name.label("label"),
            func.avg(
                FactNetworkActivity.total_activity
            ).label("avg_activity"),
        )
        .join(
            FactNetworkActivity,
            FactNetworkActivity.time_key == DimTime.time_key
        )
        .group_by(
            DimTime.day_of_week,
            DimTime.day_name,
        )
        .order_by(DimTime.day_of_week)
        .all()
    )

    return [
        DayOfWeekProfilePoint(
            day_of_week=row.day_of_week,
            label=row.label,
            avg_activity=float(row.avg_activity or 0),
        )
        for row in rows
    ]

@router.get(
    "/summary/traffic-mix",
    response_model=list[TrafficMixPoint]
)
def get_traffic_mix(
    days: int = Query(
        default=14,
        ge=1,
        le=90,
        description="Number of days to include"
    ),
    db: Session = Depends(get_db),
    current_user: str = Depends(get_current_user),
):
    latest_date = (
        db.query(func.max(DimTime.date))
        .join(
            FactNetworkActivity,
            FactNetworkActivity.time_key == DimTime.time_key
        )
        .scalar()
    )

    if latest_date is None:
        raise HTTPException(
            status_code=404,
            detail="No network activity data available."
        )

    start_date = latest_date - timedelta(days=days - 1)

    rows = (
        db.query(
            DimTime.date.label("summary_date"),
            func.sum(
                FactNetworkActivity.total_calls
            ).label("voice_activity"),
            func.sum(
                FactNetworkActivity.total_sms
            ).label("sms_activity"),
            func.sum(
                FactNetworkActivity.internet_activity
            ).label("internet_activity"),
        )
        .join(
            FactNetworkActivity,
            FactNetworkActivity.time_key == DimTime.time_key
        )
        .filter(
            DimTime.date >= start_date,
            DimTime.date <= latest_date,
        )
        .group_by(DimTime.date)
        .order_by(DimTime.date)
        .all()
    )

    return [
        TrafficMixPoint(
            summary_date=row.summary_date,
            voice_activity=float(row.voice_activity or 0),
            sms_activity=float(row.sms_activity or 0),
            internet_activity=float(row.internet_activity or 0),
        )
        for row in rows
    ]

@router.get("/grids",response_model=list[GridLocationResponse])
def get_grids(date: str | None = Query(default=None,), db: Session = Depends(get_db), current_user: str = Depends(get_current_user)):

    if date is not None:
        activity_subq = (
            db.query(
                GridDailySummary.grid_id.label("grid_id"),
                GridDailySummary.total_activity.label("activity"),
            )
            .filter(GridDailySummary.day == date)
            .subquery()
        )
    else:
        activity_subq = (
            db.query(
                GridDailySummary.grid_id.label("grid_id"),
                func.sum(GridDailySummary.total_activity).label("activity"),
            )
            .group_by(GridDailySummary.grid_id)
            .subquery()
        )

    rows = (
        db.query(
            DimGrid.grid_id,
            DimGrid.centroid_latitude,
            DimGrid.centroid_longitude,
            func.coalesce(activity_subq.c.activity, 0).label("activity"),
        )
        .outerjoin(activity_subq, DimGrid.grid_id == activity_subq.c.grid_id)
        .order_by(DimGrid.grid_id)
        .all()
    )

    if not rows:
        raise HTTPException(404)

    return [
        GridLocationResponse(
            grid_id=row.grid_id,
            centroid_lat=row.centroid_latitude,
            centroid_lon=row.centroid_longitude,
            activity=float(row.activity),
        )
        for row in rows
    ]
@router.get(
    "/grids/top",
    response_model=list[TopGridResponse]
)
def get_top_grids(
    date: date | None = Query(
        default=None,
        description="Optional date in YYYY-MM-DD format"
    ),
    limit: int = Query(
        default=10,
        ge=1,
        le=100
    ),
    db: Session = Depends(get_db),
    current_user: str = Depends(get_current_user),
):
    if date is None:
        latest_date = (
            db.query(func.max(GridDailySummary.day))
            .scalar()
        )

        if latest_date is None:
            raise HTTPException(
                status_code=404,
                detail="No grid activity data available."
            )

        effective_date = latest_date

    else:
        effective_date = date

    rows = (
        db.query(
            GridDailySummary.grid_id,
            GridDailySummary.total_activity.label("activity"),
            DimGrid.centroid_latitude,
            DimGrid.centroid_longitude,
        )
        .join(
            DimGrid,
            DimGrid.grid_id == GridDailySummary.grid_id
        )
        .filter(
            GridDailySummary.day == effective_date
        )
        .order_by(
            GridDailySummary.total_activity.desc()
        )
        .limit(limit)
        .all()
    )

    return [
        TopGridResponse(
            grid_id=row.grid_id,
            activity=float(row.activity or 0),
            centroid_lat=float(row.centroid_latitude),
            centroid_lon=float(row.centroid_longitude),
        )
        for row in rows
    ]

@router.get(
    "/grid/{grid_id}",
    response_model=GridActivityResponse
)
def get_grid_activity(
    grid_id: int,

    date: str | None = Query(
        default=None,
        description="Optional date filter in YYYY-MM-DD format"
    ),

    hour: int | None = Query(
        default=None,
        ge=0,
        le=23,
        description="Optional hour filter from 0 to 23"
    ),

    as_of: datetime | None = Query(
        default=None,
        description="End timestamp for the requested window"
    ),

    db: Session = Depends(get_db), current_user: str = Depends(get_current_user)
):
    try:

        if grid_id < 1 or grid_id > 10000:
            raise HTTPException(
                status_code=404,
                detail=f"Grid {grid_id} not found."
            )

 
        if as_of is None:

            result = db.execute(
                text("""
                    SELECT MAX(dt.timestamp)
                    FROM fact_network_activity f
                    JOIN dim_time dt
                        ON f.time_key = dt.time_key
                """)
            )

            effective_as_of = result.scalar()

            if effective_as_of is None:
                raise HTTPException(
                    status_code=500,
                    detail="Analytics warehouse contains no activity data."
                )

        else:
            effective_as_of = as_of

        window_start = effective_as_of - timedelta(hours=23)

     
        query = """
            SELECT
                dt.timestamp,
                f.total_activity,
                f.total_sms,
                f.total_calls,
                f.internet_activity
            FROM fact_network_activity f
            JOIN dim_time dt
                ON f.time_key = dt.time_key
            WHERE f.grid_id = :grid_id
              AND dt.timestamp BETWEEN :window_start AND :as_of
        """

        params = {
            "grid_id": grid_id,
            "window_start": window_start,
            "as_of": effective_as_of
        }

     
        if date is not None:
            query += """
                AND DATE(dt.timestamp) = :date
            """
            params["date"] = date

        if hour is not None:
            query += """
                AND dt.hour = :hour
            """
            params["hour"] = hour

        query += """
            ORDER BY dt.timestamp ASC
        """

        result = db.execute(
            text(query),
            params
        )

        rows = result.mappings().all()

        if not rows:
            # A valid grid can legitimately have no data in a
            # requested window, so verify that the grid exists.
            grid_exists = db.execute(
                text("""
                    SELECT 1
                    FROM dim_grid
                    WHERE grid_id = :grid_id
                    LIMIT 1
                """),
                {"grid_id": grid_id}
            ).first()

            if grid_exists is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Grid {grid_id} not found."
                )

       
        data = [
            GridActivityPoint(
                timestamp=row["timestamp"],
                total_activity=float(row["total_activity"] or 0),
                total_sms=float(row["total_sms"] or 0),
                total_calls=float(row["total_calls"] or 0),
                internet_activity=float(
                    row["internet_activity"] or 0
                )
            )
            for row in rows
        ]

        return GridActivityResponse(
            grid_id=grid_id,
            as_of=effective_as_of,
            data=data
        )

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Network analytics data source unavailable: {exc}"
        )

@router.get(
    "/grid/{grid_id}/features",
    response_model=GridFeaturesResponse,
)
def get_grid_features(
    grid_id: int,
    as_of: datetime | None = Query(
        None,
        description="Feature timestamp. Defaults to the latest stored feature timestamp."
    ),
    db=Depends(get_db),current_user: str = Depends(get_current_user)
):

    # ---------------------------------------------------------
    # 1. Validate grid
    # ---------------------------------------------------------

    if grid_id < 1 or grid_id > 10000:
        raise HTTPException(
            status_code=404,
            detail=f"Grid {grid_id} not found",
        )

    # ---------------------------------------------------------
    # 2. Determine timestamp
    # ---------------------------------------------------------

    if as_of is None:

        latest_query = text("""
            SELECT MAX(timestamp) AS latest_timestamp
            FROM grid_features
            WHERE grid_id = :grid_id
        """)

        latest = db.execute(
            latest_query,
            {"grid_id": grid_id},
        ).mappings().first()

        if latest is None or latest["latest_timestamp"] is None:
            raise HTTPException(
                status_code=404,
                detail=f"No features found for grid {grid_id}",
            )

        effective_timestamp = latest["latest_timestamp"]

    else:
        effective_timestamp = as_of

    # ---------------------------------------------------------
    # 3. Fetch exact ML2 feature row
    # ---------------------------------------------------------

    query = text("""
    SELECT
        grid_id,
        timestamp,
        variability,
        hour,
        hour_sin,
        hour_cos,
        internet_share_at_t,
        activity_growth,
        peak_ratio,
        avg_activity,
        data_to_voice_ratio,
        active_hours,
        day_of_week,
        x,
        y,
        activity_lag_24,
        neighbor_avg_activity
    FROM grid_features
    WHERE grid_id = :grid_id
      AND timestamp = :timestamp
    LIMIT 1
""")

    row = db.execute(
        query,
        {
            "grid_id": grid_id,
            "timestamp": effective_timestamp,
        },
    ).mappings().first()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No feature record found for "
                f"grid {grid_id} at {effective_timestamp}"
            ),
        )

    # ---------------------------------------------------------
    # 4. Data-quality assessment
    # ---------------------------------------------------------

    feature_columns = {
    "grid_id": row["grid_id"],
    "timestamp": row["timestamp"],
    "variability": row["variability"],
    "hour": row["hour"],
    "hour_sin": row["hour_sin"],
    "hour_cos": row["hour_cos"],
    "internet_share_at_t": row["internet_share_at_t"], # FIXED!
    "activity_growth": row["activity_growth"],
    "peak_ratio": row["peak_ratio"],
    "avg_activity": row["avg_activity"],
    "data_to_voice_ratio": row["data_to_voice_ratio"],
    "active_hours": row["active_hours"],
    "day_of_week": row["day_of_week"],
    "x": row["x"],
    "y": row["y"],
    "activity_lag_24": row["activity_lag_24"],
    "neighbor_avg_activity": row["neighbor_avg_activity"]
}

    missing_features = [
        column
        for column in feature_columns
        if row[column] is None
    ]

    if missing_features:

        data_quality = {
            "status": "INVALID",
            "message": (
                "Missing feature values: "
                + ", ".join(missing_features)
            ),
        }

    else:

        data_quality = {
            "status": "OK",
            "message": None,
        }

    # ---------------------------------------------------------
    # 5. Feature freshness
    # ---------------------------------------------------------

    simulated_now = effective_timestamp 

    age_seconds = (simulated_now - row["timestamp"]).total_seconds()

    age_hours = max(age_seconds / 3600, 0)

    # For hourly features:
    # <= 2 hours = fresh
    # <= 6 hours = stale
    # > 6 hours = outdated

    if age_hours <= 2:
        freshness_status = "FRESH"

    elif age_hours <= 6:
        freshness_status = "STALE"

    else:
        freshness_status = "OUTDATED"

    # ---------------------------------------------------------
    # 6. Return stable contract
    # ---------------------------------------------------------

    return {
        "grid_id": row["grid_id"],

        "features" : {
            "grid_id": row["grid_id"],
            "timestamp": row["timestamp"],
            "variability": row["variability"],
            "hour": row["hour"],
            "hour_sin": row["hour_sin"],
            "hour_cos": row["hour_cos"],
            "internet_share_at_t": row["internet_share_at_t"], # FIXED!
            "activity_growth": row["activity_growth"],
            "peak_ratio": row["peak_ratio"],
            "avg_activity": row["avg_activity"],
            "data_to_voice_ratio": row["data_to_voice_ratio"],
            "active_hours": row["active_hours"],
            "day_of_week": row["day_of_week"],
            "x": row["x"],
            "y": row["y"],
            "activity_lag_24": row["activity_lag_24"],
            "neighbor_avg_activity": row["neighbor_avg_activity"]
        },

        "data_quality": data_quality,

        "feature_freshness": {
            "status": freshness_status,
            "age_hours": round(age_hours, 2),
        },
    }