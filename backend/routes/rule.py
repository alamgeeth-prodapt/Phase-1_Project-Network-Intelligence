from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text, func, case
from sqlalchemy.orm import Session

from database import get_db
from models import RuleAlert, RuleAlertResponse, AlertTimelinePoint
from schemas import NetworkRuleAlert
from .auth import get_current_user


router = APIRouter(
    prefix="/network/rule",
    tags=["Rule-based Network Analytics"]
)


@router.get("/alerts", response_model=RuleAlertResponse)
def get_rule_alerts(
    limit: int = Query(default=20, ge=1, le=100),
    as_of: Optional[datetime] = Query(default=None),
    db: Session = Depends(get_db),
    current_user: str = Depends(get_current_user)
):
    try:
        if as_of is None:
            result = db.execute(
                text(
                    "SELECT MAX(timestamp) "
                    "FROM network_rule_alerts"
                )
            )

            effective_as_of = result.scalar()

            if effective_as_of is None:
                raise HTTPException(
                    status_code=500,
                    detail="Rule-based alert data is unavailable."
                )

        else:
            effective_as_of = as_of

        query = """
            SELECT
                grid_id,
                timestamp,
                alert_type,
                current_activity,
                baseline_activity,
                reason
            FROM network_rule_alerts
            WHERE timestamp <= :as_of
            ORDER BY timestamp DESC
            LIMIT :limit
        """

        result = db.execute(
            text(query),
            {
                "as_of": effective_as_of,
                "limit": limit
            }
        )

        rows = result.mappings().all()

        data = [
            RuleAlert(
                grid_id=row["grid_id"],
                timestamp=row["timestamp"],
                alert_type=row["alert_type"],
                current_activity=float(
                    row["current_activity"] or 0
                ),
                baseline_activity=float(
                    row["baseline_activity"] or 0
                ),
                reason=row["reason"]
            )
            for row in rows
        ]

        return RuleAlertResponse(
            as_of=effective_as_of,
            data=data
        )

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                "Rule-based analytics data source "
                f"unavailable: {exc}"
            )
        )


@router.get(
    "/alerts/timeline",
    response_model=list[AlertTimelinePoint]
)
def get_alert_timeline(
    days: int = Query(
        default=7,
        ge=1,
        le=90,
        description="Number of days to include"
    ),
    db: Session = Depends(get_db),
    current_user: str = Depends(get_current_user),
):
    latest_timestamp = (
        db.query(
            func.max(NetworkRuleAlert.timestamp)
        )
        .scalar()
    )

    if latest_timestamp is None:
        raise HTTPException(
            status_code=404,
            detail="No rule-based alert data available."
        )

    start_date = latest_timestamp.date() - timedelta(days=days - 1)
    start_timestamp = datetime.combine(start_date, datetime.min.time())

    rows = (
        db.query(
            func.date(
                NetworkRuleAlert.timestamp
            ).label("date"),

            func.sum(
                case(
                    (
                        NetworkRuleAlert.alert_type
                        == "HIGH_ACTIVITY",
                        1
                    ),
                    else_=0
                )
            ).label("critical"),

            func.sum(
                case(
                    (
                        NetworkRuleAlert.alert_type.in_(
                            ["DROP", "SPIKE"]
                        ),
                        1
                    ),
                    else_=0
                )
            ).label("warn"),
        )
        .filter(
            NetworkRuleAlert.timestamp >= start_timestamp,
            NetworkRuleAlert.timestamp <= latest_timestamp,
        )
        .group_by(
            func.date(NetworkRuleAlert.timestamp)
        )
        .order_by(
            func.date(NetworkRuleAlert.timestamp)
        )
        .all()
    )

    return [
        AlertTimelinePoint(
            date=row.date,
            critical=int(row.critical or 0),
            warn=int(row.warn or 0),
        )
        for row in rows
    ]


@router.get("/hotspots", response_model=RuleAlertResponse)
def get_rule_hotspots(
    limit: int = Query(default=20, ge=1, le=100),
    as_of: Optional[datetime] = Query(default=None),
    db: Session = Depends(get_db),
    current_user: str = Depends(get_current_user)
):
    try:
        if as_of is None:
            result = db.execute(
                text(
                    """
                    SELECT MAX(timestamp)
                    FROM network_rule_alerts
                    WHERE alert_type = 'HIGH_ACTIVITY'
                    """
                )
            )

            effective_as_of = result.scalar()

            if effective_as_of is None:
                raise HTTPException(
                    status_code=500,
                    detail="Rule-based hotspot data is unavailable."
                )

        else:
            effective_as_of = as_of

        query = """
            SELECT
                grid_id,
                timestamp,
                alert_type,
                current_activity,
                baseline_activity,
                reason
            FROM network_rule_alerts
            WHERE alert_type = 'HIGH_ACTIVITY'
              AND timestamp <= :as_of
            ORDER BY current_activity DESC
            LIMIT :limit
        """

        result = db.execute(
            text(query),
            {
                "as_of": effective_as_of,
                "limit": limit
            }
        )

        rows = result.mappings().all()

        data = [
            RuleAlert(
                grid_id=row["grid_id"],
                timestamp=row["timestamp"],
                alert_type=row["alert_type"],
                current_activity=float(
                    row["current_activity"] or 0
                ),
                baseline_activity=float(
                    row["baseline_activity"] or 0
                ),
                reason=row["reason"]
            )
            for row in rows
        ]

        return RuleAlertResponse(
            as_of=effective_as_of,
            data=data
        )

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                "Rule-based hotspot data source "
                f"unavailable: {exc}"
            )
        )