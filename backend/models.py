from datetime import datetime, date
from typing import Optional, List

from pydantic import BaseModel

class NetworkSummaryResponse(BaseModel):
    summary_date: datetime
    total_activity: float
    active_grids: int
    peak_hour: int
    top_grid: int
    

class GridActivityPoint(BaseModel):
    timestamp: datetime
    total_activity: float
    total_sms: float
    total_calls: float
    internet_activity: float


class GridActivityResponse(BaseModel):
    grid_id: int
    as_of: datetime
    data: list[GridActivityPoint]

class RuleAlert(BaseModel):
    grid_id: int
    timestamp: datetime
    alert_type: str
    current_activity: float
    baseline_activity: float
    reason: str

class RuleAlertResponse(BaseModel):
    as_of: datetime
    data: List[RuleAlert]

class GridFeatureValues(BaseModel):
    avg_activity: float
    activity_growth: float
    active_hours: int
    peak_ratio: float
    variability: float
    internet_share_at_t: float
    timestamp: datetime

class admin(BaseModel):
    username: str
    password: str

class GridLocationResponse(BaseModel):
    grid_id: int
    centroid_lat: float
    centroid_lon: float
    activity: float | None = None

class FeatureDataQuality(BaseModel):
    status: str
    message: Optional[str] = None


class FeatureFreshness(BaseModel):
    status: str
    age_hours: float


class GridFeaturesResponse(BaseModel):
    grid_id: int
    features: GridFeatureValues
    data_quality: FeatureDataQuality
    feature_freshness: FeatureFreshness

class PipelineTaskStatus(BaseModel):
    status: str
    rows_in: int = 0
    rows_rejected: int = 0
    nulls_handled: int = 0
    rows_published: int = 0


class PipelineFreshness(BaseModel):
    status: str
    age_hours: float


class PipelineStatusResponse(BaseModel):
    healthy: bool

    run_id: str
    run_timestamp: datetime

    tasks: dict[str, PipelineTaskStatus]

    as_of: datetime

    freshness: PipelineFreshness

    reasons: list[str]

class HourlyProfilePoint(BaseModel):
    hour: int
    avg_activity: float


class DayOfWeekProfilePoint(BaseModel):
    day_of_week: int
    label: str
    avg_activity: float


class TrafficMixPoint(BaseModel):
    summary_date: date
    voice_activity: float
    sms_activity: float
    internet_activity: float


class TopGridResponse(BaseModel):
    grid_id: int
    activity: float
    centroid_lat: float
    centroid_lon: float


class AlertTimelinePoint(BaseModel):
    date: date
    critical: int
    warn: int


class FeatureMetric(BaseModel):
    avg: float
    min: float
    max: float


class FeatureSummaryResponse(BaseModel):
    avg_activity: FeatureMetric
    activity_growth: FeatureMetric
    active_hours: FeatureMetric
    peak_ratio: FeatureMetric
    variability: FeatureMetric
    internet_share_at_t: FeatureMetric


class FeatureAllResponse(BaseModel):
    grid_id: int
    peak_ratio: float
    variability: float
    avg_activity: float