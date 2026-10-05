from sqlalchemy import Column, Integer, BigInteger, Float, DateTime, Date, Text, ForeignKey
from database import Base


class DimGrid(Base):
    __tablename__ = "dim_grid"

    grid_id = Column(BigInteger, primary_key=True)
    centroid_latitude = Column(Float)
    centroid_longitude = Column(Float)


class DimTime(Base):
    __tablename__ = "dim_time"

    time_key = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    date = Column(Date)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    hour = Column(Integer)
    day_of_week = Column(Integer)
    day_name = Column(Text)


class FactNetworkActivity(Base):
    __tablename__ = "fact_network_activity"

    time_key = Column(Integer, primary_key=True)
    grid_id = Column(Integer)

    sms_in = Column(Float)
    sms_out = Column(Float)
    call_in = Column(Float)
    call_out = Column(Float)
    internet_activity = Column(Float)

    total_sms = Column(Float)
    total_calls = Column(Float)
    total_activity = Column(Float)


class GridFeatures(Base):
    __tablename__ = "grid_features"

    grid_id = Column(BigInteger, primary_key=True)
    avg_activity = Column(Float)
    activity_growth = Column(Float)
    active_hours = Column(Float)
    peak_ratio = Column(Float)
    variability = Column(Float)
    internet_share = Column(Float)
    timestamp = Column(DateTime, primary_key=True)

class GridDailySummary(Base):
    __tablename__ = "grid_daily_summary"

    grid_id = Column(Integer, ForeignKey("dim_grid.grid_id"), primary_key=True)
    day = Column(Date, primary_key=True)
    total_activity = Column(Float, nullable=False)

class NetworkRuleAlert(Base):
    __tablename__ = "network_rule_alerts"

    grid_id = Column(BigInteger, primary_key=True)
    timestamp = Column(DateTime, primary_key=True)

    alert_type = Column(Text)
    current_activity = Column(Float)
    baseline_activity = Column(Float)
    reason = Column(Text)