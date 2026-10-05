import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    BigInteger,
    func
)
load_dotenv(".env.sql")

DATABASE_URL = (
    os.getenv("DATABASE_URL")
)

if DATABASE_URL is None:
    raise ValueError("DATABASE_URL environment variable is not set.")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)
Base = declarative_base()

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

class DaySummary(Base):
    __tablename__ = "day_summary"

    summary_date = Column(Date, primary_key=True)
    total_activity = Column(Float, nullable=False)
    active_grids = Column(Integer, nullable=False)
    peak_hour = Column(Integer, nullable=False)
    top_grid = Column(Integer, nullable=False)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()

    try:
        yield db
    finally:
        db.close()