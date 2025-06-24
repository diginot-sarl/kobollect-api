# app/database.py
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv
import os

load_dotenv()

engine = create_engine(
    os.getenv('MSSQL_SERVER'),
    isolation_level="REPEATABLE READ",
    poolclass=QueuePool,
    pool_size=20,          # Max idle connections
    max_overflow=40,       # Max temporary connections beyond pool_size
    pool_timeout=30,       # Wait time for connection (seconds)
    pool_recycle=300       # Recycle connections every 5 minutes
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()