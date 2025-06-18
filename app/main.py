import logging

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from tenacity import retry, wait_fixed, stop_after_attempt
from app.database import engine
from app.routes import router as routes

# Configure logging
logging.basicConfig(format='%(asctime)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Retry configuration
@retry(wait=wait_fixed(30), stop=stop_after_attempt(30), reraise=True)
def connect_to_db():
    try:
        # Attempt to establish a connection
        engine.connect()
        logger.info("Successfully connected to the database.")
    except Exception as e:
        logger.error("Database connection failed. Retrying...", exc_info=True)
        raise e

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        connect_to_db()
        yield
    finally:
        await engine.dispose()

app = FastAPI(
    title="API KOBO/SIG",
    description="API de récupération des données KOBO",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "Hids Collect Working v1"}

app.include_router(routes, prefix='/api/v1')
