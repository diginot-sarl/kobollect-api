import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from tenacity import retry, wait_fixed, stop_after_attempt
from app.database import engine
from app.routes import router as routes

# Configure logging
logging.basicConfig(format='%(asctime)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="API KOBO/SIG",
    description="API de récupération des données KOBO",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Retry configuration
@retry(wait=wait_fixed(5), stop=stop_after_attempt(15), reraise=True)
def connect_to_db():
    try:
        # Attempt to establish a connection
        engine.connect()
        logger.info("Successfully connected to the database.")
    except Exception as e:
        logger.error("Database connection failed. Retrying...", exc_info=True)
        raise e

@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "ok"}

app.include_router(routes, prefix='/api/v1')