import logging
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Load environment variables
load_dotenv()

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

@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "ok"}

