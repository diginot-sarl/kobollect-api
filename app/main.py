import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from service import process_kobo_data

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


@app.post("/import-kobo-data", tags=["KOBO"])
async def import_kobo_data(request: Request):
    try:
        data = await request.json()

        # Validation de l'identifiant de soumission
        if "received_data" not in data or "_id" not in data["received_data"]:
            raise HTTPException(status_code=400, detail="Champ '_id' manquant dans les données reçues.")

        return process_kobo_data(data)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
