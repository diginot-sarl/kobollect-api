import zipfile
from io import BytesIO
import json
import json5
import requests
import logging
from collections import defaultdict

from datetime import timedelta, datetime
from typing import Optional, List
from sqlalchemy import Date
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text, func, cast, and_, or_  # Add Date and or_ here
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    File,
    Query,
    status,
    Request,
    UploadFile,
    BackgroundTasks)
from fastapi.security import OAuth2PasswordRequestForm
from app.auth import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    authenticate_user,
    create_access_token,
    get_current_active_user,
    Token,
    User,
)
from app.service import (
    process_recensement_form,
    process_rapport_superviseur_form,
    process_parcelles_non_baties_form,
    process_immeuble_plusieurs_proprietaires_form,
    process_immeuble_seul_proprietaire_form,
    update_to_erecettes
)
from app.schemas import (
    UserCreate,
    TeamCreate,
    AssignUserTeams,
    UserUpdate,
    PaginatedModuleResponse,
    ModuleOut,
    ModuleCreate,
    ModuleUpdate,
    PaginatedGroupeResponse,
    GroupeOut,
    GroupeCreate,
    GroupeUpdate,
    DroitOut,
    DroitCreate,
    DroitUpdate,
    AssignDroitsToEntity,
    UpdatePassword)
from app.database import get_db
from app.models import (
    Bien,
    Parcelle,
    Equipe,
    AgentEquipe,
    Utilisateur,
    Module,
    Groupe,
    Droit,
    GroupeDroit,
    UtilisateurDroit,
    Commune,
    Quartier,
    Menage,
    MembreMenage,
    Logs,
    LogsArchive,
    RapportRecensement,
    Adresse,
    Ville,
    Personne,
    Avenue,
    Quartier, Province, Rang, TypePersonne, NatureBien
)
from app.auth import get_password_hash
from app.utils import remove_trailing_commas

router = APIRouter()

logging.basicConfig(format='%(asctime)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

'''
@router.get("/geojson", tags=["GeoJSON"])
async def get_geojson(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=10000),
    date_start: str = Query(None),
    date_end: str = Query(None),
    province: str = Query(None),
    ville: str = Query(None),
    commune: str = Query(None),
    quartier: str = Query(None),
    avenue: str = Query(None),
    rang: str = Query(None),
    nature: str = Query(None),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Retrieve parcelles with their associated biens as GeoJSON data, with pagination and filters.

    Args:
        page: Page number for pagination (default: 1).
        page_size: Number of parcelles per page (default: 10, max: 10000).
        date_start: Filter parcelles created on or after this date (YYYY-MM-DD).
        date_end: Filter parcelles created on or before this date (YYYY-MM-DD).
        type: Ignored, kept for compatibility (always returns parcelles with biens).
        province: Filter by province ID.
        ville: Filter by ville ID.
        commune: Filter by commune ID.
        quartier: Filter by quartier ID.
        avenue: Filter by avenue ID.
        rang: Filter by rang ID.
        nature: Filter by bien nature ID (optional, not applied in this version).
        current_user: Authenticated user.
        db: Database session.

    Returns:
        A dictionary with paginated parcelle data, each including associated biens.

    Raises:
        HTTPException: If an error occurs during query execution.
    """
    try:
        # Build base query for parcelles with necessary relationships
        query = db.query(Parcelle).options(
            joinedload(Parcelle.fk_adresse).joinedload(Adresse.fk_avenue).joinedload(Avenue.fk_quartier).joinedload(Quartier.fk_commune).joinedload(Commune.fk_ville).joinedload(Ville.fk_province),
            joinedload(Parcelle.proprietaire).joinedload(Personne.fk_type_personne),
            joinedload(Parcelle.rang),
            joinedload(Parcelle.biens).joinedload(Bien.fk_nature_bien),
            joinedload(Parcelle.biens).joinedload(Bien.fk_agent)
        )

        # Apply filters
        if date_start:
            query = query.filter(cast(Parcelle.date_create, Date) >= date_start)
        if date_end:
            query = query.filter(cast(Parcelle.date_create, Date) <= date_end)
        if province:
            query = query.join(Parcelle.fk_adresse).join(Adresse.fk_avenue).join(Avenue.fk_quartier).join(Quartier.fk_commune).join(Commune.fk_ville).join(Ville.fk_province).filter(Province.id == province)
        if ville:
            query = query.join(Parcelle.fk_adresse).join(Adresse.fk_avenue).join(Avenue.fk_quartier).join(Quartier.fk_commune).join(Commune.fk_ville).filter(Ville.id == ville)
        if commune:
            query = query.join(Parcelle.fk_adresse).join(Adresse.fk_avenue).join(Avenue.fk_quartier).join(Quartier.fk_commune).filter(Commune.id == commune)
        if quartier:
            query = query.join(Parcelle.fk_adresse).join(Adresse.fk_avenue).join(Avenue.fk_quartier).filter(Quartier.id == quartier)
        if avenue:
            query = query.join(Parcelle.fk_adresse).join(Adresse.fk_avenue).filter(Avenue.id == avenue)
        if rang:
            query = query.join(Parcelle.fk_rang).filter(Rang.id == rang)
        # Note: 'nature' filter could be applied to filter parcelles with specific bien types, but omitted for simplicity

        # Get total count for pagination
        total = query.count()

        # Apply pagination
        query = query.order_by(Parcelle.id.desc()).offset((page - 1) * page_size).limit(page_size)

        # Execute query
        parcelles = query.all()

        # Format response
        data = []
        for parcelle in parcelles:
            parcelle_data = {
                "id": str(parcelle.id),
                "coordinates": parcelle.coordonnee_geographique,
                "date": parcelle.date_create.isoformat() if parcelle.date_create else None,
                "adresse": {
                    "numero": parcelle.adresse.numero if parcelle.adresse else None,
                    "avenue": parcelle.adresse.avenue.intitule if parcelle.adresse and parcelle.adresse.avenue else None,
                    "quartier": parcelle.adresse.avenue.quartier.intitule if parcelle.adresse and parcelle.adresse.avenue and parcelle.adresse.avenue.quartier else None,
                    "commune": parcelle.adresse.avenue.quartier.commune.intitule if parcelle.adresse and parcelle.adresse.avenue and parcelle.adresse.avenue.quartier and parcelle.adresse.avenue.quartier.commune else None
                },
                "proprietaire": {
                    "nom": parcelle.proprietaire.nom if parcelle.proprietaire else None,
                    "postnom": parcelle.proprietaire.postnom if parcelle.proprietaire else None,
                    "prenom": parcelle.proprietaire.prenom if parcelle.proprietaire else None,
                    "denomination": parcelle.proprietaire.denomination if parcelle.proprietaire else None,
                    "type_personne": parcelle.proprietaire.type_personne.intitule if parcelle.proprietaire and parcelle.proprietaire.type_personne else None
                },
                "biens": [
                    {
                        "id": str(bien.id),
                        "coordinates": bien.coordinates,
                        "date": bien.date_create.isoformat() if bien.date_create else None,
                        "nature": bien.nature_bien.intitule if bien.nature_bien else None,
                        "recense_par": f"{bien.fk_agent.nom} {bien.fk_agent.prenom}" if bien.fk_agent else None
                    } for bien in parcelle.biens
                ]
            }
            data.append(parcelle_data)

        return {
            "data": data,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        import traceback
        logger.error(f"Error in get_geojson: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

'''

from fastapi import Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from collections import defaultdict
from app.database import get_db
from app.auth import get_current_active_user
import logging

logger = logging.getLogger(__name__)

@router.get("/geojson", tags=["GeoJSON"])
async def get_geojson(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=10000),
    date_start: str = Query(None),
    date_end: str = Query(None),
    province: str = Query(None),
    ville: str = Query(None),
    commune: str = Query(None),
    quartier: str = Query(None),
    avenue: str = Query(None),
    rang: str = Query(None),
    nature: str = Query(None),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Build filters for parcelles
        filters = []
        params = {}

        if date_start:
            filters.append("CAST(p.date_create AS DATE) >= CAST(:date_start AS DATE)")
            params["date_start"] = date_start
        if date_end:
            filters.append("CAST(p.date_create AS DATE) <= CAST(:date_end AS DATE)")
            params["date_end"] = date_end
        if province:
            filters.append("pr.id = :province")
            params["province"] = province
        if ville:
            filters.append("v.id = :ville")
            params["ville"] = ville
        if commune:
            filters.append("c.id = :commune")
            params["commune"] = commune
        if quartier:
            filters.append("q.id = :quartier")
            params["quartier"] = quartier
        if avenue:
            filters.append("av.id = :avenue")
            params["avenue"] = avenue
        if rang:
            filters.append("r.id = :rang")
            params["rang"] = rang
        # Note: 'nature' filter could be added to filter parcelles with specific bien types, but omitted for simplicity

        # Base query fragment for parcelles
        base_query = """
            FROM parcelle p
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN ville v ON c.fk_ville = v.id
            LEFT JOIN province pr ON v.fk_province = pr.id
            LEFT JOIN rang r ON p.fk_rang = r.id
            WHERE 1=1
        """

        if filters:
            base_query += " AND " + " AND ".join(filters)

        # Count query for total parcelles
        count_query = f"SELECT COUNT(*) {base_query}"

        # Main query with CTE to fetch paginated parcelles and their biens
        cte_query = f"""
            SELECT p.id
            {base_query}
            ORDER BY p.id DESC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """

        main_query = f"""
            WITH selected_parcelles AS ({cte_query})
            SELECT
                p.id AS parcelle_id,
                p.coordonnee_geographique,
                p.date_create AS parcelle_date_create,
                a.numero AS adresse_numero,
                av.intitule AS avenue,
                q.intitule AS quartier,
                c.intitule AS commune,
                per.nom AS proprietaire_nom,
                per.postnom AS proprietaire_postnom,
                per.prenom AS proprietaire_prenom,
                per.denomination AS proprietaire_denomination,
                tp.intitule AS type_personne,
                b.id AS bien_id,
                b.coordinates AS bien_coordinates,
                b.date_create AS bien_date_create,
                nb.intitule AS nature,
                CONCAT(u.nom, ' ', u.prenom) AS recense_par
            FROM selected_parcelles sp
            JOIN parcelle p ON sp.id = p.id
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN personne per ON p.fk_proprietaire = per.id
            LEFT JOIN type_personne tp ON per.fk_type_personne = tp.id
            LEFT JOIN bien b ON b.fk_parcelle = p.id
            LEFT JOIN nature_bien nb ON b.fk_nature_bien = nb.id
            LEFT JOIN utilisateur u ON b.fk_agent = u.id
            ORDER BY p.id DESC, b.id DESC
        """

        # Set pagination parameters
        params["offset"] = (page - 1) * page_size
        params["limit"] = page_size

        # Execute count query
        total = db.execute(text(count_query), params).scalar()

        # Execute main query
        results = db.execute(text(main_query), params).fetchall()

        # Group biens under parcelles
        parcelles_dict = defaultdict(lambda: {
            "id": None,
            "coordinates": None,
            "date": None,
            "adresse": {},
            "proprietaire": {},
            "biens": []
        })

        for row in results:
            parcelle_id = row.parcelle_id
            if parcelles_dict[parcelle_id]["id"] is None:
                parcelles_dict[parcelle_id]["id"] = str(parcelle_id)
                parcelles_dict[parcelle_id]["coordinates"] = row.coordonnee_geographique
                parcelles_dict[parcelle_id]["date"] = row.parcelle_date_create.isoformat() if row.parcelle_date_create else None
                parcelles_dict[parcelle_id]["adresse"] = {
                    "numero": row.adresse_numero,
                    "avenue": row.avenue,
                    "quartier": row.quartier,
                    "commune": row.commune
                }
                parcelles_dict[parcelle_id]["proprietaire"] = {
                    "nom": row.proprietaire_nom,
                    "postnom": row.proprietaire_postnom,
                    "prenom": row.proprietaire_prenom,
                    "denomination": row.proprietaire_denomination,
                    "type_personne": row.type_personne
                }
            if row.bien_id:
                parcelles_dict[parcelle_id]["biens"].append({
                    "id": str(row.bien_id),
                    "coordinates": row.bien_coordinates,
                    "date": row.bien_date_create.isoformat() if row.bien_date_create else None,
                    "nature": row.nature,
                    "recense_par": row.recense_par
                })

        data = list(parcelles_dict.values())

        # Return response
        return {
            "data": data,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        import traceback
        logger.error(f"Error in get_geojson: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/import-geojson", tags=["GeoJSON"])
async def import_geojson_zip(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Import a zip file containing multiple GeoJSON files, process each, and send updated keys to a background task.

    Args:
        background_tasks: BackgroundTasks instance for running tasks asynchronously.
        file: The uploaded zip file containing GeoJSON files.
        current_user: The authenticated user, provided by dependency injection.
        db: The database session, provided by dependency injection.

    Returns:
        A dictionary with the processing results for each GeoJSON file.

    Raises:
        HTTPException: If the zip file is invalid or processing fails.
    """
    try:
        # Read the zip file content
        zip_content = await file.read()
        with zipfile.ZipFile(BytesIO(zip_content)) as zip_file:
            # Filter for GeoJSON files
            geojson_files = [f for f in zip_file.namelist() if f.endswith('.geojson')]
            if not geojson_files:
                raise HTTPException(status_code=400, detail="No GeoJSON files found in the zip archive.")

            all_updated_keys = []
            results = []

            # Process each GeoJSON file
            for geojson_file in geojson_files:
                with zip_file.open(geojson_file) as geojson_fp:
                    geojson_content = geojson_fp.read()
                    try:
                        geojson = json.loads(geojson_content)
                        updated_keys, updated_parcelle, updated_biens = process_geojson(geojson, db)
                        all_updated_keys.append(updated_keys)
                        results.append({
                            "file": geojson_file,
                            "updated_parcelle": updated_parcelle,
                            "updated_biens": updated_biens,
                            "status": "success"
                        })
                        db.commit()  # Commit changes for this file
                    except Exception as e:
                        db.rollback()  # Rollback on error for this file
                        results.append({
                            "file": geojson_file,
                            "error": str(e),
                            "status": "failed"
                        })

            # Send all updated keys to the background task
            background_tasks.add_task(update_to_erecettes, all_updated_keys, db)

            return {"results": results}

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing zip file: {str(e)}")


def process_geojson(geojson, db: Session):
    """
    Process a single GeoJSON file to update Parcelle and Bien records.

    Args:
        geojson: The parsed GeoJSON data.
        db: The database session.

    Returns:
        Tuple of (updated_keys, updated_parcelle_id, updated_bien_ids).

    Raises:
        ValueError: If the GeoJSON is invalid or lacks required features.
    """
    if geojson.get("type") != "FeatureCollection":
        raise ValueError("GeoJSON must be a FeatureCollection.")

    features = geojson.get("features", [])
    parcelle_features = [f for f in features if f.get("properties", {}).get("type") == "Parcelle"]
    if len(parcelle_features) != 1:
        raise ValueError("GeoJSON must contain exactly one Parcelle feature.")

    # Process Parcelle
    parcelle_feature = parcelle_features[0]
    parcelle_props = parcelle_feature.get("properties", {})
    parcelle_id = parcelle_props.get("id")
    parcelle_geometry = parcelle_feature.get("geometry", {})
    parcelle_coordinates = parcelle_geometry.get("coordinates")
    parcelle_superficie = parcelle_props.get("Sup")

    if not parcelle_id or not parcelle_coordinates:
        raise ValueError("Parcelle feature missing required properties.")

    parcelle = db.query(Parcelle).filter(Parcelle.id == parcelle_id).first()
    if not parcelle:
        raise ValueError(f"Parcelle with id {parcelle_id} not found.")
    parcelle.coord_corrige = json.dumps(parcelle_coordinates)
    parcelle.superficie_corrige = parcelle_superficie if parcelle_superficie is not None and parcelle_superficie != 0 else parcelle.superficie_calculee

    # Process Bien features
    bien_features = [f for f in features if f.get("properties", {}).get("type") == "Bien"]
    updated_bien_ids = []

    for bien_feature in bien_features:
        bien_props = bien_feature.get("properties", {})
        bien_id = bien_props.get("id")
        bien_geometry = bien_feature.get("geometry", {})
        bien_coordinates = bien_geometry.get("coordinates")
        bien_superficie = bien_props.get("Sup")

        if not bien_id or not bien_coordinates:
            continue  # Skip invalid Bien features

        bien = db.query(Bien).filter(Bien.id == bien_id).first()
        if bien:
            bien.coord_corrige = json.dumps(bien_coordinates)
            bien.superficie_corrige = bien_superficie if bien_superficie is not None and bien_superficie != 0 else bien.superficie
            updated_bien_ids.append(bien_id)

    # Collect updated keys
    updated_keys = {
        "parcelle": parcelle_id,
        "biens": updated_bien_ids
    }

    return updated_keys, parcelle_id, updated_bien_ids
    
      
# remember that updated keys is [{"parcelle": parcelle_id, "biens": ["bien_1", "bien_2"]}]

