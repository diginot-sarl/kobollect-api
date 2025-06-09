import json
import json5
import requests
import logging
from collections import defaultdict

from datetime import timedelta, datetime
from typing import Optional, List
from sqlalchemy import Date
from sqlalchemy.orm import Session
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
    process_immeuble_form)
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
    RapportRecensement)
from app.auth import get_password_hash
from app.utils import remove_trailing_commas

router = APIRouter()

logging.basicConfig(format='%(asctime)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


# Helper function to format personne (used in get_parcelle_details)
def format_personne(row):
    return {
        "id": row.id,
        "nom": row.nom,
        "postnom": row.postnom,
        "prenom": row.prenom,
        "denomination": row.denomination,
        "sigle": row.sigle,
    }
    
# Helper function to parse coordinates
def parse_coordinates(coord_str):
    # If coord_str is already an array, return it directly
    if isinstance(coord_str, list):
        return coord_str
    # If coord_str is None or empty, return None
    if not coord_str:
        return None
    points = []
    for part in coord_str.split(';'):
        vals = part.strip().split()
        if len(vals) >= 2:
            try:
                lat = float(vals[0])
                lng = float(vals[1])
                points.append([lat, lng])
            except Exception:
                continue
    return points if points else None


@router.post("/create-user-kobo", tags=["Kobo"])
async def create_kobo_account(user_data: UserCreate):
    """
    Route de test pour créer un compte KoboToolbox via l'API
    et vérifier les utilisateurs après 1 minute
    """
    KOBOTOOLBOX_URL = "http://kf.hidscollect.hologram.cd"
    KOBOTOOLBOX_ADMIN_USER = "super_admin"
    KOBOTOOLBOX_ADMIN_PASSWORD = 123456

    user_info = {
        "username": user_data.login,
        "password": 123456,
        "email": user_data.mail,
        "first_name": user_data.prenom,
        "last_name": user_data.nom
    }

    kobotoolbox_api_url = f"{KOBOTOOLBOX_URL}/api/v2/users/"
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(
            kobotoolbox_api_url,
            headers=headers,
            data=json.dumps(user_info),
            auth=(KOBOTOOLBOX_ADMIN_USER, KOBOTOOLBOX_ADMIN_PASSWORD)
        )
        
        if response.status_code == 201:
            return logger.info(f"Compte créé avec succès: {response.json()}")
        else:
            return logger.warning(f"Échec de la création du compte: {response.text}")
            
    except requests.exceptions.RequestException as e:
        return logger.error(f"Erreur de requête {str(e)}")

# Process Kobo data from Kobotoolbox
@router.post("/import-from-kobo", tags=["Kobo"])
async def process_kobo(request: Request,  background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    try:
        # Parse the raw JSON body
        payload = await request.json()
        
        # Validate that payload is a dictionary
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid payload format. Expected a JSON object.")
                
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")
    
    # Process the payload using the service function
    return process_recensement_form(payload, db, background_tasks)


# Process Kobo data from Kobotoolbox
@router.post("/import-rapport-superviseur", tags=["Kobo"])
async def process_rapport_superviseur(request: Request, db: Session = Depends(get_db)):
    try:
        # Parse the raw JSON body
        payload = await request.json()
        
        # Validate that payload is a dictionary
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid payload format. Expected a JSON object.")
                
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")
    
    # Process the payload using the service function
    return process_rapport_superviseur_form(payload, db)


# Process Kobo data from Kobotoolbox
@router.post("/import-parcelle-non-batie", tags=["Kobo"])
async def process_parcelles_non_baties(request: Request,  background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    try:
        # Parse the raw JSON body
        payload = await request.json()
        
        # Validate that payload is a dictionary
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid payload format. Expected a JSON object.")
                
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")
    
    # Process the payload using the service function
    return process_parcelles_non_baties_form(payload, db, background_tasks)


# Process Kobo data from Kobotoolbox
@router.post("/import-immeuble", tags=["Kobo"])
async def process_immeuble(request: Request,  background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    try:
        # Parse the raw JSON body
        payload = await request.json()
        
        # Validate that payload is a dictionary
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid payload format. Expected a JSON object.")
                
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")
    
    # Process the payload using the service function
    return process_immeuble_form(payload, db, background_tasks)


@router.post("/token", response_model=Token, tags=["Authentication"])
def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = authenticate_user(form_data.username, form_data.password, db)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.login}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@router.get("/users")
def get_all_users(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1),
    name: Optional[str] = None,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Base query with joins to get user, team, and droit information
        base_query = """
            SELECT 
                u.id, u.login, u.nom, u.postnom, u.prenom, u.date_create, 
                u.mail, u.telephone, u.photo_url, u.code_chasuble, u.fk_groupe,
                e.id AS equipe_id, e.intitule AS equipe_intitule,
                d.id AS droit_code,
                gd_d.id AS groupe_droit_code  -- Add this line to select group droits
            FROM utilisateur u
            LEFT JOIN agent_equipe ae ON u.id = ae.fk_agent
            LEFT JOIN equipe e ON ae.fk_equipe = e.id
            LEFT JOIN utilisateur_droit ud ON u.id = ud.fk_utilisateur
            LEFT JOIN droit d ON ud.fk_droit = d.id
            LEFT JOIN groupe_droit gd ON u.fk_groupe = gd.fk_groupe
            LEFT JOIN droit gd_d ON gd.fk_droit = gd_d.id
            WHERE 1=1
        """

        # Add filters
        filters = []
        params = {}
        if name:
            filters.append("(u.nom LIKE :name OR u.postnom LIKE :name OR u.prenom LIKE :name OR u.login LIKE :name)")
            params["name"] = f"%{name}%"
        if date_start:
            try:
                filters.append("CAST(u.date_create AS DATE) >= CAST(:date_start AS DATE)")
                params["date_start"] = date_start
            except Exception:
                pass
        if date_end:
            try:
                filters.append("CAST(u.date_create AS DATE) <= CAST(:date_end AS DATE)")
                params["date_end"] = date_end
            except Exception:
                pass

        # Build final query
        query = base_query
        if filters:
            query += " AND " + " AND ".join(filters)

        # Count total records (distinct users)
        count_query = f"""
            SELECT COUNT(DISTINCT subquery.id) 
            FROM (
                SELECT 
                    u.id, u.login, u.nom, u.postnom, u.prenom, u.date_create, 
                    u.mail, u.telephone, u.photo_url, u.code_chasuble, u.fk_groupe,
                    e.id AS equipe_id, e.intitule AS equipe_intitule,
                    d.id AS droit_code,
                    gd_d.id AS groupe_droit_code  -- Add this line to select group droits
                FROM utilisateur u
                LEFT JOIN agent_equipe ae ON u.id = ae.fk_agent
                LEFT JOIN equipe e ON ae.fk_equipe = e.id
                LEFT JOIN utilisateur_droit ud ON u.id = ud.fk_utilisateur
                LEFT JOIN droit d ON ud.fk_droit = d.id
                LEFT JOIN groupe_droit gd ON u.fk_groupe = gd.fk_groupe
                LEFT JOIN droit gd_d ON gd.fk_droit = gd_d.id
                WHERE 1=1
                {" AND " + " AND ".join(filters) if filters else ""}
            ) AS subquery
        """
        total = db.execute(text(count_query), params).scalar()

        # Add pagination using SQL Server syntax
        query += """
            ORDER BY u.id DESC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """
        params["limit"] = page_size
        params["offset"] = (page - 1) * page_size

        # Execute query
        results = db.execute(text(query), params).fetchall()

        # Process results
        users_map = {}
        for row in results:
            if row.id not in users_map:
                users_map[row.id] = {
                    "id": row.id,
                    "login": row.login,
                    "nom": row.nom,
                    "postnom": row.postnom,
                    "prenom": row.prenom,
                    "mail": row.mail,
                    "telephone": row.telephone,
                    "code_chasuble": row.code_chasuble,
                    "photo_url": row.photo_url,
                    "fk_groupe": row.fk_groupe,
                    "date_create": row.date_create.isoformat() if row.date_create else None,
                    "teams": [],
                    "droits": set()
                }
            
            # Add team if exists
            if row.equipe_id and not any(t['id'] == row.equipe_id for t in users_map[row.id]["teams"]):
                users_map[row.id]["teams"].append({
                    "id": row.equipe_id,
                    "intitule": row.equipe_intitule
                })
            
            # Add droit if exists
            if row.droit_code:
                users_map[row.id]["droits"].add(row.droit_code)
            # Add group droit if exists
            if row.groupe_droit_code:  # Add this block to handle group droits
                users_map[row.id]["droits"].add(row.groupe_droit_code)

        # Convert map to list and transform droits to list
        users = [
            {**user, "droits": list(user["droits"])}
            for user in users_map.values()
        ]

        return {
            "data": users,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/users/simple", tags=["Users"])
def get_all_users_simple(
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Query to get all users with only id, nom, postnom, and prenom
        query = """
            SELECT 
                u.id, 
                CONCAT(u.nom, ' ', u.postnom, ' ', u.prenom) AS full_name
            FROM utilisateur u
        """

        # Execute query
        results = db.execute(text(query)).fetchall()

        # Format results
        users = [{"id": row.id, "label": row.full_name} for row in results]

        return users

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/users/{user_id}")
def get_user(
    user_id: int,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Base query with joins to get team information
        query = """
            SELECT 
                u.id, u.login, u.nom, u.postnom, u.prenom, u.date_create, 
                u.mail, u.telephone, u.photo_url, u.code_chasuble, u.sexe, u.fk_groupe,
                e.id AS equipe_id, e.intitule AS equipe_intitule
            FROM utilisateur u
            LEFT JOIN agent_equipe ae ON u.id = ae.fk_agent
            LEFT JOIN equipe e ON ae.fk_equipe = e.id
            WHERE u.id = :user_id
        """

        # Execute query
        results = db.execute(text(query), {"user_id": user_id}).fetchall()

        if not results:
            raise HTTPException(status_code=404, detail="User not found")

        # Format results - group teams by user
        user_data = {
            "id": results[0].id,
            "login": results[0].login,
            "nom": results[0].nom,
            "postnom": results[0].postnom,
            "prenom": results[0].prenom,
            "sexe": results[0].sexe,
            "mail": results[0].mail,
            "telephone": results[0].telephone,
            "code_chasuble": results[0].code_chasuble,
            "photo_url": results[0].photo_url,
            "fk_groupe": results[0].fk_groupe,
            "date_create": results[0].date_create.isoformat() if results[0].date_create else None,
            "teams": []
        }

        # Add all teams if they exist
        for row in results:
            if row.equipe_id:
                user_data["teams"].append({
                    "id": row.equipe_id,
                    "intitule": row.equipe_intitule
                })

        return user_data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class UserWithDroits(User):
    droits: List[str] = []

@router.get("/user/me/", response_model=UserWithDroits, tags=["Users"])
def read_users_me(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    # Query user's direct droits
    direct_droits_query = """
        SELECT d.code 
        FROM utilisateur_droit ud
        JOIN droit d ON ud.fk_droit = d.id
        WHERE ud.fk_utilisateur = :user_id
    """
    direct_droits = db.execute(text(direct_droits_query), {"user_id": int(current_user.id)}).fetchall()
    
    # Query user's group droits
    group_droits_query = """
        SELECT d.code 
        FROM groupe_droit gd
        JOIN droit d ON gd.fk_droit = d.id
        WHERE gd.fk_groupe = :group_id
    """
    group_droits = []
    if current_user.fk_groupe:
        group_droits = db.execute(text(group_droits_query), {"group_id": current_user.fk_groupe}).fetchall()
    
    # Combine and deduplicate droit codes
    droit_codes = list(set([row.code for row in direct_droits + group_droits]))
    
    # Return user with droits
    return UserWithDroits(
        **current_user.__dict__,
        droits=droit_codes
    )


# Create a new user
@router.post("/users", response_model=User, tags=["Users"])
async def create_new_user(user_data: UserCreate, background_tasks: BackgroundTasks, current_user = Depends(get_current_active_user), db: Session = Depends(get_db)):
    try:
        # Generate login by combining first and last name, removing whitespace, and converting to lowercase
        login = f"{user_data.prenom}{user_data.nom}".replace(" ", "").lower()
        
        # Check if the user already exists
        existing_user = db.query(Utilisateur).filter(Utilisateur.login == login).first()
        if existing_user:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="User with this login already exists")

        # Handle empty string for code_chasuble
        code_chasuble = user_data.code_chasuble if user_data.code_chasuble else None

        # Check if code_chasuble is unique
        if code_chasuble:
            existing_code = db.query(Utilisateur).filter(Utilisateur.code_chasuble == code_chasuble).first()
            if existing_code:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="User with this code_chasuble already exists"
                )

        # Hash the password
        hashed_password = get_password_hash(user_data.password)

        # Create the new user
        new_user = Utilisateur(
            prenom=user_data.prenom,
            nom=user_data.nom,
            postnom=user_data.postnom,
            sexe=user_data.sexe,
            telephone=user_data.telephone,
            login=login,
            id_kobo=login,
            password=hashed_password,
            mail=user_data.mail,
            photo_url=user_data.photo_url,
            code_chasuble=code_chasuble,
            fk_groupe=user_data.fk_groupe
        )
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        
        new_kobo_user: UserCreate = UserCreate(
            prenom=user_data.prenom,
            nom=user_data.nom,
            login=login,
            password=user_data.password,
            mail=user_data.mail,
        )
        
        background_tasks.add_task(create_kobo_account, new_kobo_user)
        
        logger.info(f"User {login} created successfully")
        
        return new_user

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to create user: {str(e)}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Failed to create user: {str(e)}")

# Fetch GeoJSON data with filters
@router.get("/geojson", tags=["GeoJSON"])
async def get_geojson(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=10000),
    date_start: str = Query(None),
    date_end: str = Query(None),
    type: str = Query('parcelle'),
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
        # Base queries
        parcelle_query = """
            SELECT
                p.id,
                p.coordonnee_geographique,
                p.date_create,
                a.numero AS adresse_numero,
                av.intitule AS avenue,
                q.intitule AS quartier,
                c.intitule AS commune,
                per.nom AS proprietaire_nom,
                per.postnom AS proprietaire_postnom,
                per.prenom AS proprietaire_prenom,
                per.denomination AS proprietaire_denomination,
                tp.intitule AS type_personne
            FROM parcelle p
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN personne per ON p.fk_proprietaire = per.id
            LEFT JOIN type_personne tp ON per.fk_type_personne = tp.id
            LEFT JOIN ville v ON c.fk_ville = v.id
            LEFT JOIN province pr ON v.fk_province = pr.id
            LEFT JOIN rang r ON p.fk_rang = r.id
            WHERE 1=1
        """
        
        bien_query = """
            SELECT DISTINCT
                b.id,
                b.coordinates,
                b.date_create,
                nb.intitule AS nature,
                CONCAT(u.nom, ' ', u.prenom) AS recense_par,
                a.numero AS adresse_numero,
                av.intitule AS avenue,
                q.intitule AS quartier,
                c.intitule AS commune,
                per.nom AS proprietaire_nom,
                per.postnom AS proprietaire_postnom,
                per.prenom AS proprietaire_prenom,
                per.denomination AS proprietaire_denomination,
                tp.intitule AS type_personne
            FROM bien b
            LEFT JOIN nature_bien nb ON b.fk_nature_bien = nb.id
            LEFT JOIN utilisateur u ON b.fk_agent = u.id
            LEFT JOIN parcelle p ON b.fk_parcelle = p.id
            LEFT JOIN personne per ON b.fk_proprietaire = per.id
            LEFT JOIN type_personne tp ON per.fk_type_personne = tp.id
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN ville v ON c.fk_ville = v.id
            LEFT JOIN province pr ON v.fk_province = pr.id
            LEFT JOIN rang r ON p.fk_rang = r.id
            WHERE 1=1
        """

        query = parcelle_query if type == "parcelle" else bien_query
        date_field = "p.date_create" if type == "parcelle" else "b.date_create"

        # Initialize filters and parameters
        filters = []
        params = {}
        join_clauses = []

        # Date filters
        if date_start:
            filters.append(f"CAST({date_field} AS DATE) >= CAST(:date_start AS DATE)")
            params["date_start"] = date_start
        if date_end:
            filters.append(f"CAST({date_field} AS DATE) <= CAST(:date_end AS DATE)")
            params["date_end"] = date_end

        # Location filters
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
        if nature and type == "bien":
            filters.append("nb.id = :nature")
            params["nature"] = nature

        # Apply filters
        if filters:
            query += " AND " + " AND ".join(filters)

        # Count total records with optimized queries
        if type == "bien":
            # Efficient count query for biens
            count_query = "SELECT COUNT(DISTINCT b.id) FROM bien b"
            
            # Define location patterns that require joins
            location_patterns = ["pr.id", "v.id", "c.id", "q.id", "av.id", "r.id"]
            
            # Check if any filter contains location patterns
            if any(any(pattern in f for pattern in location_patterns) for f in filters):
                count_query += """
                    LEFT JOIN parcelle p ON b.fk_parcelle = p.id
                    LEFT JOIN adresse a ON p.fk_adresse = a.id
                    LEFT JOIN avenue av ON a.fk_avenue = av.id
                    LEFT JOIN quartier q ON av.fk_quartier = q.id
                    LEFT JOIN commune c ON q.fk_commune = c.id
                    LEFT JOIN ville v ON c.fk_ville = v.id
                    LEFT JOIN province pr ON v.fk_province = pr.id
                    LEFT JOIN rang r ON p.fk_rang = r.id
                """
            
            # Check for nature filter
            if any("nb.id" in f for f in filters):
                count_query += " LEFT JOIN nature_bien nb ON b.fk_nature_bien = nb.id"
            
            # Apply filters
            if filters:
                count_query += " WHERE " + " AND ".join(filters)
                
            total = db.execute(text(count_query), params).scalar()
        else:
            # Standard count for parcelles
            count_query = f"SELECT COUNT(*) FROM ({query}) AS total"
            total = db.execute(text(count_query), params).scalar()

        # Add pagination
        query += f"""
            ORDER BY {"p.id" if type == "parcelle" else "b.id"} DESC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """
        params["limit"] = page_size
        params["offset"] = (page - 1) * page_size

        # Execute query
        results = db.execute(text(query), params).fetchall()

        # Format results
        common_fields = {
            "id": lambda row: str(row.id),
            "coordinates": lambda row: row.coordonnee_geographique if type == "parcelle" else row.coordinates,
            "adresse": lambda row: {
                "numero": row.adresse_numero,
                "avenue": row.avenue,
                "quartier": row.quartier,
                "commune": row.commune
            },
            "proprietaire": lambda row: {
                "nom": row.proprietaire_nom,
                "postnom": row.proprietaire_postnom,
                "prenom": row.proprietaire_prenom,
                "denomination": row.proprietaire_denomination,
                "type_personne": row.type_personne
            },
            "date": lambda row: row.date_create.isoformat() if row.date_create else None,
        }

        if type == "parcelle":
            data = [{key: func(row) for key, func in common_fields.items()} for row in results]
        else:
            data = [{
                **{key: func(row) for key, func in common_fields.items()},
                "recense_par": row.recense_par,
                "nature": row.nature
            } for row in results]

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

# Fetch provinces
@router.get("/provinces", tags=["GeoJSON"])
def get_provinces(
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = "SELECT id, intitule FROM province"
        result = db.execute(text(query)).fetchall()
        return [{"id": row[0], "intitule": row[1]} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Fetch villes by province
@router.get("/villes", tags=["GeoJSON"])
async def get_villes(
    province: str = Query(...),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = "SELECT id, intitule FROM ville WHERE fk_province = :province"
        result = db.execute(text(query), {"province": province}).fetchall()
        return [{"id": row[0], "intitule": row[1]} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch communes by ville
@router.get("/communes", tags=["GeoJSON"])
def get_communes(
    ville: str = Query(...),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = "SELECT id, intitule FROM commune WHERE fk_ville = :ville"
        result = db.execute(text(query), {"ville": ville}).fetchall()
        return [{"id": row[0], "intitule": row[1]} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch quartiers by commune
@router.get("/quartiers", tags=["GeoJSON"])
def get_quartiers(
    commune: str = Query(...),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = "SELECT id, intitule FROM quartier WHERE fk_commune = :commune"
        result = db.execute(text(query), {"commune": commune}).fetchall()
        return [{"id": row[0], "intitule": row[1]} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch avenues by quartier
@router.get("/avenues", tags=["GeoJSON"])
def get_avenues(
    quartier: str = Query(...),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = "SELECT id, intitule FROM avenue WHERE fk_quartier = :quartier"
        result = db.execute(text(query), {"quartier": quartier}).fetchall()
        return [{"id": row[0], "intitule": row[1]} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch rangs
@router.get("/rangs", tags=["GeoJSON"])
def get_rangs(
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = "SELECT id, intitule FROM rang"
        result = db.execute(text(query)).fetchall()
        return [{"id": row[0], "intitule": row[1]} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch natures
@router.get("/natures", tags=["GeoJSON"])
def get_natures(
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = "SELECT id, intitule FROM nature_bien"
        result = db.execute(text(query)).fetchall()
        return [{"id": row[0], "intitule": row[1]} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch usages
@router.get("/usages", tags=["GeoJSON"])
def get_usages(
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = "SELECT id, intitule FROM usage"
        result = db.execute(text(query)).fetchall()
        return [{"id": row[0], "intitule": row[1]} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch usage_specifiques
@router.get("/usage-specifiques", tags=["GeoJSON"])
def get_usage_specifiques(
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = "SELECT id, intitule FROM usage_specifique"
        result = db.execute(text(query)).fetchall()
        return [{"id": row[0], "intitule": row[1]} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch parcelles with filters
@router.get("/parcelles", tags=["Parcelles"])
async def get_parcelles(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    date_start: str = Query(None),
    date_end: str = Query(None),
    province: str = Query(None),
    ville: str = Query(None),
    commune: str = Query(None),
    quartier: str = Query(None),
    avenue: str = Query(None),
    rang: str = Query(None),
    nature: str = Query(None),
    keyword: str = Query(None),
    accessibilite: int = Query(None, description="Filter by accessibility: 1 for accessible, 2 for inaccessible"),  # Changed to int
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
    fk_agent: int = Query(None)
):
    try:
        # Base query with all necessary joins
        base_query = """
            SELECT 
                p.id, p.numero_parcellaire, p.superficie_calculee, p.coordonnee_geographique, p.date_create, p.statut,p.fk_agent,
                per.id AS proprietaire_id, per.nom AS proprietaire_nom, per.postnom AS proprietaire_postnom,
                per.prenom AS proprietaire_prenom, per.denomination AS proprietaire_denomination,
                per.sigle AS proprietaire_sigle, per.fk_type_personne AS proprietaire_type_id,
                r.id AS rang_id, r.intitule AS rang_intitule,
                u.id AS type_personne_id, u.intitule AS type_personne_intitule,
                c.intitule AS commune,
                q.intitule AS quartier,
                av.intitule AS avenue,
                b.id AS bien_id,
                b.nombre_etage,
                nb.intitule AS nature_bien,
                un.intitule AS unite,
                us.intitule AS usage,
                usp.intitule AS usage_specifique
            FROM parcelle p
            LEFT JOIN personne per ON p.fk_proprietaire = per.id
            LEFT JOIN rang r ON p.fk_rang = r.id
            LEFT JOIN type_personne u ON per.fk_type_personne = u.id
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN ville v ON c.fk_ville = v.id
            LEFT JOIN province pr ON v.fk_province = pr.id
            LEFT JOIN bien b ON p.id = b.fk_parcelle
            LEFT JOIN nature_bien nb ON b.fk_nature_bien = nb.id
            LEFT JOIN unite un ON b.fk_unite = un.id
            LEFT JOIN usage us ON b.fk_usage = us.id
            LEFT JOIN usage_specifique usp ON b.fk_usage_specifique = usp.id
            WHERE 1=1
        """

        # Add filters
        filters = []
        params = {}
        if fk_agent:
            filters.append("p.fk_agent = :fk_agent")
            params["fk_agent"] = fk_agent
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
        if keyword:
            keyword_filters = [
                "per.nom LIKE :keyword",
                "per.postnom LIKE :keyword",
                "per.prenom LIKE :keyword",
                "per.denomination LIKE :keyword",
                "r.intitule LIKE :keyword"
            ]
            filters.append(f"({' OR '.join(keyword_filters)})")
            params["keyword"] = f"%{keyword}%"
        if accessibilite:
            if accessibilite in [1, 2]:  # Only accept 1 or 2
                filters.append("p.statut = :accessibilite")
                params["accessibilite"] = accessibilite

        # Build final query
        query = base_query
        if filters:
            query += " AND " + " AND ".join(filters)

        # Count total records
        count_query = f"SELECT COUNT(*) FROM ({query}) AS total"
        total = db.execute(text(count_query), params).scalar()

        # Add pagination using SQL Server syntax
        query += """
            ORDER BY p.id DESC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """
        params["limit"] = page_size
        params["offset"] = (page - 1) * page_size

        # Execute main query
        results = db.execute(text(query), params).fetchall()

        # Format results
        data = []
        parcelle_map = {}
        for row in results:
            if row.id not in parcelle_map:
                parcelle_map[row.id] = {
                    "id": row.id,
                    "numero_parcellaire": row.numero_parcellaire,
                    "superficie_calculee": row.superficie_calculee,
                    "coordonnee_geographique": row.coordonnee_geographique,
                    "date_create": row.date_create.isoformat() if row.date_create else None,
                    "statut": "Accessible" if row.statut == 1 else "Non accessible",
                    "adresse": {
                        "commune": row.commune,
                        "quartier": row.quartier,
                        "avenue": row.avenue
                    },
                    "proprietaire": {
                        "id": row.proprietaire_id,
                        "nom": row.proprietaire_nom,
                        "postnom": row.proprietaire_postnom,
                        "prenom": row.proprietaire_prenom,
                        "denomination": row.proprietaire_denomination,
                        "sigle": row.proprietaire_sigle,
                        "type_personne": row.type_personne_intitule if row.type_personne_id else None,
                    },
                    "rang": row.rang_intitule,
                    "biens": [],
                    "fk_agent": row.fk_agent
                }
            
            if row.bien_id:
                parcelle_map[row.id]["biens"].append({
                    "id": row.bien_id,
                    "nature_bien": row.nature_bien,
                    "unite": row.unite,
                    "usage": row.usage,
                    "usage_specifique": row.usage_specifique,
                    "nombre_etage": row.nombre_etage
                })

        # Convert the map to a list
        data = list(parcelle_map.values())

        return {
            "data": data,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch parcelle details by ID
@router.get("/parcelles/{parcelle_id}", tags=["Parcelles"])
async def get_parcelle_details(
    parcelle_id: int,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Main query to fetch parcelle, owner, and address hierarchy
        parcelle_query = """
            SELECT 
                p.id, p.numero_parcellaire, p.superficie_calculee, p.coordonnee_geographique, 
                p.date_create, p.longueur, p.largeur, p.fk_agent, p.statut,
                per.id AS proprietaire_id, per.nom AS proprietaire_nom, per.postnom AS proprietaire_postnom,
                per.prenom AS proprietaire_prenom, per.denomination AS proprietaire_denomination,
                per.sigle AS proprietaire_sigle, per.fk_type_personne AS proprietaire_type_id,
                a.numero AS adresse_numero, av.intitule AS avenue_intitule,
                q.intitule AS quartier_intitule, c.intitule AS commune_intitule,
                v.intitule AS ville_intitule, pr.intitule AS province_intitule,
                tp.id AS type_personne_id, tp.intitule AS type_personne_intitule,
                u.id AS unite_id, u.intitule AS unite_intitule,
                r.id AS rang_id, r.intitule AS rang_intitule,
                agent.nom AS agent_nom, agent.postnom AS agent_postnom, agent.prenom AS agent_prenom
            FROM parcelle p
            LEFT JOIN personne per ON p.fk_proprietaire = per.id
            LEFT JOIN type_personne tp ON per.fk_type_personne = tp.id
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN ville v ON c.fk_ville = v.id
            LEFT JOIN province pr ON v.fk_province = pr.id
            LEFT JOIN unite u ON p.fk_unite = u.id
            LEFT JOIN rang r ON p.fk_rang = r.id
            LEFT JOIN utilisateur agent ON p.fk_agent = agent.id
            WHERE p.id = :parcelle_id
        """

        # Execute main query
        parcelle_result = db.execute(text(parcelle_query), {"parcelle_id": parcelle_id}).first()
        
        if not parcelle_result:
            raise HTTPException(status_code=404, detail="Parcelle not found")

        # Format address information
        address_info = {
            "numero": parcelle_result.adresse_numero,
            "avenue": parcelle_result.avenue_intitule,
            "quartier": parcelle_result.quartier_intitule,
            "commune": parcelle_result.commune_intitule,
            "ville": parcelle_result.ville_intitule,
            "province": parcelle_result.province_intitule,
        }

        # Format owner information
        proprietaire = {
            "id": parcelle_result.proprietaire_id,
            "nom": parcelle_result.proprietaire_nom,
            "postnom": parcelle_result.proprietaire_postnom,
            "prenom": parcelle_result.proprietaire_prenom,
            "denomination": parcelle_result.proprietaire_denomination,
            "sigle": parcelle_result.proprietaire_sigle,
            "type_personne": parcelle_result.type_personne_intitule if parcelle_result.type_personne_id else None,
        }

        # Query for biens and their related information
        biens_query = """
            SELECT 
                b.id, b.ref_bien, b.coordinates, b.superficie, b.date_create,
                nb.intitule AS nature_bien,
                u.intitule AS unite,
                us.intitule AS usage,
                usp.intitule AS usage_specifique,
                m.id AS menage_id, m.fk_personne AS menage_owner_id,
                lb.fk_personne AS locataire_id,
                mm.fk_personne AS membre_id
            FROM bien b
            LEFT JOIN nature_bien nb ON b.fk_nature_bien = nb.id
            LEFT JOIN unite u ON b.fk_unite = u.id
            LEFT JOIN usage us ON b.fk_usage = us.id
            LEFT JOIN usage_specifique usp ON b.fk_usage_specifique = usp.id
            LEFT JOIN menage m ON b.id = m.fk_bien
            LEFT JOIN location_bien lb ON b.id = lb.fk_bien
            LEFT JOIN membre_menage mm ON m.id = mm.fk_menage
            WHERE b.fk_parcelle = :parcelle_id
        """

        # Execute biens query
        biens_results = db.execute(text(biens_query), {"parcelle_id": parcelle_id}).fetchall()

        # Helper function to get personne details
        def get_personne_details(personne_id: int):
            if not personne_id:
                return None
            query = """
                SELECT 
                    p.id, p.nom, p.postnom, p.prenom, p.denomination, p.sigle, p.nif,
                    p.sexe, p.fk_type_personne, tp.intitule AS type_personne,
                    fm.intitule AS lien_parente, n.intitule AS nationalite
                FROM personne p
                LEFT JOIN type_personne tp ON p.fk_type_personne = tp.id
                LEFT JOIN filiation_membre fm ON p.fk_lien_parente = fm.id
                LEFT JOIN nationalite n ON p.fk_nationalite = n.id
                WHERE p.id = :personne_id
            """
            result = db.execute(text(query), {"personne_id": personne_id}).first()
            if result:
                return {
                    "id": result.id,
                    "nom": result.nom,
                    "postnom": result.postnom,
                    "prenom": result.prenom,
                    "denomination": result.denomination,
                    "sigle": result.sigle,
                    "nif": result.nif,
                    "sexe": result.sexe,
                    "type_personne": result.type_personne,
                    "lien_parente": result.lien_parente,
                    "nationalite": result.nationalite
                }
            return None

        # Process biens and their relationships
        biens_map = {}
        for row in biens_results:
            if row.id not in biens_map:
                biens_map[row.id] = {
                    "id": row.id,
                    "ref_bien": row.ref_bien,
                    "coordinates": row.coordinates,
                    "superficie": row.superficie,
                    "date_create": row.date_create.isoformat() if row.date_create else None,
                    "nature_bien": row.nature_bien,
                    "unite": row.unite,
                    "usage": row.usage,
                    "usage_specifique": row.usage_specifique,
                    "proprietaire": get_personne_details(row.menage_owner_id),
                    "locataire": get_personne_details(row.locataire_id),
                    "membres_menage": []
                }

            # Add membre if exists
            if row.membre_id:
                membre_details = get_personne_details(row.membre_id)
                if membre_details:
                    biens_map[row.id]["membres_menage"].append(membre_details)

        return {
            "parcelle": {
                "id": parcelle_result.id,
                "numero_parcellaire": parcelle_result.numero_parcellaire,
                "superficie_calculee": parcelle_result.superficie_calculee,
                "coordonnee_geographique": parcelle_result.coordonnee_geographique,
                "date_create": parcelle_result.date_create.isoformat() if parcelle_result.date_create else None,
                "longueur": parcelle_result.longueur,
                "largeur": parcelle_result.largeur,
                "unite": parcelle_result.unite_intitule,
                "rang": parcelle_result.rang_intitule,
                "ajoute_par": f"{parcelle_result.agent_nom} {parcelle_result.agent_postnom} {parcelle_result.agent_prenom}",
                "adresse": address_info,
                "statut": parcelle_result.statut
            },
            "proprietaire": proprietaire,
            "biens": list(biens_map.values()),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/population-by-age-pyramid", tags=["Populations"])
def get_age_pyramid(
    commune: Optional[str] = Query(None),
    quartier: Optional[str] = Query(None),
    avenue: Optional[str] = Query(None),
    date_start: str = Query(..., description="Start date in format YYYY-MM-DD", regex=r"^\d{4}-\d{2}-\d{2}$"),
    date_end: str = Query(..., description="End date in format YYYY-MM-DD", regex=r"^\d{4}-\d{2}-\d{2}$"),
    current_user=Depends(get_current_active_user),
    db: Session=Depends(get_db),
):
    try:
        # Base query to get filtered parcelle IDs
        parcelle_query = """
            SELECT DISTINCT p.id
            FROM parcelle p
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            WHERE 1=1
        """

        # Add filters
        filters = []
        params = {}
        if commune:
            filters.append("c.id = :commune")
            params["commune"] = commune
        if quartier:
            filters.append("q.id = :quartier")
            params["quartier"] = quartier
        if avenue:
            filters.append("av.id = :avenue")
            params["avenue"] = avenue

        # Build final parcelle query
        if filters:
            parcelle_query += " AND " + " AND ".join(filters)

        # Get filtered parcelle IDs
        parcelle_ids = [row[0] for row in db.execute(text(parcelle_query), params).fetchall()]

        # If no parcelle IDs found, return empty list
        if not parcelle_ids:
            return []

        # Prepare parcelle IDs for IN clause
        parcelle_ids_str = ",".join(map(str, parcelle_ids))

        # Query to get persons with date filters
        persons_query = """
        WITH related_persons AS (
            SELECT DISTINCT p.id, p.sexe, p.date_naissance
            FROM personne p
            WHERE p.date_naissance IS NOT NULL 
            AND p.date_naissance <= GETDATE()
            AND CAST(p.date_create AS DATE) >= CAST(:date_start AS DATE)
            AND CAST(p.date_create AS DATE) <= CAST(:date_end AS DATE)
            AND (
                -- Owners of parcelles
                p.id IN (
                    SELECT fk_proprietaire 
                    FROM parcelle 
                    WHERE id IN ({parcelle_ids}) AND fk_proprietaire IS NOT NULL
                )
                OR
                -- Persons in menage related to parcelles
                p.id IN (
                    SELECT m.fk_personne 
                    FROM bien b
                    JOIN menage m ON b.id = m.fk_bien
                    WHERE b.fk_parcelle IN ({parcelle_ids})
                )
                OR
                -- Persons in location_bien related to parcelles
                p.id IN (
                    SELECT lb.fk_personne 
                    FROM bien b
                    JOIN location_bien lb ON b.id = lb.fk_bien
                    WHERE b.fk_parcelle IN ({parcelle_ids})
                )
                OR
                -- Members of menage related to parcelles
                p.id IN (
                    SELECT mm.fk_personne 
                    FROM bien b
                    JOIN menage m ON b.id = m.fk_bien
                    JOIN membre_menage mm ON m.id = mm.fk_menage
                    WHERE b.fk_parcelle IN ({parcelle_ids})
                )
            )
        )
        SELECT id, sexe, date_naissance
        FROM related_persons
        """.format(parcelle_ids=parcelle_ids_str)

        # Execute query with date parameters
        results = db.execute(
            text(persons_query),
            {
                "date_start": date_start,
                "date_end": date_end,
                **params
            }
        ).fetchall()

        # Log the number of persons retrieved
        logger.info(f"Retrieved {len(results)} persons with non-NULL date_naissance")

        # Format results
        persons = [
            {
                "id": row[0],
                "sexe": row[1],
                "date_naissance": row[2].isoformat() if row[2] else None
            }
            for row in results
        ]

        return persons

    except Exception as e:
        logger.error(f"Error in get_age_pyramid: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# Fetch populations with filters
@router.get("/populations", tags=["Populations"])
def get_populations(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    commune: Optional[str] = Query(None),
    quartier: Optional[str] = Query(None),
    avenue: Optional[str] = Query(None),
    rang: Optional[str] = Query(None),
    keyword: Optional[str] = Query(None),
    date_start: str = Query(..., description="Start date in format YYYY-MM-DD", regex=r"^\d{4}-\d{2}-\d{2}$"),
    date_end: str = Query(..., description="End date in format YYYY-MM-DD", regex=r"^\d{4}-\d{2}-\d{2}$"),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Base query to get filtered parcelle IDs
        parcelle_query = """
            SELECT DISTINCT p.id
            FROM parcelle p
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN personne per ON p.fk_proprietaire = per.id
            WHERE 1=1 AND per.fk_type_personne = 1
        """

        # Add filters
        filters = []
        params = {}
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
            filters.append("p.fk_rang = :rang")
            params["rang"] = rang
        if keyword:
            keyword_filters = [
                "per.nom LIKE :keyword",
                "per.postnom LIKE :keyword",
                "per.prenom LIKE :keyword",
                "per.denomination LIKE :keyword",
                "per.sigle LIKE :keyword"
            ]
            filters.append(f"({' OR '.join(keyword_filters)})")
            params["keyword"] = f"%{keyword}%"

        # Add date filters
        filters.append("CAST(p.date_create AS DATE) >= CAST(:date_start AS DATE)")
        filters.append("CAST(p.date_create AS DATE) <= CAST(:date_end AS DATE)")
        params["date_start"] = date_start
        params["date_end"] = date_end

        # Build final parcelle query
        if filters:
            parcelle_query += " AND " + " AND ".join(filters)

        # Get filtered parcelle IDs
        parcelle_ids = [row[0] for row in db.execute(text(parcelle_query), params).fetchall()]

        if not parcelle_ids:
            logger.info("Not parcelle_ids")
            return {
                "data": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
            }

        # Get all person IDs with sorting applied
        person_query = """
            WITH all_persons AS (
                SELECT p.fk_proprietaire AS person_id
                FROM parcelle p
                JOIN personne per ON p.fk_proprietaire = per.id
                WHERE p.id IN (SELECT * FROM string_split(:parcelle_ids, ','))
                AND p.fk_proprietaire IS NOT NULL
                AND per.fk_type_personne = 1
                UNION
                SELECT m.fk_personne AS person_id
                FROM bien b
                JOIN menage m ON b.id = m.fk_bien
                JOIN personne per ON m.fk_personne = per.id
                WHERE b.fk_parcelle IN (SELECT * FROM string_split(:parcelle_ids, ','))
                AND m.fk_personne IS NOT NULL
                AND per.fk_type_personne = 1
                UNION
                SELECT lb.fk_personne AS person_id
                FROM bien b
                JOIN location_bien lb ON b.id = lb.fk_bien
                JOIN personne per ON lb.fk_personne = per.id
                WHERE b.fk_parcelle IN (SELECT * FROM string_split(:parcelle_ids, ','))
                AND lb.fk_personne IS NOT NULL
                AND per.fk_type_personne = 1
                UNION
                SELECT mm.fk_personne AS person_id
                FROM bien b
                JOIN menage m ON b.id = m.fk_bien
                JOIN membre_menage mm ON m.id = mm.fk_menage
                WHERE b.fk_parcelle IN (SELECT * FROM string_split(:parcelle_ids, ','))
                AND mm.fk_personne IS NOT NULL
            )
            SELECT DISTINCT p.id, p.date_create  -- Include date_create in SELECT
            FROM all_persons ap
            JOIN personne p ON ap.person_id = p.id
            ORDER BY p.date_create DESC  -- Apply sorting here
        """

        # Convert parcelle_ids to a comma-separated string
        parcelle_ids_str = ",".join(str(id) for id in parcelle_ids)

        # Execute person query with proper parameterization
        person_ids = [
            row[0] for row in db.execute(
                text(person_query),
                {"parcelle_ids": parcelle_ids_str if parcelle_ids_str else "0"}
            ).fetchall()
        ]

        # Pagination
        total = len(person_ids)
        start = (page - 1) * page_size
        end = start + page_size
        paginated_ids = person_ids[start:end]

        # Query to get person details (remove ORDER BY here since it's already sorted)
        person_details_query = """
            SELECT 
                p.id, p.nom, p.postnom, p.prenom, p.fk_lien_parente,
                p.nif, p.lieu_naissance, p.date_naissance, p.profession,
                p.etat_civil, p.telephone, p.adresse_mail, p.niveau_etude,
                p.date_create,
                tp.id AS type_personne_id, tp.intitule AS type_personne_intitule,
                n.intitule AS nationalite,
                a.numero AS adresse_numero,
                av.intitule AS avenue,
                q.intitule AS quartier,
                c.intitule AS commune,
                CASE
                    WHEN p.id IN (
                        SELECT p.fk_proprietaire
                        FROM parcelle p
                        WHERE p.id IN (SELECT * FROM string_split(:parcelle_ids, ','))
                    ) THEN 'Propriétaire'
                    WHEN p.id IN (
                        SELECT m.fk_personne
                        FROM bien b
                        JOIN menage m ON b.id = m.fk_bien
                        WHERE b.fk_parcelle IN (SELECT * FROM string_split(:parcelle_ids, ','))
                    ) THEN 'Responsable menage'
                    WHEN p.id IN (
                        SELECT mm.fk_personne
                        FROM bien b
                        JOIN menage m ON b.id = m.fk_bien
                        JOIN membre_menage mm ON m.id = mm.fk_menage
                        WHERE b.fk_parcelle IN (SELECT * FROM string_split(:parcelle_ids, ','))
                    ) THEN 'Membre menage'
                    ELSE 'Inconnu'
                END AS categorie,
                CASE
                    WHEN p.id IN (
                        SELECT mm.fk_personne
                        FROM bien b
                        JOIN menage m ON b.id = m.fk_bien
                        JOIN membre_menage mm ON m.id = mm.fk_menage
                        WHERE b.fk_parcelle IN (SELECT * FROM string_split(:parcelle_ids, ','))
                    ) THEN (
                        SELECT TOP 1 CONCAT(rp.nom, ' ', rp.prenom)
                        FROM menage m
                        JOIN personne rp ON m.fk_personne = rp.id
                        WHERE m.id = (
                            SELECT TOP 1 mm.fk_menage
                            FROM membre_menage mm
                            WHERE mm.fk_personne = p.id
                        )
                    )
                    ELSE NULL
                END AS nom_responsable
            FROM personne p
            LEFT JOIN type_personne tp ON p.fk_type_personne = tp.id
            LEFT JOIN nationalite n ON p.fk_nationalite = n.id
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            WHERE p.id IN (SELECT * FROM string_split(:person_ids, ','))
        """

        # Convert paginated_ids to a comma-separated string
        paginated_ids_str = ",".join(str(id) for id in paginated_ids)

        # Get person details
        person_results = db.execute(
            text(person_details_query),
            {"person_ids": paginated_ids_str if paginated_ids_str else "0", "parcelle_ids": parcelle_ids_str if parcelle_ids_str else "0"}
        ).fetchall()

        # Format results
        data = [{
            "id": row.id,
            "nom": row.nom,
            "postnom": row.postnom,
            "prenom": row.prenom,
            "categorie": row.categorie,
            "lien_de_famille": row.fk_lien_parente,
            "type_personne": row.type_personne_intitule if row.type_personne_id else None,
            "nif": row.nif,
            "lieu_naissance": row.lieu_naissance,
            "date_naissance": row.date_naissance.isoformat() if row.date_naissance else None,
            "nationalite": row.nationalite,
            "profession": row.profession,
            "etat_civil": row.etat_civil,
            "telephone": row.telephone,
            "adresse_mail": row.adresse_mail,
            "niveau_etude": row.niveau_etude,
            "date_create": row.date_create.isoformat() if row.date_create else None,  # Included date_create
            "adresse": {
                "numero": row.adresse_numero,
                "avenue": row.avenue,
                "quartier": row.quartier,
                "commune": row.commune
            },
            "nom_responsable": row.nom_responsable
        } for row in person_results]

        return {
            "data": data,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fixed cartographie endpoint
# @router.get("/cartographie", tags=["Cartographie"])
# def get_cartographie(
#     commune: Optional[str] = Query(None),
#     quartier: Optional[str] = Query(None),
#     avenue: Optional[str] = Query(None),
#     rang: Optional[str] = Query(None),
#     nature: Optional[str] = Query(None),
#     usage: Optional[str] = Query(None),
#     usage_specifique: Optional[str] = Query(None),
#     type_donnee: Optional[str] = Query(None),
#     fk_agent: Optional[str] = Query(None),
#     current_user=Depends(get_current_active_user),
#     db: Session = Depends(get_db),
#     entity_type: Optional[str] = Query(None, alias="type"),
# ):
#     try:
#         # Base query with all necessary joins
#         base_query = """
#             SELECT 
#                 b.id AS bien_id, 
#                 b.coordinates AS bien_coordinates, 
#                 b.coord_corrige AS bien_coord_corrige,
#                 b.superficie AS bien_superficie, 
#                 b.date_create AS bien_date_create,
#                 p.id AS parcelle_id, 
#                 p.numero_parcellaire AS parcelle_numero,
#                 p.coordonnee_geographique AS parcelle_coordinates, 
#                 p.coord_corrige AS parcelle_coord_corrige,
#                 p.superficie_calculee AS parcelle_superficie,
#                 p.date_create AS parcelle_date_create,
#                 c.intitule AS commune,
#                 q.intitule AS quartier,
#                 av.intitule AS avenue,
#                 COALESCE(
#                     CONCAT(pp.nom, ' ', pp.prenom),  -- Parcelle owner
#                     CONCAT(mp.nom, ' ', mp.prenom)   -- Bien owner through menage
#                 ) AS nom_proprietaire,
#                 CONCAT(mp.nom, ' ', mp.prenom) AS bien_proprietaire,
#                 nb.intitule AS nature_bien,
#                 u.intitule AS unite,
#                 usg.intitule AS usage,
#                 us.intitule AS usage_specifique,
#                 CONCAT(per.nom, ' ', per.prenom) AS ajouter_par,
#                 r.intitule AS rang,
#                 pu.intitule AS parcelle_unite
#             FROM bien b
#             LEFT JOIN parcelle p ON b.fk_parcelle = p.id
#             LEFT JOIN personne pp ON p.fk_proprietaire = pp.id
#             LEFT JOIN menage m ON b.id = m.fk_bien
#             LEFT JOIN personne mp ON m.fk_personne = mp.id
#             LEFT JOIN adresse a ON p.fk_adresse = a.id
#             LEFT JOIN avenue av ON a.fk_avenue = av.id
#             LEFT JOIN quartier q ON av.fk_quartier = q.id
#             LEFT JOIN commune c ON q.fk_commune = c.id
#             LEFT JOIN rang r ON p.fk_rang = r.id
#             LEFT JOIN nature_bien nb ON b.fk_nature_bien = nb.id
#             LEFT JOIN unite u ON b.fk_unite = u.id
#             LEFT JOIN unite pu ON p.fk_unite = pu.id
#             LEFT JOIN usage usg ON b.fk_usage = usg.id
#             LEFT JOIN usage_specifique us ON b.fk_usage_specifique = us.id
#             LEFT JOIN personne per ON b.fk_agent = per.id
#             WHERE 1=1
#         """

#         # Add filters
#         filters = []
#         params = {}
#         if commune:
#             filters.append("c.id = :commune")
#             params["commune"] = commune
#         if quartier:
#             filters.append("q.id = :quartier")
#             params["quartier"] = quartier
#         if avenue:
#             filters.append("av.id = :avenue")
#             params["avenue"] = avenue
#         if rang:
#             filters.append("r.id = :rang")
#             params["rang"] = rang
#         if nature:
#             filters.append("nb.id = :nature")
#             params["nature"] = nature
#         if usage:
#             filters.append("usg.id = :usage")
#             params["usage"] = usage
#         if usage_specifique:
#             filters.append("us.id = :usage_specifique")
#             params["usage_specifique"] = usage_specifique
#         if fk_agent:
#             try:
#                 agent_id = int(fk_agent)
#                 filters.append("(b.fk_agent = :fk_agent OR p.fk_agent = :fk_agent)")
#                 params["fk_agent"] = agent_id
#             except ValueError:
#                 raise HTTPException(status_code=400, detail="Invalid agent ID format")
        
#         # Build final query
#         query = base_query
#         if filters:
#             query += " AND " + " AND ".join(filters)

#         # Entity type filtering - CRITICAL FIX
#         if entity_type == "bien":
#             # Only fetch biens with their linked parcelles
#             query = query.replace("FROM bien b", "FROM bien b LEFT JOIN parcelle p ON b.fk_parcelle = p.id")
#         elif entity_type == "parcelle":
#             # Only fetch parcelles and their linked biens
#             query = query.replace("FROM bien b", "FROM parcelle p LEFT JOIN bien b ON b.fk_parcelle = p.id")
#         else:
#             # Fetch both with bien as primary
#             pass

#         # Add ordering
#         query += " ORDER BY COALESCE(b.id, p.id) DESC"

#         # Execute query
#         results = db.execute(text(query), params).fetchall()

#         # Format results
#         data = []
#         for row in results:
#             item = {}
            
#             # Process bien data if exists
#             if row.bien_id:
#                 item["bien"] = {
#                     "id": row.bien_id,
#                     "coordinates": parse_coordinates(row.bien_coordinates) if type_donnee == 'collected' else parse_coordinates(row.bien_coord_corrige),
#                     "superficie": row.bien_superficie,
#                     "date_create": row.bien_date_create.isoformat() if row.bien_date_create else None,
#                     "nature_bien": row.nature_bien,
#                     "unite": row.unite,
#                     "usage": row.usage,
#                     "usage_specifique": row.usage_specifique,
#                     "nom_proprietaire": row.bien_proprietaire
#                 }
            
#             # Process parcelle data if exists
#             if row.parcelle_id:
#                 item["parcelle"] = {
#                     "id": row.parcelle_id,
#                     "numero_parcellaire": row.parcelle_numero,
#                     "coordinates": parse_coordinates(row.parcelle_coordinates) if type_donnee == 'collected' else parse_coordinates(row.parcelle_coord_corrige),
#                     "superficie_calculee": row.parcelle_superficie,
#                     "date_create": row.parcelle_date_create.isoformat() if row.parcelle_date_create else None,
#                     "unite": row.parcelle_unite,
#                     "rang": row.rang,
#                     "nom_proprietaire": row.nom_proprietaire
#                 }
            
#             # Only add items that match the entity_type
#             if not entity_type:
#                 data.append(item)
#             elif entity_type == "bien" and "bien" in item:
#                 data.append({"bien": item["bien"]})
#             elif entity_type == "parcelle" and "parcelle" in item:
#                 data.append({"parcelle": item["parcelle"]})

#         return {
#             "data": data,
#             "total": len(data),
#         }

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# Fetch cartographie data
@router.get("/cartographie", tags=["Cartographie"])
def get_cartographie(
    commune: str = Query(None),
    quartier: str = Query(None),
    avenue: str = Query(None),
    rang: str = Query(None),
    nature: str = Query(None),
    usage: str = Query(None),
    usage_specifique: str = Query(None),
    type_donnee: str = Query(None),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Base query with all necessary joins
        base_query = """
            SELECT 
                b.id AS bien_id, b.coordinates AS bien_coordinates, b.coord_corrige AS bien_coord_corrige,
                b.superficie AS bien_superficie, b.date_create AS bien_date_create,
                p.id AS parcelle_id, p.numero_parcellaire AS parcelle_numero,
                p.coordonnee_geographique AS parcelle_coordinates, p.coord_corrige AS parcelle_coord_corrige,
                p.superficie_calculee AS parcelle_superficie,
                p.date_create AS parcelle_date_create,
                c.intitule AS commune,
                q.intitule AS quartier,
                av.intitule AS avenue,
                COALESCE(
                    CONCAT(pp.nom, ' ', pp.prenom),  -- Parcelle owner
                    CONCAT(mp.nom, ' ', mp.prenom)   -- Bien owner through menage
                ) AS nom_proprietaire,
                CONCAT(mp.nom, ' ', mp.prenom) AS bien_proprietaire,  -- Specific owner for bien
                nb.intitule AS nature_bien,
                u.intitule AS unite,
                usg.intitule AS usage,
                us.intitule AS usage_specifique,
                CONCAT(per.nom, ' ', per.prenom) AS ajouter_par,
                r.intitule AS rang,
                pu.intitule AS parcelle_unite
            FROM bien b
            LEFT JOIN parcelle p ON b.fk_parcelle = p.id
            LEFT JOIN personne pp ON p.fk_proprietaire = pp.id  -- Parcelle owner
            LEFT JOIN menage m ON b.id = m.fk_bien
            LEFT JOIN personne mp ON m.fk_personne = mp.id  -- Bien owner through menage
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN rang r ON p.fk_rang = r.id
            LEFT JOIN nature_bien nb ON b.fk_nature_bien = nb.id
            LEFT JOIN unite u ON b.fk_unite = u.id
            LEFT JOIN unite pu ON p.fk_unite = pu.id
            LEFT JOIN usage usg ON b.fk_usage = usg.id
            LEFT JOIN usage_specifique us ON b.fk_usage_specifique = us.id
            LEFT JOIN personne per ON b.fk_agent = per.id
            WHERE 1=1
        """

        # Add filters
        filters = []
        params = {}
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
        if nature:
            filters.append("nb.id = :nature")
            params["nature"] = nature
        if usage:
            filters.append("usg.id = :usage")
            params["usage"] = usage
        if usage_specifique:
            filters.append("us.id = :usage_specifique")
            params["usage_specifique"] = usage_specifique

        # Build final query
        query = base_query
        if filters:
            query += " AND " + " AND ".join(filters)

        # Add ordering
        query += " ORDER BY b.id DESC"

        # Execute query
        results = db.execute(text(query), params).fetchall()

        # Format results
        data = []
        for row in results:
            data.append({
                "bien": {
                    "id": row.bien_id,
                    "coordinates": parse_coordinates(row.bien_coordinates) if type_donnee == 'collected' else parse_coordinates(row.bien_coord_corrige),
                    "superficie": row.bien_superficie,
                    "date_create": row.bien_date_create.isoformat() if row.bien_date_create else None,
                    "nature_bien": row.nature_bien,
                    "unite": row.unite,
                    "usage": row.usage,
                    "usage_specifique": row.usage_specifique,
                    "nom_proprietaire": row.bien_proprietaire  # Added bien_proprietaire
                },
                "parcelle": {
                    "id": row.parcelle_id,
                    "numero_parcellaire": row.parcelle_numero,
                    "coordinates": parse_coordinates(row.parcelle_coordinates) if type_donnee == 'collected' else parse_coordinates(row.parcelle_coord_corrige),
                    "superficie_calculee": row.parcelle_superficie,
                    "date_create": row.parcelle_date_create.isoformat() if row.parcelle_date_create else None,
                    "unite": row.parcelle_unite,
                    "rang": row.rang,
                    "nom_proprietaire": row.nom_proprietaire
                },
                "ajouter_par": row.ajouter_par,
                "commune": row.commune,
                "quartier": row.quartier,
                "avenue": row.avenue
            })

        return {
            "data": data,
            "total": len(data),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# Fetch dashboard statistics
@router.get("/stats/dashboard", tags=["Stats"])
def get_dashboard_stats(
    commune: str = Query(None),
    quartier: str = Query(None),
    avenue: str = Query(None),
    rang: str = Query(None),
    nature: str = Query(None),
    date_start: str = Query(None),
    date_end: str = Query(None),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Base query for parcelles
        parcelle_query = """
            SELECT p.id
            FROM parcelle p
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN rang r ON p.fk_rang = r.id
            WHERE 1=1
        """

        # Base query for biens
        bien_query = """
            SELECT b.id
            FROM bien b
            LEFT JOIN parcelle p ON b.fk_parcelle = p.id
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN rang r ON p.fk_rang = r.id
            LEFT JOIN nature_bien nb ON b.fk_nature_bien = nb.id
            LEFT JOIN usage u ON b.fk_usage = u.id
            LEFT JOIN usage_specifique us ON b.fk_usage_specifique = us.id
            WHERE 1=1
        """

        # Add filters
        filters = []
        params = {}
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
        if nature:
            filters.append("nb.id = :nature")
            params["nature"] = nature
        if date_start:
            filters.append("CAST(p.date_create AS DATE) >= CAST(:date_start AS DATE)")
            params["date_start"] = date_start
        if date_end:
            filters.append("CAST(p.date_create AS DATE) <= CAST(:date_end AS DATE)")
            params["date_end"] = date_end

        # Build final queries
        if filters:
            parcelle_query += " AND " + " AND ".join(filters)
            bien_query += " AND " + " AND ".join(filters)

        # Get total parcelles - split into accessible and inaccessible
        parcelle_accessibility_query = f"""
            SELECT 
                SUM(CASE WHEN p.statut = 1 THEN 1 ELSE 0 END) AS accessible,
                SUM(CASE WHEN p.statut = 2 THEN 1 ELSE 0 END) AS inaccessible
            FROM ({parcelle_query}) AS filtered_parcelles
            JOIN parcelle p ON filtered_parcelles.id = p.id
        """
        parcelle_accessibility = db.execute(text(parcelle_accessibility_query), params).fetchone()
        total_parcelles_accessibles = parcelle_accessibility[0] or 0
        total_parcelles_inaccessibles = parcelle_accessibility[1] or 0

        # Get total biens
        total_biens = db.execute(text(f"SELECT COUNT(*) FROM ({bien_query}) AS total"), params).scalar()

        # Get total proprietaires
        proprietaire_query = f"""
            SELECT COUNT(DISTINCT p.fk_proprietaire)
            FROM ({parcelle_query}) AS filtered_parcelles
            JOIN parcelle p ON filtered_parcelles.id = p.id
            WHERE p.fk_proprietaire IS NOT NULL
        """
        total_proprietaires = db.execute(text(proprietaire_query), params).scalar()

        # Get total population
        population_query = f"""
            WITH filtered_parcelles AS ({parcelle_query})
            SELECT COUNT(DISTINCT person_id) FROM (
                SELECT p.fk_proprietaire AS person_id
                FROM filtered_parcelles fp
                JOIN parcelle p ON fp.id = p.id
                WHERE p.fk_proprietaire IS NOT NULL
                UNION
                SELECT m.fk_personne AS person_id
                FROM filtered_parcelles fp
                JOIN bien b ON fp.id = b.fk_parcelle
                JOIN menage m ON b.id = m.fk_bien
                WHERE m.fk_personne IS NOT NULL
                UNION
                SELECT lb.fk_personne AS person_id
                FROM filtered_parcelles fp
                JOIN bien b ON fp.id = b.fk_parcelle
                JOIN location_bien lb ON b.id = lb.fk_bien
                WHERE lb.fk_personne IS NOT NULL
                UNION
                SELECT mm.fk_personne AS person_id
                FROM filtered_parcelles fp
                JOIN bien b ON fp.id = b.fk_parcelle
                JOIN menage m ON b.id = m.fk_bien
                JOIN membre_menage mm ON m.id = mm.fk_menage
                WHERE mm.fk_personne IS NOT NULL
            ) AS all_persons
        """
        total_population = db.execute(text(population_query), params).scalar()

        # Get biens by nature
        biens_by_nature_query = f"""
            WITH all_natures AS (
                SELECT id, intitule 
                FROM nature_bien
            ),
            filtered_biens AS ({bien_query})
            SELECT 
                an.intitule, 
                COUNT(fb.id) AS bien_count
            FROM all_natures an
            LEFT JOIN bien b ON an.id = b.fk_nature_bien
            LEFT JOIN filtered_biens fb ON b.id = fb.id
            GROUP BY an.id, an.intitule
        """
        biens_by_nature = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(biens_by_nature_query), params).fetchall()
        }

        # Get biens by rang
        biens_by_rang_query = f"""
            SELECT r.intitule, COUNT(b.id)
            FROM ({bien_query}) AS filtered_biens
            JOIN bien b ON filtered_biens.id = b.id
            LEFT JOIN parcelle p ON b.fk_parcelle = p.id
            LEFT JOIN rang r ON p.fk_rang = r.id
            GROUP BY r.intitule
        """
        biens_by_rang = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(biens_by_rang_query), params).fetchall()
        }

        # Get biens by usage
        biens_by_usage_query = f"""
            WITH all_usages AS (
                SELECT id, intitule 
                FROM usage
            ),
            filtered_biens AS ({bien_query})
            SELECT 
                au.intitule, 
                COUNT(fb.id) AS bien_count
            FROM all_usages au
            LEFT JOIN bien b ON au.id = b.fk_usage
            LEFT JOIN filtered_biens fb ON b.id = fb.id
            GROUP BY au.id, au.intitule
        """
        biens_by_usage = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(biens_by_usage_query), params).fetchall()
        }

        # Get biens by usage_specifique
        biens_by_usage_specifique_query = f"""
            WITH all_usage_specifiques AS (
                SELECT id, intitule 
                FROM usage_specifique
            ),
            filtered_biens AS ({bien_query})
            SELECT 
                aus.intitule, 
                COUNT(fb.id) AS bien_count
            FROM all_usage_specifiques aus
            LEFT JOIN bien b ON aus.id = b.fk_usage_specifique
            LEFT JOIN filtered_biens fb ON b.id = fb.id
            GROUP BY aus.id, aus.intitule
        """
        biens_by_usage_specifique = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(biens_by_usage_specifique_query), params).fetchall()
        }

        # Get parcelles by rang
        parcelles_by_rang_query = f"""
            WITH all_rangs AS (
                SELECT id, intitule 
                FROM rang
            ),
            filtered_parcelles AS ({parcelle_query})
            SELECT 
                ar.intitule, 
                COUNT(fp.id) AS parcelle_count
            FROM all_rangs ar
            LEFT JOIN parcelle p ON ar.id = p.fk_rang
            LEFT JOIN filtered_parcelles fp ON p.id = fp.id
            GROUP BY ar.id, ar.intitule
        """
        parcelles_by_rang = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(parcelles_by_rang_query), params).fetchall()
        }

        # Get parcelles by commune
        parcelles_by_commune_query = f"""
            WITH all_communes AS (
                SELECT id, intitule 
                FROM commune 
                WHERE fk_ville = 1
            ),
            filtered_parcelles AS ({parcelle_query})
            SELECT 
                ac.intitule, 
                COUNT(fp.id) AS parcelle_count
            FROM all_communes ac
            LEFT JOIN quartier q ON ac.id = q.fk_commune
            LEFT JOIN avenue av ON q.id = av.fk_quartier
            LEFT JOIN adresse a ON av.id = a.fk_avenue
            LEFT JOIN parcelle p ON a.id = p.fk_adresse
            LEFT JOIN filtered_parcelles fp ON p.id = fp.id
            GROUP BY ac.id, ac.intitule
        """
        parcelles_by_commune = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(parcelles_by_commune_query), params).fetchall()
        }

        # Get parcelles by quartier
        parcelles_by_quartier_query = f"""
            SELECT 
                CONCAT(q.intitule, ' (', c.intitule, ')') AS quartier_commune, 
                COUNT(p.id) AS count
            FROM ({parcelle_query}) AS filtered_parcelles
            JOIN parcelle p ON filtered_parcelles.id = p.id
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            GROUP BY q.intitule, c.intitule
        """
        parcelles_by_quartier = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(parcelles_by_quartier_query), params).fetchall()
        }

        # Get parcelles by avenue
        parcelles_by_avenue_query = f"""
            SELECT 
                CONCAT(av.intitule, ' (', c.intitule, ')') AS avenue_commune, 
                COUNT(p.id) AS count
            FROM ({parcelle_query}) AS filtered_parcelles
            JOIN parcelle p ON filtered_parcelles.id = p.id
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            GROUP BY av.intitule, c.intitule
        """
        parcelles_by_avenue = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(parcelles_by_avenue_query), params).fetchall()
        }

        return {
            "total_parcelles_accessibles": total_parcelles_accessibles,
            "total_parcelles_inaccessibles": total_parcelles_inaccessibles,
            "total_biens": total_biens,
            "total_proprietaires": total_proprietaires,
            "total_population": total_population,
            "biens_by_nature": biens_by_nature,
            "biens_by_rang": biens_by_rang,
            "biens_by_usage": biens_by_usage,
            "biens_by_usage_specifique": biens_by_usage_specifique,
            "parcelles_by_rang": parcelles_by_rang,
            "parcelles_by_commune": parcelles_by_commune,
            "parcelles_by_quartier": parcelles_by_quartier,
            "parcelles_by_avenue": parcelles_by_avenue,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Import GeoJSON data
@router.post("/import-geojson", tags=["GeoJSON"])
async def import_geojson(
    type: str = Query(..., regex="^(parcelle|bien)$"),
    file: UploadFile = File(...),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Import a GeoJSON file and update Parcelle or Bien records based on the id in each feature.
    """
    try:
        content = await file.read()
        geojson = json.loads(content)
        if geojson.get("type") != "FeatureCollection":
            raise HTTPException(status_code=400, detail="Le fichier n'est pas un FeatureCollection GeoJSON valide.")

        updated_ids = []
        for feature in geojson.get("features", []):
            props = feature.get("properties", {})
            obj_id = props.get("id")
            geometry = feature.get("geometry", {})
            coordinates = geometry.get("coordinates")

            if not obj_id or not coordinates:
                continue  # skip invalid features

            if type == "parcelle":
                parcelle = db.query(Parcelle).filter(Parcelle.id == obj_id).first()
                if parcelle:
                    parcelle.coord_corrige = json.dumps(coordinates)  # Store as JSON string
                    updated_ids.append(obj_id)
            elif type == "bien":
                bien = db.query(Bien).filter(Bien.id == obj_id).first()
                if bien:
                    bien.coord_corrige = json.dumps(coordinates)  # Store as JSON string
                    updated_ids.append(obj_id)

        db.commit()
        return {"updated": updated_ids, "count": len(updated_ids)}

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Erreur lors de l'import: {str(e)}")
    

@router.get("/recherche-utilisateur/{code_chasuble}", tags=["Users"])
def get_user_by_code_chasuble(
    code_chasuble: str,
    db: Session = Depends(get_db)
):
    # Query to find user by code_chasuble with team and location information
    query = """
        SELECT 
            u.id, u.nom, u.postnom, u.prenom, u.date_create, u.code_chasuble, 
            u.photo_url, u.sexe, u.mail, u.telephone,
            e.id AS team_id, e.intitule AS team_name,
            q.id AS quartier_id, q.intitule AS quartier_name,
            c.id AS commune_id, c.intitule AS commune_name
        FROM utilisateur u
        LEFT JOIN agent_equipe ae ON u.id = ae.fk_agent
        LEFT JOIN equipe e ON ae.fk_equipe = e.id
        LEFT JOIN quartier q ON e.fk_quartier = q.id
        LEFT JOIN commune c ON q.fk_commune = c.id
        WHERE u.code_chasuble = :code_chasuble
    """
    results = db.execute(text(query), {"code_chasuble": code_chasuble}).fetchall()
    
    if not results:
        return {
            "status": 204,
            "data": None,
            "message": "Utilisateur non trouvé"
        }
        
    # Format teams with location information
    teams = []
    for row in results:
        if row.team_id:
            team_info = {
                "nom_equipe": row.team_name,
                "lieu_affectation": {
                    "quartier": row.quartier_name,
                    "commune": row.commune_name
                }
            }
            teams.append(team_info)
    
    # Get first row for user details
    first_row = results[0]
        
    return {
        "status": 200,
        "data": {
            "id": first_row.id,
            "nom": first_row.nom,
            "postnom": first_row.postnom,
            "prenom": first_row.prenom,
            "email": first_row.mail,
            "telephone": first_row.telephone,
            "sexe": first_row.sexe,
            "date_create": first_row.date_create.isoformat() if first_row.date_create else None,
            "code_chasuble": first_row.code_chasuble,
            "photo_url": first_row.photo_url,
            "equipes": teams
        }
    }


@router.post("/teams", tags=["Teams"])
def create_team(
    team_data: TeamCreate,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    try:
        # Check if a team already exists with this fk_quartier
        existing_team = db.query(Equipe).filter(Equipe.fk_quartier == team_data.fk_quartier).first()
        if existing_team:
            raise HTTPException(
                status_code=400,
                detail="Un quartier ne peut être associé qu'à une seule équipe"
            )

        # Create new team
        new_team = Equipe(intitule=team_data.intitule, fk_quartier=team_data.fk_quartier)
        db.add(new_team)
        db.commit()
        db.refresh(new_team)
        return {"id": new_team.id, "intitule": new_team.intitule, "fk_quartier": new_team.fk_quartier}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    
@router.post("/assign-to-teams", tags=["Teams"])
def assign_to_teams(
    assign_user_teams: AssignUserTeams,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    try:
        # First, remove any existing team assignments for this user
        db.query(AgentEquipe).filter(AgentEquipe.fk_agent == assign_user_teams.user_id).delete()
        
        # Add new team assignments
        for team_id in assign_user_teams.team_ids:
            # Verify team exists
            team = db.query(Equipe).filter(Equipe.id == team_id).first()
            if not team:
                raise HTTPException(
                    status_code=404,
                    detail=f"Team with ID {team_id} not found"
                )
            
            # Create new assignment
            new_assignment = AgentEquipe(
                fk_equipe=team_id,
                fk_agent=assign_user_teams.user_id
            )
            db.add(new_assignment)
        
        db.commit()
        
        return {
            "message": "User successfully assigned to teams",
            "user_id": assign_user_teams.user_id,
            "team_ids": assign_user_teams.team_ids
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/teams", tags=["Teams"])
def get_teams(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=1000),
    date_start: str = Query(None),
    date_end: str = Query(None),
    fk_quartier: int = Query(None),
    intitule: str = Query(None),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    try:
        # Base query with member count
        query = """
            SELECT 
                e.id, 
                e.intitule, 
                e.fk_quartier,
                q.intitule AS quartier_intitule,
                COUNT(ae.fk_agent) AS member_count
            FROM equipe e
            LEFT JOIN quartier q ON e.fk_quartier = q.id
            LEFT JOIN agent_equipe ae ON e.id = ae.fk_equipe
            WHERE 1=1
        """

        # Add filters
        filters = []
        params = {}
        if date_start:
            filters.append("CAST(e.date_create AS DATE) >= CAST(:date_start AS DATE)")
            params["date_start"] = date_start
        if date_end:
            filters.append("CAST(e.date_create AS DATE) <= CAST(:date_end AS DATE)")
            params["date_end"] = date_end
        if fk_quartier:
            filters.append("e.fk_quartier = :fk_quartier")
            params["fk_quartier"] = fk_quartier
        if intitule:
            filters.append("e.intitule LIKE :intitule")
            params["intitule"] = f"%{intitule}%"

        # Build final query
        if filters:
            query += " AND " + " AND ".join(filters)

        # Add GROUP BY clause
        query += " GROUP BY e.id, e.intitule, e.fk_quartier, q.intitule"

        # Count total records
        count_query = f"SELECT COUNT(*) FROM ({query}) AS total"
        total = db.execute(text(count_query), params).scalar()

        # Add pagination
        query += """
            ORDER BY e.id DESC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """
        params["limit"] = page_size
        params["offset"] = (page - 1) * page_size

        # Execute query
        results = db.execute(text(query), params).fetchall()

        # Format results with member_count
        teams = [{
            "id": row.id,
            "intitule": row.intitule,
            "fk_quartier": row.fk_quartier,
            "quartier_intitule": row.quartier_intitule,
            "member_count": row.member_count,
        } for row in results]

        return {
            "data": teams,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/teams/{team_id}/members", tags=["Teams"])
def get_team_members(
    team_id: int,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    try:
        # Query to get team members
        query = """
            SELECT 
                u.id, u.nom, u.postnom, u.prenom, u.mail, u.telephone, u.photo_url, u.sexe
            FROM utilisateur u
            JOIN agent_equipe ae ON u.id = ae.fk_agent
            WHERE ae.fk_equipe = :team_id
        """
        results = db.execute(text(query), {"team_id": team_id}).fetchall()
        
        # Format results
        members = [{
            "id": row.id,
            "nom": row.nom,
            "postnom": row.postnom,
            "prenom": row.prenom,
            "mail": row.mail,
            "telephone": row.telephone,
            "photo_url": row.photo_url,
            "sexe": row.sexe,
        } for row in results]

        return {"data": members}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fetch-kobo-users", tags=["Kobo"])
async def fetch_kobo_users(
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Route pour récupérer et afficher les utilisateurs depuis KoboToolbox et les synchroniser dans la base de données locale
    """
    KOBOTOOLBOX_URL = "http://kf.hidscollect.hologram.cd"
    KOBOTOOLBOX_ADMIN_USER = "super_admin"
    KOBOTOOLBOX_ADMIN_PASSWORD = "xy9AhsnsRI7My2fIDYgX"

    kobotoolbox_api_url = f"{KOBOTOOLBOX_URL}/api/v2/users/"
    headers = {'Content-Type': 'application/json'}

    try:
        # Fetch users from KoboToolbox API
        response = requests.get(
            kobotoolbox_api_url,
            headers=headers,
            auth=(KOBOTOOLBOX_ADMIN_USER, KOBOTOOLBOX_ADMIN_PASSWORD)
        )
        
        if response.status_code == 200:
            users = response.json()
            results = users.get("results", [])

            # Synchronize users with local database
            for user in results:
                username = user.get("username")
                if not username:
                    continue  # Skip if username is missing

                # Check if user exists in local database
                existing_user = db.query(Utilisateur).filter(Utilisateur.id_kobo == username).first()

                if not existing_user:
                    # Create new user in local database
                    new_user = Utilisateur(
                        id_kobo=username,
                        login=username,
                        mail=user.get("email", None),  # Optional: Save email if available
                    )
                    db.add(new_user)
            
            # Commit the transaction
            try:
                db.commit()
            except SQLAlchemyError as e:
                db.rollback()
                return {
                    "status": "error",
                    "message": "Erreur lors de l'enregistrement des utilisateurs dans la base de données",
                    "details": str(e)
                }

            return {"status": "success", "data": users}
        else:
            return {
                "status": "error",
                "message": "Échec de la récupération des utilisateurs",
                "details": response.text
            }
            
    except requests.exceptions.RequestException as e:
        return {
            "status": "error",
            "message": "Erreur de requête",
            "details": str(e)
        }


@router.put("/users/{user_id}", tags=["Users"])
async def update_user(
    user_id: int,
    user_data: UserUpdate,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Get the user to update
        user = db.query(Utilisateur).filter(Utilisateur.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Convert Pydantic model to dict and remove None values
        update_data = user_data.model_dump(exclude_unset=True)

        # Define allowed fields based on the UserUpdate schema
        allowed_fields = {
            'login', 'nom', 'postnom', 'prenom', 'mail', 'code_chasuble',
            'photo_url', 'sexe', 'telephone', 'fk_groupe'
        }

        # Update only the provided and allowed fields
        for key, value in update_data.items():
            if key in allowed_fields:
                setattr(user, key, value)

        # Commit the changes
        db.commit()
        db.refresh(user)

        return {
            "id": user.id,
            "login": user.login,
            "nom": user.nom,
            "postnom": user.postnom,
            "prenom": user.prenom,
            "mail": user.mail,
            "code_chasuble": user.code_chasuble,
            "photo_url": user.photo_url,
            "sexe": user.sexe,
            "telephone": user.telephone,
            "fk_groupe": user.fk_groupe
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/get-parameters", tags=["GeoJSON"])
def get_parameters(
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Fetch all parameters in parallel using SQLAlchemy's execute
        rangs_query = text("SELECT id, intitule FROM rang")
        natures_query = text("SELECT id, intitule FROM nature_bien")
        usages_query = text("SELECT id, intitule FROM usage")
        usage_specifiques_query = text("SELECT id, intitule FROM usage_specifique")

        # Execute all queries
        rangs = db.execute(rangs_query).fetchall()
        natures = db.execute(natures_query).fetchall()
        usages = db.execute(usages_query).fetchall()
        usage_specifiques = db.execute(usage_specifiques_query).fetchall()

        # Format results
        return {
            "rangs": [{"id": row[0], "intitule": row[1]} for row in rangs],
            "natures": [{"id": row[0], "intitule": row[1]} for row in natures],
            "usages": [{"id": row[0], "intitule": row[1]} for row in usages],
            "usage_specifiques": [{"id": row[0], "intitule": row[1]} for row in usage_specifiques],
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/populations/{personne_id}", tags=["Populations"])
def get_personne(
    personne_id: int,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Updated query to handle district, territoire, secteur, and village as strings
        personne_query = """
            SELECT 
                p.id, p.nom, p.postnom, p.prenom, p.denomination, p.sigle, p.fk_lien_parente,
                p.nif, p.domaine_activite, p.lieu_naissance, p.date_naissance, p.profession,
                p.etat_civil, p.telephone, p.adresse_mail, p.niveau_etude, p.sexe,
                p.numero_impot, p.rccm, p.id_nat, p.type_piece_identite, p.numero_piece_identite,
                p.nom_du_pere, p.nom_de_la_mere, p.nombre_enfant,
                p.district, p.territoire, p.secteur, p.village,
                tp.id AS type_personne_id, tp.intitule AS type_personne_intitule,
                n.id AS nationalite_id, n.intitule AS nationalite,
                a.id AS adresse_id, a.numero AS adresse_numero,
                av.id AS avenue_id, av.intitule AS avenue,
                q.id AS quartier_id, q.intitule AS quartier,
                c.id AS commune_id, c.intitule AS commune,
                pr.id AS province_id, pr.intitule AS province,
                CASE
                    WHEN p.id IN (
                        SELECT p.fk_proprietaire
                        FROM parcelle p
                        WHERE p.fk_proprietaire = :personne_id
                    ) THEN 'Propriétaire'
                    WHEN p.id IN (
                        SELECT m.fk_personne
                        FROM bien b
                        JOIN menage m ON b.id = m.fk_bien
                        WHERE m.fk_personne = :personne_id
                    ) THEN 'Responsable menage'
                    WHEN p.id IN (
                        SELECT mm.fk_personne
                        FROM bien b
                        JOIN menage m ON b.id = m.fk_bien
                        JOIN membre_menage mm ON m.id = mm.fk_menage
                        WHERE mm.fk_personne = :personne_id
                    ) THEN 'Membre menage'
                    ELSE 'Inconnu'
                END AS categorie,
                CASE
                    WHEN p.id IN (
                        SELECT mm.fk_personne
                        FROM bien b
                        JOIN menage m ON b.id = m.fk_bien
                        JOIN membre_menage mm ON m.id = mm.fk_menage
                        WHERE mm.fk_personne = :personne_id
                    ) THEN (
                        SELECT TOP 1 CONCAT(rp.nom, ' ', rp.prenom)
                        FROM menage m
                        JOIN personne rp ON m.fk_personne = rp.id
                        WHERE m.id = (
                            SELECT TOP 1 mm.fk_menage
                            FROM membre_menage mm
                            WHERE mm.fk_personne = p.id
                        )
                    )
                    ELSE NULL
                END AS nom_responsable
            FROM personne p
            LEFT JOIN type_personne tp ON p.fk_type_personne = tp.id
            LEFT JOIN nationalite n ON p.fk_nationalite = n.id
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN ville v ON c.fk_ville = v.id  -- Join ville first
            LEFT JOIN province pr ON v.fk_province = pr.id  -- Then join province through ville
            WHERE p.id = :personne_id
        """

        # Execute query
        result = db.execute(text(personne_query), {"personne_id": personne_id}).first()

        if not result:
            raise HTTPException(status_code=404, detail="Personne not found")

        # Format result with corrected address fields
        personne_data = {
            "id": result.id,
            "nom": result.nom,
            "postnom": result.postnom,
            "prenom": result.prenom,
            "denomination": result.denomination,
            "sigle": result.sigle,
            "sexe": result.sexe,
            "categorie": result.categorie,
            "lien_de_famille": result.fk_lien_parente,
            "type_personne": result.type_personne_intitule if result.type_personne_id else None,
            "nif": result.nif,
            "numero_impot": result.numero_impot,
            "rccm": result.rccm,
            "id_nat": result.id_nat,
            "domaine_activite": result.domaine_activite,
            "lieu_naissance": result.lieu_naissance,
            "date_naissance": result.date_naissance.isoformat() if result.date_naissance else None,
            "nationalite": result.nationalite if result.nationalite_id else None,
            "profession": result.profession,
            "etat_civil": result.etat_civil,
            "telephone": result.telephone,
            "adresse_mail": result.adresse_mail,
            "niveau_etude": result.niveau_etude,
            "type_piece_identite": result.type_piece_identite,
            "numero_piece_identite": result.numero_piece_identite,
            "nom_du_pere": result.nom_du_pere,
            "nom_de_la_mere": result.nom_de_la_mere,
            "nombre_enfant": result.nombre_enfant,
            "district": result.district,
            "territoire": result.territoire,
            "secteur": result.secteur,
            "village": result.village,
            "adresse": {
                "id": result.adresse_id,
                "numero": result.adresse_numero,
                "avenue": result.avenue if result.avenue_id else None,
                "quartier": result.quartier if result.quartier_id else None,
                "commune": result.commune if result.commune_id else None,
                "province": result.province if result.province_id else None
            },
            "nom_responsable": result.nom_responsable
        }

        return personne_data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/modules", response_model=PaginatedModuleResponse, tags=["Modules"])
def get_modules(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    keyword: Optional[str] = None,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Base query
        query = "SELECT id, intitule FROM module WHERE 1=1"
        params = {}

        # Add keyword filter
        if keyword:
            query += " AND intitule LIKE :keyword"
            params["keyword"] = f"%{keyword}%"

        # Count total records
        count_query = f"SELECT COUNT(*) FROM ({query}) AS total"
        total = db.execute(text(count_query), params).scalar()

        # Add pagination
        query += """
            ORDER BY id DESC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """
        params["limit"] = page_size
        params["offset"] = (page - 1) * page_size

        # Execute query
        results = db.execute(text(query), params).fetchall()

        # Format results
        modules = [{"id": row[0], "intitule": row[1]} for row in results]

        return {
            "data": modules,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/modules", response_model=ModuleOut, tags=["Modules"])
def create_module(
    module_data: ModuleCreate,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Check if module already exists
        existing_module = db.query(Module).filter(Module.intitule == module_data.intitule).first()
        if existing_module:
            raise HTTPException(
                status_code=400,
                detail="Module with this name already exists"
            )

        # Create new module
        new_module = Module(
            intitule=module_data.intitule,
            fk_agent=current_user.id
        )
        db.add(new_module)
        db.commit()
        db.refresh(new_module)

        return new_module

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/modules/{module_id}", response_model=ModuleOut, tags=["Modules"])
def update_module(
    module_id: int,
    module_data: ModuleUpdate,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Get the module to update
        module = db.query(Module).filter(Module.id == module_id).first()
        if not module:
            raise HTTPException(status_code=404, detail="Module not found")

        if module_data.intitule:
            existing_module = db.query(Module).filter(
                Module.intitule == module_data.intitule,
                Module.id != module_id
            ).first()
            if existing_module:
                raise HTTPException(
                    status_code=400,
                    detail="Another module with this name already exists"
                )

            module.intitule = module_data.intitule

        db.commit()
        db.refresh(module)
        return module

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/modules/{module_id}", tags=["Modules"])
def delete_module(
    module_id: int,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Get the module to delete
        module = db.query(Module).filter(Module.id == module_id).first()
        if not module:
            raise HTTPException(status_code=404, detail="Module not found")

        # Delete the module
        db.delete(module)
        db.commit()

        return {
            "message": "Module deleted successfully",
            "id": module_id
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/groupes", response_model=PaginatedGroupeResponse, tags=["Groupes"])
def get_groupes(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    keyword: Optional[str] = None,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Base query
        query = """
            SELECT g.id, g.intitule, g.description, 
                   STRING_AGG(CAST(d.id AS VARCHAR), ',') AS droit_ids
            FROM groupe g
            LEFT JOIN groupe_droit gd ON g.id = gd.fk_groupe
            LEFT JOIN droit d ON gd.fk_droit = d.id
            WHERE 1=1
        """
        params = {}

        # Add keyword filter
        if keyword:
            query += " AND (g.intitule LIKE :keyword OR g.description LIKE :keyword)"
            params["keyword"] = f"%{keyword}%"

        # Group by clause
        query += " GROUP BY g.id, g.intitule, g.description"

        # Count total records - modified to use CTE
        count_query = f"""
            WITH filtered_groups AS (
                SELECT g.id
                FROM groupe g
                LEFT JOIN groupe_droit gd ON g.id = gd.fk_groupe
                LEFT JOIN droit d ON gd.fk_droit = d.id
                WHERE 1=1
                { " AND (g.intitule LIKE :keyword OR g.description LIKE :keyword)" if keyword else "" }
                GROUP BY g.id, g.intitule, g.description
            )
            SELECT COUNT(DISTINCT id) FROM filtered_groups
        """
        total = db.execute(text(count_query), params).scalar()

        # Add pagination
        query += """
            ORDER BY g.id DESC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """
        params["limit"] = page_size
        params["offset"] = (page - 1) * page_size

        # Execute query
        results = db.execute(text(query), params).fetchall()

        # Format results
        groupes = [{
            "id": row[0],
            "intitule": row[1],
            "description": row[2],
            "droit_ids": [int(d) for d in (row[3].split(',') if row[3] else [])]  # Convert string to list of ints
        } for row in results]

        return {
            "data": groupes,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/groupes", response_model=GroupeOut, tags=["Groupes"])
def create_groupe(
    groupe_data: GroupeCreate,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Check if groupe already exists
        existing_groupe = db.query(Groupe).filter(Groupe.intitule == groupe_data.intitule).first()
        if existing_groupe:
            raise HTTPException(
                status_code=400,
                detail="Groupe with this name already exists"
            )

        # Create new groupe
        new_groupe = Groupe(
            intitule=groupe_data.intitule,
            description=groupe_data.description,
            fk_agent=current_user.id
        )
        db.add(new_groupe)
        db.commit()
        db.refresh(new_groupe)

        return new_groupe

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/groupes/{groupe_id}", response_model=GroupeOut, tags=["Groupes"])
def update_groupe(
    groupe_id: int,
    groupe_data: GroupeUpdate,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Get the groupe to update
        groupe = db.query(Groupe).filter(Groupe.id == groupe_id).first()
        if not groupe:
            raise HTTPException(status_code=404, detail="Groupe not found")

        if groupe_data.intitule:
            existing_groupe = db.query(Groupe).filter(
                Groupe.intitule == groupe_data.intitule,
                Groupe.id != groupe_id
            ).first()
            if existing_groupe:
                raise HTTPException(
                    status_code=400,
                    detail="Another groupe with this name already exists"
                )

            groupe.intitule = groupe_data.intitule

        if groupe_data.description is not None:
            groupe.description = groupe_data.description

        db.commit()
        db.refresh(groupe)
        return groupe

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/groupes/{groupe_id}", tags=["Groupes"])
def delete_groupe(
    groupe_id: int,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Get the groupe to delete
        groupe = db.query(Groupe).filter(Groupe.id == groupe_id).first()
        if not groupe:
            raise HTTPException(status_code=404, detail="Groupe not found")

        # Delete the groupe
        db.delete(groupe)
        db.commit()

        return {
            "message": "Groupe deleted successfully",
            "id": groupe_id
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/droits", tags=["Droits"])
def get_droits(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=1000),
    keyword: Optional[str] = None,
    fk_module: Optional[int] = None,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Base query with module join
        query = """
            SELECT d.id, d.code, d.intitule, d.fk_module, m.intitule as module_intitule
            FROM droit d
            LEFT JOIN module m ON d.fk_module = m.id
                WHERE 1=1
        """
        params = {}

        # Add keyword filter
        if keyword:
            query += " AND (d.code LIKE :keyword OR d.intitule LIKE :keyword)"
            params["keyword"] = f"%{keyword}%"

        # Add module filter
        if fk_module:
            query += " AND d.fk_module = :fk_module"
            params["fk_module"] = fk_module

        # Count total records
        count_query = f"SELECT COUNT(*) FROM ({query}) AS total"
        total = db.execute(text(count_query), params).scalar()

        # Add pagination
        query += """
            ORDER BY d.id DESC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """
        params["limit"] = page_size
        params["offset"] = (page - 1) * page_size

        # Execute query
        results = db.execute(text(query), params).fetchall()

        # Format results with module information
        droits = [{
            "id": row[0],
            "code": row[1],
            "intitule": row[2],
            "fk_module": row[3],
            "module": {
                "id": row[3],
                "intitule": row[4]
            } if row[3] else None
        } for row in results]

        return {
            "data": droits,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/droits", response_model=DroitOut, tags=["Droits"])
def create_droit(
    droit_data: DroitCreate,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Check if droit already exists
        existing_droit = db.query(Droit).filter(
            (Droit.code == droit_data.code) | (Droit.intitule == droit_data.intitule)
        ).first()
        if existing_droit:
            raise HTTPException(
                status_code=400,
                detail="Droit with this code or name already exists"
            )

        # Create new droit
        new_droit = Droit(
            code=droit_data.code,
            intitule=droit_data.intitule,
            fk_module=droit_data.fk_module
        )
        db.add(new_droit)
        db.commit()
        db.refresh(new_droit)

        return new_droit

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/droits/{droit_id}", response_model=DroitOut, tags=["Droits"])
def update_droit(
    droit_id: int,
    droit_data: DroitUpdate,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Get the droit to update
        droit = db.query(Droit).filter(Droit.id == droit_id).first()
        if not droit:
            raise HTTPException(status_code=404, detail="Droit not found")

        if droit_data.code or droit_data.intitule:
            existing_droit = db.query(Droit).filter(
                ((Droit.code == droit_data.code) if droit_data.code else False) |
                ((Droit.intitule == droit_data.intitule) if droit_data.intitule else False),
                Droit.id != droit_id
            ).first()
            if existing_droit:
                raise HTTPException(
                    status_code=400,
                    detail="Another droit with this code or name already exists"
                )

        if droit_data.code:
            droit.code = droit_data.code
        if droit_data.intitule:
            droit.intitule = droit_data.intitule
        if droit_data.fk_module:
            droit.fk_module = droit_data.fk_module
            
        db.commit()
        db.refresh(droit)
        return droit

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/droits/{droit_id}", tags=["Droits"])
def delete_droit(
    droit_id: int,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Get the droit to delete
        droit = db.query(Droit).filter(Droit.id == droit_id).first()
        if not droit:
            raise HTTPException(status_code=404, detail="Droit not found")

        # Delete the droit
        db.delete(droit)
        db.commit()

        return {
            "message": "Droit deleted successfully",
            "id": droit_id
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/groupes/{groupe_id}/assign-droits", tags=["Groupes"])
def assign_droits_to_groupe(
    groupe_id: int,
    assign_data: AssignDroitsToEntity,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Verify groupe exists
        groupe = db.query(Groupe).filter(Groupe.id == groupe_id).first()
        if not groupe:
            raise HTTPException(status_code=404, detail="Groupe not found")

        # Remove existing assignments for this groupe
        db.query(GroupeDroit).filter(GroupeDroit.fk_groupe == groupe_id).delete()

        # Create new assignments
        for droit_id in assign_data.droit_ids:
            # Verify droit exists
            droit = db.query(Droit).filter(Droit.id == droit_id).first()
            if not droit:
                raise HTTPException(
                    status_code=404,
                    detail=f"Droit with ID {droit_id} not found"
                )
            
            # Create new assignment
            new_assignment = GroupeDroit(
                fk_groupe=groupe_id,
                fk_droit=droit_id
            )
            db.add(new_assignment)
        
        db.commit()
        
        return {
            "message": "Droits successfully assigned to groupe",
            "groupe_id": groupe_id,
            "droit_ids": assign_data.droit_ids
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/users/{user_id}/assign-droits", tags=["Users"])
def assign_droits_to_user(
    user_id: int,
    assign_data: AssignDroitsToEntity,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Verify user exists
        user = db.query(Utilisateur).filter(Utilisateur.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Remove existing assignments for this user
        db.query(UtilisateurDroit).filter(UtilisateurDroit.fk_utilisateur == user_id).delete()

        # Create new assignments
        for droit_id in assign_data.droit_ids:
            # Verify droit exists
            droit = db.query(Droit).filter(Droit.id == droit_id).first()
            if not droit:
                raise HTTPException(
                    status_code=404,
                    detail=f"Droit with ID {droit_id} not found"
                )
            
            # Create new assignment
            new_assignment = UtilisateurDroit(
                fk_utilisateur=user_id,
                fk_droit=droit_id
            )
            db.add(new_assignment)
        
        db.commit()
        
        return {
            "message": "Droits successfully assigned to user",
            "user_id": user_id,
            "droit_ids": assign_data.droit_ids
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/menages", tags=["Menages"])
def get_menages(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    commune: Optional[str] = Query(None),
    quartier: Optional[str] = Query(None),
    avenue: Optional[str] = Query(None),
    rang: Optional[str] = Query(None),
    date_start: str = Query(..., description="Start date in format YYYY-MM-DD", regex=r"^\d{4}-\d{2}-\d{2}$"),
    date_end: str = Query(..., description="End date in format YYYY-MM-DD", regex=r"^\d{4}-\d{2}-\d{2}$"),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Base query to get menage information with SQL Server pagination
        menage_query = """
            SELECT 
                m.id AS menage_id,
                p.id AS proprietaire_id,
                p.nom, p.postnom, p.prenom, p.date_naissance, p.sexe, 
                p.etat_civil, p.profession, p.niveau_etude, p.lieu_naissance,
                p.telephone, n.intitule AS nationalite,
                c.intitule AS commune, q.intitule AS quartier,
                av.intitule AS avenue, a.numero, r.intitule AS rang,
                fm.intitule AS lien_parente
            FROM menage m
            JOIN personne p ON m.fk_personne = p.id
            LEFT JOIN nationalite n ON p.fk_nationalite = n.id
            LEFT JOIN bien b ON m.fk_bien = b.id
            LEFT JOIN parcelle par ON b.fk_parcelle = par.id
            LEFT JOIN adresse a ON par.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN rang r ON par.fk_rang = r.id
            LEFT JOIN filiation_membre fm ON p.fk_lien_parente = fm.id
            WHERE p.fk_type_personne = 1
            AND CAST(p.date_create AS DATE) >= CAST(:date_start AS DATE)
            AND CAST(p.date_create AS DATE) <= CAST(:date_end AS DATE)
        """

        # Add filters
        filters = []
        params = {
            "date_start": date_start,
            "date_end": date_end,
            "offset": (page - 1) * page_size,
            "page_size": page_size
        }
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

        if filters:
            menage_query += " AND " + " AND ".join(filters)

        # Add SQL Server-compatible pagination
        menage_query += " ORDER BY m.id OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY"

        # Get total count for pagination
        count_query = """
            SELECT COUNT(*) 
            FROM menage m
            JOIN personne p ON m.fk_personne = p.id
            LEFT JOIN bien b ON m.fk_bien = b.id
            LEFT JOIN parcelle par ON b.fk_parcelle = par.id
            LEFT JOIN adresse a ON par.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN rang r ON par.fk_rang = r.id
            WHERE p.fk_type_personne = 1
            AND CAST(p.date_create AS DATE) >= CAST(:date_start AS DATE)
            AND CAST(p.date_create AS DATE) <= CAST(:date_end AS DATE)
        """
        if filters:
            count_query += " AND " + " AND ".join(filters)

        # Execute queries
        total = db.execute(text(count_query), params).scalar()
        menage_results = db.execute(text(menage_query), params).fetchall()

        # Get member data for each menage
        menage_data = []
        for menage in menage_results:
            member_query = """
                SELECT 
                    p.id AS member_id,
                    p.nom, p.postnom, p.prenom, p.date_naissance, p.sexe,
                    p.etat_civil, p.profession, p.niveau_etude, p.lieu_naissance,
                    p.telephone, n.intitule AS nationalite,
                    fm.intitule AS lien_parente
                FROM membre_menage mm
                JOIN personne p ON mm.fk_personne = p.id
                LEFT JOIN nationalite n ON p.fk_nationalite = n.id
                LEFT JOIN filiation_membre fm ON mm.fk_filiation = fm.id
                WHERE mm.fk_menage = :menage_id
            """
            members = db.execute(text(member_query), {"menage_id": menage.menage_id}).fetchall()

            # Format menage data
            menage_data.append({
                "id": menage.menage_id,
                "proprietaire": {
                    "id": menage.proprietaire_id,
                    "nom": menage.nom,
                    "postnom": menage.postnom,
                    "prenom": menage.prenom,
                    "date_naissance": menage.date_naissance.isoformat() if menage.date_naissance else None,
                    "sexe": menage.sexe,
                    "etat_civil": menage.etat_civil,
                    "profession": menage.profession,
                    "niveau_etude": menage.niveau_etude,
                    "lieu_naissance": menage.lieu_naissance,
                    "nationalite": menage.nationalite,
                    "telephone": menage.telephone,
                    "lien_parente": menage.lien_parente
                },
                "membres": [{
                    "id": member.member_id,
                    "nom": member.nom,
                    "postnom": member.postnom,
                    "prenom": member.prenom,
                    "date_naissance": member.date_naissance.isoformat() if member.date_naissance else None,
                    "sexe": member.sexe,
                    "lien_parente": member.lien_parente,
                    "etat_civil": member.etat_civil,
                    "profession": member.profession,
                    "niveau_etude": member.niveau_etude,
                    "lieu_naissance": member.lieu_naissance,
                    "nationalite": member.nationalite,
                    "telephone": member.telephone
                } for member in members],
                "adresse": {
                    "commune": menage.commune,
                    "quartier": menage.quartier,
                    "avenue": menage.avenue,
                    "numero": menage.numero,
                    "rang": menage.rang
                }
            })

        return {
            "data": menage_data,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/agent-activity/{fk_agent}", tags=["Stats"])
def get_agent_activity_stats(
    fk_agent: int,
    date_debut: str = Query(..., description="Start date in format YYYY-MM-DD"),
    date_fin: str = Query(..., description="End date in format YYYY-MM-DD"),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Get agent activity statistics.
    Returns counts of Parcelle, Bien, Menage, and MembreMenage collected,
    along with agent information and location details.
    """
    # Get agent information
    agent = db.query(
        Utilisateur.nom,
        Utilisateur.postnom,
        Utilisateur.prenom
    ).filter(Utilisateur.id == fk_agent)\
     .first()

    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    # Try to get quartier information
    agent_equipe = db.query(Equipe.fk_quartier)\
        .join(AgentEquipe, AgentEquipe.fk_equipe == Equipe.id)\
        .filter(AgentEquipe.fk_agent == fk_agent)\
        .first()
    
    quartier = agent_equipe.fk_quartier if agent_equipe else None
    agent = {**agent._asdict(), "fk_quartier": quartier}

    # Get location information
    location = None
    if agent["fk_quartier"]:
        location = db.query(
            Commune.intitule.label("commune"),
            Quartier.intitule.label("quartier")
        ).join(Quartier, Quartier.fk_commune == Commune.id)\
         .filter(Quartier.id == agent["fk_quartier"])\
         .first()

    # Get activity counts with date range filter using cast
    parcelle_accessible_count = db.query(func.count(Parcelle.id)).filter(
        Parcelle.fk_agent == fk_agent,
        Parcelle.statut == 1,
        cast(Parcelle.date_create, Date).between(date_debut, date_fin)
    ).scalar() or 0

    parcelle_inaccessible_count = db.query(func.count(Parcelle.id)).filter(
        Parcelle.fk_agent == fk_agent,
        Parcelle.statut == 2,
        cast(Parcelle.date_create, Date).between(date_debut, date_fin)
    ).scalar() or 0

    bien_count = db.query(func.count(Bien.id)).filter(
        Bien.fk_agent == fk_agent,
        cast(Bien.date_create, Date).between(date_debut, date_fin)
    ).scalar() or 0

    menage_count = db.query(func.count(Menage.id)).filter(
        Menage.fk_agent == fk_agent,
        cast(Menage.date_create, Date).between(date_debut, date_fin)
    ).scalar() or 0

    membre_menage_count = db.query(func.count(MembreMenage.id)).filter(
        MembreMenage.fk_agent == fk_agent,
        cast(MembreMenage.date_create, Date).between(date_debut, date_fin)
    ).scalar() or 0

    # Get stats for Linear Scale Chart with date range filter using cast
    parcelle_accessible_stats = db.query(
        cast(Parcelle.date_create, Date).label("date"),
        func.count(Parcelle.id).label("parcelle_accessible_count")
    ).filter(
        Parcelle.fk_agent == fk_agent,
        Parcelle.statut == 1,
        cast(Parcelle.date_create, Date).between(date_debut, date_fin)
    ).group_by(cast(Parcelle.date_create, Date))\
     .all()

    parcelle_inaccessible_stats = db.query(
        cast(Parcelle.date_create, Date).label("date"),
        func.count(Parcelle.id).label("parcelle_inaccessible_count")
    ).filter(
        Parcelle.fk_agent == fk_agent,
        Parcelle.statut == 2,
        cast(Parcelle.date_create, Date).between(date_debut, date_fin)
    ).group_by(cast(Parcelle.date_create, Date))\
     .all()

    bien_stats = db.query(
        cast(Bien.date_create, Date).label("date"),
        func.count(Bien.id).label("bien_count")
    ).filter(
        Bien.fk_agent == fk_agent,
        cast(Bien.date_create, Date).between(date_debut, date_fin)
    ).group_by(cast(Bien.date_create, Date))\
     .all()

    menage_stats = db.query(
        cast(Menage.date_create, Date).label("date"),
        func.count(Menage.id).label("menage_count")
    ).filter(
        Menage.fk_agent == fk_agent,
        cast(Menage.date_create, Date).between(date_debut, date_fin)
    ).group_by(cast(Menage.date_create, Date))\
     .all()

    membre_menage_stats = db.query(
        cast(MembreMenage.date_create, Date).label("date"),
        func.count(MembreMenage.id).label("membre_menage_count")
    ).filter(
        MembreMenage.fk_agent == fk_agent,
        cast(MembreMenage.date_create, Date).between(date_debut, date_fin)
    ).group_by(cast(MembreMenage.date_create, Date))\
     .all()

    # Combine all stats into a single dictionary
    stats_dict = defaultdict(lambda: {
        "parcelle_accessible_count": 0,
        "parcelle_inaccessible_count": 0,
        "bien_count": 0,
        "menage_count": 0,
        "membre_menage_count": 0
    })

    for stat in parcelle_accessible_stats:
        stats_dict[stat.date]["parcelle_accessible_count"] = stat.parcelle_accessible_count

    for stat in parcelle_inaccessible_stats:
        stats_dict[stat.date]["parcelle_inaccessible_count"] = stat.parcelle_inaccessible_count

    for stat in bien_stats:
        stats_dict[stat.date]["bien_count"] = stat.bien_count

    for stat in menage_stats:
        stats_dict[stat.date]["menage_count"] = stat.menage_count

    for stat in membre_menage_stats:
        stats_dict[stat.date]["membre_menage_count"] = stat.membre_menage_count

    # Convert the dictionary to a list of stats
    stats = [
        {
            "date": date,
            "parcelle_accessible_count": counts["parcelle_accessible_count"],
            "parcelle_inaccessible_count": counts["parcelle_inaccessible_count"],
            "bien_count": counts["bien_count"],
            "menage_count": counts["menage_count"],
            "membre_menage_count": counts["membre_menage_count"]
        }
        for date, counts in sorted(stats_dict.items())
    ]

    return {
        "agent": {
            "nom": agent.get("nom"),
            "postnom": agent.get("postnom"),
            "prenom": agent.get("prenom"),
            "commune": location.commune if location else None,
            "quartier": location.quartier if location else None
        },
        "total_counts": {
            "parcelle_accessible": parcelle_accessible_count,
            "parcelle_inaccessible": parcelle_inaccessible_count,
            "bien": bien_count,
            "menage": menage_count,
            "membre_menage": membre_menage_count
        },
        "stats": stats
    }

@router.get("/stats/all-agents-activity", tags=["Stats"])
def get_all_agents_activity_stats(
    date_debut: str = Query(..., description="Start date in format YYYY-MM-DD"),
    date_fin: str = Query(..., description="End date in format YYYY-MM-DD"),
    keyword: str = Query(None, description="Search keyword for agent's name"),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Get activity statistics for all agents within a date range.
    Returns a paginated list of agents with their counts of Parcelle, Bien, Menage, and MembreMenage collected.
    """
    try:
        # Validate date formats
        start_date = datetime.strptime(date_debut, "%Y-%m-%d")
        end_date = datetime.strptime(date_fin, "%Y-%m-%d")
        if start_date > end_date:
            raise ValueError("Start date must be before or equal to end date")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Subquery for accessible Parcelle count (statut = 1)
    parcelle_accessible_subquery = db.query(
        Parcelle.fk_agent.label("agent_id"),
        func.count(Parcelle.id).label("parcelle_accessible_count")
    ).filter(
        and_(
            cast(Parcelle.date_create, Date) >= date_debut,
            cast(Parcelle.date_create, Date) <= date_fin,
            Parcelle.statut == 1
        )
    ).group_by(Parcelle.fk_agent).subquery()

    # Subquery for inaccessible Parcelle count (statut = 2)
    parcelle_inaccessible_subquery = db.query(
        Parcelle.fk_agent.label("agent_id"),
        func.count(Parcelle.id).label("parcelle_inaccessible_count")
    ).filter(
        and_(
            cast(Parcelle.date_create, Date) >= date_debut,
            cast(Parcelle.date_create, Date) <= date_fin,
            Parcelle.statut == 2
        )
    ).group_by(Parcelle.fk_agent).subquery()

    # Subquery for Bien count
    bien_subquery = db.query(
        Bien.fk_agent.label("agent_id"),
        func.count(Bien.id).label("bien_count")
    ).filter(
        and_(
            cast(Bien.date_create, Date) >= date_debut,
            cast(Bien.date_create, Date) <= date_fin
        )
    ).group_by(Bien.fk_agent).subquery()

    # Subquery for Menage count
    menage_subquery = db.query(
        Bien.fk_agent.label("agent_id"),
        func.count(Menage.id).label("menage_count")
    ).join(Menage, Menage.fk_bien == Bien.id)\
     .filter(
        and_(
            cast(Menage.date_create, Date) >= date_debut,
            cast(Menage.date_create, Date) <= date_fin
        )
    ).group_by(Bien.fk_agent).subquery()

    # Subquery for MembreMenage count
    membre_menage_subquery = db.query(
        Bien.fk_agent.label("agent_id"),
        func.count(MembreMenage.id).label("membre_menage_count")
    ).join(Menage, Menage.fk_bien == Bien.id)\
     .join(MembreMenage, MembreMenage.fk_menage == Menage.id)\
     .filter(
        and_(
            cast(MembreMenage.date_create, Date) >= date_debut,
            cast(MembreMenage.date_create, Date) <= date_fin
        )
    ).group_by(Bien.fk_agent).subquery()

    # Main query - update with new fields
    base_query = db.query(
        Utilisateur.id.label("agent_id"),
        Utilisateur.nom.label("nom"),
        Utilisateur.postnom.label("postnom"),
        Utilisateur.prenom.label("prenom"),
        func.coalesce(parcelle_accessible_subquery.c.parcelle_accessible_count, 0).label("parcelle_accessible_count"),
        func.coalesce(parcelle_inaccessible_subquery.c.parcelle_inaccessible_count, 0).label("parcelle_inaccessible_count"),
        func.coalesce(bien_subquery.c.bien_count, 0).label("bien_count"),
        func.coalesce(menage_subquery.c.menage_count, 0).label("menage_count"),
        func.coalesce(membre_menage_subquery.c.membre_menage_count, 0).label("membre_menage_count")
    ).outerjoin(parcelle_accessible_subquery, parcelle_accessible_subquery.c.agent_id == Utilisateur.id)\
     .outerjoin(parcelle_inaccessible_subquery, parcelle_inaccessible_subquery.c.agent_id == Utilisateur.id)\
     .outerjoin(bien_subquery, bien_subquery.c.agent_id == Utilisateur.id)\
     .outerjoin(menage_subquery, menage_subquery.c.agent_id == Utilisateur.id)\
     .outerjoin(membre_menage_subquery, membre_menage_subquery.c.agent_id == Utilisateur.id)

    # Apply keyword filter
    if keyword:
        base_query = base_query.filter(
            or_(
                Utilisateur.nom.ilike(f"%{keyword}%"),
                Utilisateur.postnom.ilike(f"%{keyword}%"),
                Utilisateur.prenom.ilike(f"%{keyword}%")
            )
        )

    # Add filter to exclude agents with all zero counts
    base_query = base_query.filter(
        or_(
            parcelle_accessible_subquery.c.parcelle_accessible_count > 0,
            parcelle_inaccessible_subquery.c.parcelle_inaccessible_count > 0,
            bien_subquery.c.bien_count > 0,
            menage_subquery.c.menage_count > 0,
            membre_menage_subquery.c.membre_menage_count > 0
        )
    )

    # Get total count
    total = base_query.count()

    # Apply pagination with ORDER BY
    agent_stats = base_query.order_by(Utilisateur.nom)\
                           .offset((page - 1) * page_size)\
                           .limit(page_size)\
                           .all()

    return {
        "date_debut": date_debut,
        "date_fin": date_fin,
        "data": [
            {
                "agent_id": stat.agent_id,
                "agent_name": f"{stat.prenom or ''} {stat.nom or ''} {stat.postnom or ''}".strip(),
                "parcelle_accessible_count": stat.parcelle_accessible_count,
                "parcelle_inaccessible_count": stat.parcelle_inaccessible_count,
                "bien_count": stat.bien_count,
                "menage_count": stat.menage_count,
                "membre_menage_count": stat.membre_menage_count
            }
            for stat in agent_stats
        ],
        "total": total,
        "page": page,
        "page_size": page_size
    }

@router.get("/stats/all-agents-activity-by-date", tags=["Stats"])
def get_all_agents_activity_stats_by_date(
    date_debut: str = Query(..., description="Start date in format YYYY-MM-DD"),
    date_fin: str = Query(..., description="End date in format YYYY-MM-DD"),
    keyword: str = Query(None, description="Search keyword for agent's name"),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Get activity statistics grouped by date and agent within a date range.
    Returns statistics grouped by date and agent with counts of Parcelle, Bien, Menage, and MembreMenage collected.
    """
    try:
        # Validate date formats
        start_date = datetime.strptime(date_debut, "%Y-%m-%d")
        end_date = datetime.strptime(date_fin, "%Y-%m-%d")
        if start_date > end_date:
            raise ValueError("Start date must be before or equal to end date")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Query for Parcelle count by date and agent
    parcelle_query = db.query(
        cast(Parcelle.date_create, Date).label("date"),
        Parcelle.fk_agent.label("agent_id"),
        func.count(Parcelle.id).label("parcelle_count")
    ).filter(
        and_(
            cast(Parcelle.date_create, Date) >= date_debut,
            cast(Parcelle.date_create, Date) <= date_fin
        )
    ).group_by(cast(Parcelle.date_create, Date), Parcelle.fk_agent).subquery()

    # Query for Bien count by date and agent
    bien_query = db.query(
        cast(Bien.date_create, Date).label("date"),
        Bien.fk_agent.label("agent_id"),
        func.count(Bien.id).label("bien_count")
    ).filter(
        and_(
            cast(Bien.date_create, Date) >= date_debut,
            cast(Bien.date_create, Date) <= date_fin
        )
    ).group_by(cast(Bien.date_create, Date), Bien.fk_agent).subquery()

    # Query for Menage count by date and agent
    menage_query = db.query(
        cast(Menage.date_create, Date).label("date"),
        Bien.fk_agent.label("agent_id"),
        func.count(Menage.id).label("menage_count")
    ).join(Bien, Menage.fk_bien == Bien.id)\
     .filter(
        and_(
            cast(Menage.date_create, Date) >= date_debut,
            cast(Menage.date_create, Date) <= date_fin
        )
    ).group_by(cast(Menage.date_create, Date), Bien.fk_agent).subquery()

    # Query for MembreMenage count by date and agent
    membre_menage_query = db.query(
        cast(MembreMenage.date_create, Date).label("date"),
        Bien.fk_agent.label("agent_id"),
        func.count(MembreMenage.id).label("membre_menage_count")
    ).join(Menage, MembreMenage.fk_menage == Menage.id)\
     .join(Bien, Menage.fk_bien == Bien.id)\
     .filter(
        and_(
            cast(MembreMenage.date_create, Date) >= date_debut,
            cast(MembreMenage.date_create, Date) <= date_fin
        )
    ).group_by(cast(MembreMenage.date_create, Date), Bien.fk_agent).subquery()

    # Main query to combine all counts by date and agent
    base_query = db.query(
        func.coalesce(parcelle_query.c.date, bien_query.c.date, menage_query.c.date, membre_menage_query.c.date).label("date"),
        func.coalesce(parcelle_query.c.agent_id, bien_query.c.agent_id, menage_query.c.agent_id, membre_menage_query.c.agent_id).label("agent_id"),
        Utilisateur.nom.label("nom"),
        Utilisateur.postnom.label("postnom"),
        Utilisateur.prenom.label("prenom"),
        func.coalesce(parcelle_query.c.parcelle_count, 0).label("parcelle_count"),
        func.coalesce(bien_query.c.bien_count, 0).label("bien_count"),
        func.coalesce(menage_query.c.menage_count, 0).label("menage_count"),
        func.coalesce(membre_menage_query.c.membre_menage_count, 0).label("membre_menage_count")
    ).outerjoin(bien_query, and_(bien_query.c.date == parcelle_query.c.date, bien_query.c.agent_id == parcelle_query.c.agent_id))\
     .outerjoin(menage_query, and_(menage_query.c.date == parcelle_query.c.date, menage_query.c.agent_id == parcelle_query.c.agent_id))\
     .outerjoin(membre_menage_query, and_(membre_menage_query.c.date == parcelle_query.c.date, membre_menage_query.c.agent_id == parcelle_query.c.agent_id))\
     .join(Utilisateur, Utilisateur.id == parcelle_query.c.agent_id)

    # Apply keyword filter
    if keyword:
        base_query = base_query.filter(
            or_(
                Utilisateur.nom.ilike(f"%{keyword}%"),
                Utilisateur.postnom.ilike(f"%{keyword}%"),
                Utilisateur.prenom.ilike(f"%{keyword}%")
            )
        )

    # Add filter to exclude agents with all zero counts
    base_query = base_query.filter(
        or_(
            parcelle_query.c.parcelle_count > 0,
            bien_query.c.bien_count > 0,
            menage_query.c.menage_count > 0,
            membre_menage_query.c.membre_menage_count > 0
        )
    )

    # Execute query and format results
    stats_by_date = base_query.order_by("date", "agent_id").all()

    return {
        "date_debut": date_debut,
        "date_fin": date_fin,
        "data": [
            {
                "date": stat.date.strftime("%Y-%m-%d"),
                "agent_id": stat.agent_id,
                "agent_name": f"{stat.prenom if stat.prenom else ''} {stat.nom if stat.nom else ''} {stat.postnom if stat.postnom else ''}".strip(),
                "parcelle_count": stat.parcelle_count,
                "bien_count": stat.bien_count,
                "menage_count": stat.menage_count,
                "membre_menage_count": stat.membre_menage_count
            }
            for stat in stats_by_date
        ]
    }

@router.get("/rapports", tags=["Rapports"])
def get_all_rapports(
    date_debut: str = Query(..., 
                          description="Start date in format YYYY-MM-DD",
                          regex=r"^\d{4}-\d{2}-\d{2}$"),
    date_fin: str = Query(...,
                         description="End date in format YYYY-MM-DD",
                         regex=r"^\d{4}-\d{2}-\d{2}$"),
    keyword: Optional[str] = Query(None, description="Search keyword for agent's name"),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Get all rapports with pagination and date filtering
    """
    # Validate date format and convert to datetime
    try:
        start_date = datetime.strptime(date_debut, "%Y-%m-%d")
        end_date = datetime.strptime(date_fin, "%Y-%m-%d") + timedelta(days=1)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Please use YYYY-MM-DD"
        )

    # Validate that end date is not before start date
    if start_date > end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="End date cannot be before start date"
        )

    query = db.query(
        RapportRecensement,
        Utilisateur.prenom,
        Utilisateur.nom,
        Utilisateur.postnom
    ).join(
        Utilisateur, RapportRecensement.fk_agent == Utilisateur.id
    ).filter(
        func.cast(RapportRecensement.date_create, Date) >= start_date,
        func.cast(RapportRecensement.date_create, Date) < end_date
    )
    
    if keyword:
        query = query.filter(
            or_(
                Utilisateur.nom.ilike(f"%{keyword}%"),
                Utilisateur.postnom.ilike(f"%{keyword}%"),
                Utilisateur.prenom.ilike(f"%{keyword}%")
            )
        )
    
    total = query.count()
    
    # Add ORDER BY clause for MSSQL compatibility
    rapports = query.order_by(RapportRecensement.date_create.desc()) \
                   .offset((page - 1) * page_size) \
                   .limit(page_size) \
                   .all()
    
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": [
            {
                **rapport[0].__dict__,
                "agent_name": f"{rapport[1]} {rapport[2]} {rapport[3] or ''}".strip()
            }
            for rapport in rapports
        ]
    }

@router.get("/rapports/{rapport_id}", tags=["Rapports"])
def get_rapport_by_id(
    rapport_id: int,
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Get a specific rapport by ID with agent details
    """
    rapport = db.query(
        RapportRecensement,
        Utilisateur.prenom,
        Utilisateur.nom
    ).join(
        Utilisateur, RapportRecensement.fk_agent == Utilisateur.id
    ).filter(
        RapportRecensement.id == rapport_id
    ).first()
    
    if not rapport:
        raise HTTPException(status_code=404, detail="Rapport not found")
    
    return {
        **rapport[0].__dict__,
        "agent_name": f"{rapport[1]} {rapport[2]}"
    }

@router.put("/users/{user_id}/update-password", tags=["Users"])
async def update_user_password(
    user_id: int,
    password_data: UpdatePassword,  # Expecting {"new_password": "new_password_value"}
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Update a user's password
    """
    try:
        # Get the user
        user = db.query(Utilisateur).filter(Utilisateur.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Hash the new password
        hashed_password = get_password_hash(password_data.new_password)
        
        # Update the password
        user.password = hashed_password
        db.commit()

        return {"message": "Password updated successfully"}

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# @router.get("/process-logs", tags=["Logs"])
# async def process_logs(db: Session = Depends(get_db)):
#     failed_log_ids = []  # List to store IDs of logs that failed
    
#     logger.info("Starting to process logs...")

#     try:
#         # Fetch all logs from the database
#         logs = db.query(LogsArchive).all()
        
#         logger.info(f"Fetched {len(logs)} logs for processing")
        
#         # return {"message": f"Fetched {len(logs)} logs for processing"}

#         # Loop over each log
#         for log in logs:
#             try:            
#                 data_json = remove_trailing_commas(log.data_json)
                
#                 json_data = json5.loads(data_json)
                                
#                 # Validate that payload is a dictionary
#                 if not isinstance(json_data, dict):
#                     raise HTTPException(status_code=400, detail="Invalid payload format. Expected a JSON object.")
                
#                 # Process based on log type
#                 if log.logs == "process_recensement_form":
#                     logger.info(f"Start processing LOGS id {log.id} for RECENSEMENT FORM")
#                     response = requests.post(
#                         "http://127.0.0.1:8000/api/v1/import-from-kobo",
#                         # "https://api.hidscollect.com:9443/api/v1/import-from-kobo",
#                         json=json_data
#                     )
#                     if response.status_code != 200:
#                         logger.error(f"Failed to process log ID {log.id}: {response.text}")
#                         failed_log_ids.append(log.id)

#                 elif log.logs == "process_rapport_superviseur_form":
#                     logger.info(f"Start processing LOGS id {log.id} for RAPPORT SUPERVISEUR FORM")
#                     response = requests.post(
#                         "http://127.0.0.1:8000/api/v1/import-rapport-superviseur",
#                         # "https://api.hidscollect.com:9443/api/v1/import-rapport-superviseur",
#                         json=json_data
#                     )
#                     if response.status_code != 200:
#                         logger.error(f"Failed to process log ID {log.id}: {response.text}")
#                         failed_log_ids.append(log.id)

#                 elif log.logs == "process_parcelles_non_baties_form":
#                     logger.info(f"Start processing LOGS id {log.id} for PARCELLE NON BATIE FORM")
#                     response = requests.post(
#                         "http://127.0.0.1:8000/api/v1/import-parcelle-non-batie",
#                         # "https://api.hidscollect.com:9443/api/v1/import-parcelle-non-batie",
#                         json=json_data
#                     )
#                     if response.status_code != 200:
#                         logger.error(f"Failed to process log ID {log.id}: {response.text}")
#                         failed_log_ids.append(log.id)

#                 elif log.logs == "process_immeuble_form":
#                     logger.info(f"Start processing LOGS id {log.id} for IMMEUBLE FORM")
#                     response = requests.post(
#                         "http://127.0.0.1:8000/api/v1/import-immeuble",
#                         # "https://api.hidscollect.com:9443/api/v1/import-immeuble",
#                         json=json_data
#                     )
#                     if response.status_code != 200:
#                         logger.error(f"Failed to process log ID {log.id}: {response.text}")
#                         failed_log_ids.append(log.id)

#             except json.JSONDecodeError as e:
#                 logger.error(f"Invalid JSON data for log ID {log.id}: {e}")
#                 failed_log_ids.append(log.id)
#                 continue
#             except Exception as e:
#                 logger.error(f"Error processing log ID {log.id}: {str(e)}")
#                 failed_log_ids.append(log.id)
#                 continue

#         # Return the list of failed log IDs
#         return {"message": "Logs processed successfully", "failed_log_ids": failed_log_ids}

#     except Exception as e:
#         logger.error(f"Unexpected error in process_logs: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")



@router.get("/process-logs-local", tags=["Logs"])
async def process_logs_in_local(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    failed_log_ids = []  # List to store IDs of logs that failed
    
    logger.info("Starting to process logs...")

    try:
        # Fetch all logs from the database
        logs = db.query(LogsArchive).all()
        
        logger.info(f"Fetched {len(logs)} logs for processing")
        
        # return {"message": f"Fetched {len(logs)} logs for processing"}

        # Loop over each log
        for log in logs:
            try:            
                data_json = remove_trailing_commas(log.data_json)
                
                json_data = json5.loads(data_json)
                                
                # Validate that payload is a dictionary
                if not isinstance(json_data, dict):
                    raise HTTPException(status_code=400, detail="Invalid payload format. Expected a JSON object.")
                
                # Process based on log type
                if log.logs == "process_recensement_form":
                    logger.info(f"Start processing LOGS id {log.id} for RECENSEMENT FORM")
                    response = process_recensement_form(json_data, db, background_tasks)

                elif log.logs == "process_rapport_superviseur_form":
                    logger.info(f"Start processing LOGS id {log.id} for RAPPORT SUPERVISEUR FORM")
                    response = process_rapport_superviseur_form(json_data, db)

                elif log.logs == "process_parcelles_non_baties_form":
                    logger.info(f"Start processing LOGS id {log.id} for PARCELLE NON BATIE FORM")
                    response = process_parcelles_non_baties_form(json_data, db, background_tasks)

                elif log.logs == "process_immeuble_form":
                    logger.info(f"Start processing LOGS id {log.id} for IMMEUBLE FORM")
                    response = process_immeuble_form(json_data, db, background_tasks)

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON data for log ID {log.id}: {e}")
                failed_log_ids.append(log.id)
                continue
            except Exception as e:
                logger.error(f"Error processing log ID {log.id}: {str(e)}")
                failed_log_ids.append(log.id)
                continue

        # Return the list of failed log IDs
        return {"message": "Logs processed successfully", "failed_log_ids": failed_log_ids}

    except Exception as e:
        logger.error(f"Unexpected error in process_logs: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    
    
@router.get("/process-logs-recensement/{id_log}", tags=["Logs"])
async def process_logs(id_log: int, db: Session = Depends(get_db)):
    try:
        # Parse the raw JSON body
        logs = db.query(Logs).where(Logs.logs == "process_recensement_form").where(Logs.id == id_log).first()
        
        data_json: str = logs.data_json
        
        valid_json_string = (
            data_json.replace("'", '"')
            .replace("None", "null")
            .replace("True", "true")
            .replace("False", "false")
            .replace("[None, None]", "[]")
        )
        
        cleaned_json = remove_trailing_commas(valid_json_string)
        
        payload = json.loads(cleaned_json)
        
        logger.info(f"Processing log ID {logs.id} with json5")
        
        # Validate that payload is a dictionary
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid payload format. Expected a JSON object.")
                
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")
    
    try:
        # Case 1: Send request to "/import-from-kobo" route
        response = requests.post(
            "https://api.hidscollect.com:9443/api/v1/import-from-kobo",
            json=payload
        )
        if response.status_code != 200:
            raise HTTPException(status_code=400, detail=f"Failed to process {response.text}")

        # Return the list of failed log IDs
        return {"message": "Logs processed successfully"}

    except Exception as e:
        logger.error(f"Unexpected error in process_logs: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


