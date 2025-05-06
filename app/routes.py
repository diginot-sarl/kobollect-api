import json

from datetime import timedelta
from typing import Annotated, Optional
from sqlalchemy.orm import Session
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    File,
    Query,
    status,
    Request,
    UploadFile,
)
from fastapi.security import OAuth2PasswordRequestForm
from app.auth import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    authenticate_user,
    create_access_token,
    get_current_active_user,
    Token,
    User,
)
from app.service import process_kobo_data, create_user
from app.schemas import UserCreate, PaginatedUserResponse
from app.database import get_db
from app.models import Bien, Parcelle, Equipe
from sqlalchemy.sql import text


router = APIRouter()

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

# Process Kobo data from Kobotoolbox
@router.post("/import-from-kobo", tags=["Kobo"])
async def process_kobo(request: Request, db: Session = Depends(get_db)):
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
    return process_kobo_data(payload, db)


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


@router.get("/users/me/", response_model=User, tags=["Users"])
def read_users_me(current_user: Annotated[User, Depends(get_current_active_user)]):
    return current_user
    

@router.get("/users", response_model=PaginatedUserResponse)
def get_all_users(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1),
    name: Optional[str] = None,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    db: Session = Depends(get_db),
):
    try:
        # Base query
        query = """
            SELECT id, login, nom, postnom, prenom, date_create, mail, telephone, photo_url, code_chasuble
            FROM utilisateur
            WHERE 1=1
        """

        # Add filters
        filters = []
        params = {}
        if name:
            filters.append("(nom LIKE :name OR postnom LIKE :name OR prenom LIKE :name)")
            params["name"] = f"%{name}%"
        if date_start:
            try:
                filters.append("CAST(date_create AS DATE) >= :date_start")
                params["date_start"] = date_start
            except Exception:
                pass
        if date_end:
            try:
                filters.append("CAST(date_create AS DATE) <= :date_end")
                params["date_end"] = date_end
            except Exception:
                pass

        # Build final query
        if filters:
            query += " AND " + " AND ".join(filters)

        # Count total records
        count_query = f"SELECT COUNT(*) FROM ({query}) AS total"
        total = db.execute(text(count_query), params).scalar()

        # Add pagination using SQL Server syntax
        query += """
            ORDER BY id DESC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """
        params["limit"] = page_size
        params["offset"] = (page - 1) * page_size

        # Execute query
        results = db.execute(text(query), params).fetchall()

        # Format results
        users = [{
            "id": row.id,
            "login": row.login,
            "nom": row.nom,
            "postnom": row.postnom,
            "prenom": row.prenom,
            "mail": row.mail,
            "telephone": row.telephone,
            "code_chasuble": row.code_chasuble,
            "photo_url": row.photo_url,
            "date_create": row.date_create.isoformat() if row.date_create else None
        } for row in results]

        return {
            "data": users,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Create a new user
@router.post("/users", response_model=User, tags=["Users"])
async def create_new_user(user_data: UserCreate, db: Session = Depends(get_db)):
    return create_user(user_data, db)


# Fetch GeoJSON data with filters
@router.get("/geojson", tags=["GeoJSON"])
async def get_geojson(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
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
    db: Session = Depends(get_db),
):
    try:
        if type == "parcelle":
            # Base query for parcelles
            query = """
                SELECT
                    p.id,
                    p.coordonnee_geographique,
                    p.date_create,
                    CONCAT(
                        a.numero, ', ',
                        av.intitule, ', Q/',
                        q.intitule, ', C/',
                        c.intitule, ', V/',
                        v.intitule, ', ',
                        pr.intitule
                    ) AS adresse
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
        else:
            # Base query for biens
            query = """
                SELECT
                    b.id,
                    b.coordinates,
                    b.date_create,
                    nb.intitule AS nature,
                    CONCAT(u.nom, ' ', u.prenom) AS recense_par,
                    CONCAT(pe.nom, ' ', pe.prenom) AS proprietaire_nom,
                    pe.denomination AS proprietaire_denomination,
                    CONCAT(
                        a.numero, ', ',
                        av.intitule, ', Q/',
                        q.intitule, ', C/',
                        c.intitule, ', V/',
                        v.intitule, ', ',
                        pr.intitule
                    ) AS adresse
                FROM bien b
                LEFT JOIN nature_bien nb ON b.fk_nature_bien = nb.id
                LEFT JOIN utilisateur u ON b.fk_agent = u.id
                LEFT JOIN parcelle p ON b.fk_parcelle = p.id
                LEFT JOIN personne pe ON p.fk_proprietaire = pe.id
                LEFT JOIN adresse a ON p.fk_adresse = a.id
                LEFT JOIN avenue av ON a.fk_avenue = av.id
                LEFT JOIN quartier q ON av.fk_quartier = q.id
                LEFT JOIN commune c ON q.fk_commune = c.id
                LEFT JOIN ville v ON c.fk_ville = v.id
                LEFT JOIN province pr ON v.fk_province = pr.id
                LEFT JOIN rang r ON p.fk_rang = r.id
                WHERE 1=1
            """

        # Add filters
        filters = []
        params = {}
        if date_start:
            filters.append("CAST(p.date_create AS DATE) >= :date_start" if type == "parcelle" else "CAST(b.date_create AS DATE) >= :date_start")
            params["date_start"] = date_start
        if date_end:
            filters.append("CAST(p.date_create AS DATE) <= :date_end" if type == "parcelle" else "CAST(b.date_create AS DATE) <= :date_end")
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
        if nature and type == "bien":
            filters.append("nb.id = :nature")
            params["nature"] = nature

        # Build final query
        if filters:
            query += " AND " + " AND ".join(filters)

        # Count total records
        count_query = f"SELECT COUNT(*) FROM ({query}) AS total"
        total = db.execute(text(count_query), params).scalar()

        # Add pagination using SQL Server syntax
        query += f"""
            ORDER BY {"p.id" if type == "parcelle" else "b.id"} DESC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """
        params["limit"] = page_size
        params["offset"] = (page - 1) * page_size

        # Execute query
        results = db.execute(text(query), params).fetchall()

        # Format results
        if type == "parcelle":
            data = [{
                "id": str(row.id),
                "coordinates": row.coordonnee_geographique,
                "adresse": row.adresse,
                "date": row.date_create.isoformat() if row.date_create else None,
            } for row in results]
        else:
            data = [{
                "id": str(row.id),
                "coordinates": row.coordinates,
                "recense_par": row.recense_par,
                "nature": row.nature,
                "proprietaire": row.proprietaire_denomination if row.proprietaire_denomination else row.proprietaire_nom,
                "adresse": row.adresse,
                "date": row.date_create.isoformat() if row.date_create else None,
            } for row in results]

        return {
            "data": data,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Fetch provinces
@router.get("/provinces", tags=["GeoJSON"])
def get_provinces(
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
    db: Session = Depends(get_db),
):
    try:
        # Base query with all necessary joins
        base_query = """
            SELECT 
                p.id, p.numero_parcellaire, p.superficie_calculee, p.coordonnee_geographique, p.date_create,
                per.id AS proprietaire_id, per.nom AS proprietaire_nom, per.postnom AS proprietaire_postnom,
                per.prenom AS proprietaire_prenom, per.denomination AS proprietaire_denomination,
                per.sigle AS proprietaire_sigle, per.fk_type_personne AS proprietaire_type_id,
                r.id AS rang_id, r.intitule AS rang_intitule,
                u.id AS type_personne_id, u.intitule AS type_personne_intitule
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
            WHERE 1=1
        """

        # Add filters
        filters = []
        params = {}
        if date_start:
            filters.append("CAST(p.date_create AS DATE) >= :date_start")
            params["date_start"] = date_start
        if date_end:
            filters.append("CAST(p.date_create AS DATE) <= :date_end")
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
        for row in results:
            data.append({
                "id": row.id,
                "numero_parcellaire": row.numero_parcellaire,
                "superficie_calculee": row.superficie_calculee,
                "coordonnee_geographique": row.coordonnee_geographique,
                "date_create": row.date_create.isoformat() if row.date_create else None,
                "proprietaire": {
                    "id": row.proprietaire_id,
                    "nom": row.proprietaire_nom,
                    "postnom": row.proprietaire_postnom,
                    "prenom": row.proprietaire_prenom,
                    "denomination": row.proprietaire_denomination,
                    "sigle": row.proprietaire_sigle,
                    "fk_type_personne": {
                        "id": row.type_personne_id,
                        "intitule": row.type_personne_intitule
                    } if row.type_personne_id else None,
                },
                "rang": {
                    "id": row.rang_id,
                    "intitule": row.rang_intitule,
                },
            })

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
    db: Session = Depends(get_db),
):
    try:
        # Main query to fetch parcelle, owner, and address hierarchy
        parcelle_query = """
            SELECT 
                p.id, p.numero_parcellaire, p.superficie_calculee, p.coordonnee_geographique, p.date_create,
                per.id AS proprietaire_id, per.nom AS proprietaire_nom, per.postnom AS proprietaire_postnom,
                per.prenom AS proprietaire_prenom, per.denomination AS proprietaire_denomination,
                per.sigle AS proprietaire_sigle, per.fk_type_personne AS proprietaire_type_id,
                a.numero AS adresse_numero, av.intitule AS avenue_intitule,
                q.intitule AS quartier_intitule, c.intitule AS commune_intitule,
                v.intitule AS ville_intitule, pr.intitule AS province_intitule,
                tp.id AS type_personne_id, tp.intitule AS type_personne_intitule
            FROM parcelle p
            LEFT JOIN personne per ON p.fk_proprietaire = per.id
            LEFT JOIN type_personne tp ON per.fk_type_personne = tp.id
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN ville v ON c.fk_ville = v.id
            LEFT JOIN province pr ON v.fk_province = pr.id
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
            "fk_type_personne": {
                "id": parcelle_result.type_personne_id,
                "intitule": parcelle_result.type_personne_intitule
            } if parcelle_result.type_personne_id else None,
        }

        # Query for biens and their related information
        biens_query = """
            SELECT 
                b.id, b.ref_bien, b.coordinates, b.superficie, b.date_create,
                m.id AS menage_id, m.fk_personne AS menage_owner_id,
                lb.fk_personne AS locataire_id,
                mm.fk_personne AS membre_id
            FROM bien b
            LEFT JOIN menage m ON b.id = m.fk_bien
            LEFT JOIN location_bien lb ON b.id = lb.fk_bien
            LEFT JOIN membre_menage mm ON m.id = mm.fk_menage
            WHERE b.fk_parcelle = :parcelle_id
        """

        # Execute biens query
        biens_results = db.execute(text(biens_query), {"parcelle_id": parcelle_id}).fetchall()

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
                    "owner": None,
                    "locataire": None,
                    "membres_menage": []
                }

            # Add owner if exists
            if row.menage_owner_id and not biens_map[row.id]["owner"]:
                owner_query = """
                    SELECT id, nom, postnom, prenom, denomination, sigle
                    FROM personne
                    WHERE id = :personne_id
                """
                owner_result = db.execute(text(owner_query), {"personne_id": row.menage_owner_id}).first()
                if owner_result:
                    biens_map[row.id]["owner"] = format_personne(owner_result)

            # Add locataire if exists
            if row.locataire_id and not biens_map[row.id]["locataire"]:
                locataire_query = """
                    SELECT id, nom, postnom, prenom, denomination, sigle
                    FROM personne
                    WHERE id = :personne_id
                """
                locataire_result = db.execute(text(locataire_query), {"personne_id": row.locataire_id}).first()
                if locataire_result:
                    biens_map[row.id]["locataire"] = format_personne(locataire_result)

            # Add membre if exists
            if row.membre_id:
                membre_query = """
                    SELECT id, nom, postnom, prenom, denomination, sigle
                    FROM personne
                    WHERE id = :personne_id
                """
                membre_result = db.execute(text(membre_query), {"personne_id": row.membre_id}).first()
                if membre_result:
                    biens_map[row.id]["membres_menage"].append(format_personne(membre_result))

        return {
            "parcelle": {
                "id": parcelle_result.id,
                "numero_parcellaire": parcelle_result.numero_parcellaire,
                "superficie_calculee": parcelle_result.superficie_calculee,
                "coordonnee_geographique": parcelle_result.coordonnee_geographique,
                "date_create": parcelle_result.date_create.isoformat() if parcelle_result.date_create else None,
                "adresse": address_info,
            },
            "proprietaire": proprietaire,
            "biens": list(biens_map.values()),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Fetch populations with filters
@router.get("/populations", tags=["Populations"])
def get_populations(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    commune: str = Query(None),
    quartier: str = Query(None),
    avenue: str = Query(None),
    rang: str = Query(None),
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
            filters.append("p.fk_rang = :rang")
            params["rang"] = rang

        # Build final parcelle query
        if filters:
            parcelle_query += " AND " + " AND ".join(filters)

        # Get filtered parcelle IDs
        parcelle_ids = [row[0] for row in db.execute(text(parcelle_query), params).fetchall()]

        if not parcelle_ids:
            return {
                "data": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
            }

        # Query to get all unique person IDs from related tables
        person_query = """
            SELECT DISTINCT person_id FROM (
                SELECT p.fk_proprietaire AS person_id
                FROM parcelle p
                WHERE p.id IN (SELECT * FROM string_split(:parcelle_ids, ','))
                AND p.fk_proprietaire IS NOT NULL
                UNION
                SELECT m.fk_personne AS person_id
                FROM bien b
                JOIN menage m ON b.id = m.fk_bien
                WHERE b.fk_parcelle IN (SELECT * FROM string_split(:parcelle_ids, ','))
                AND m.fk_personne IS NOT NULL
                UNION
                SELECT lb.fk_personne AS person_id
                FROM bien b
                JOIN location_bien lb ON b.id = lb.fk_bien
                WHERE b.fk_parcelle IN (SELECT * FROM string_split(:parcelle_ids, ','))
                AND lb.fk_personne IS NOT NULL
                UNION
                SELECT mm.fk_personne AS person_id
                FROM bien b
                JOIN menage m ON b.id = m.fk_bien
                JOIN membre_menage mm ON m.id = mm.fk_menage
                WHERE b.fk_parcelle IN (SELECT * FROM string_split(:parcelle_ids, ','))
                AND mm.fk_personne IS NOT NULL
            ) AS all_persons
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

        # Query to get person details with type_personne
        person_details_query = """
            SELECT 
                p.id, p.nom, p.postnom, p.prenom, p.denomination, p.sigle,
                tp.id AS type_personne_id, tp.intitule AS type_personne_intitule
            FROM personne p
            LEFT JOIN type_personne tp ON p.fk_type_personne = tp.id
            WHERE p.id IN (SELECT * FROM string_split(:person_ids, ','))
        """

        # Convert paginated_ids to a comma-separated string
        paginated_ids_str = ",".join(str(id) for id in paginated_ids)

        # Get person details
        person_results = db.execute(
            text(person_details_query),
            {"person_ids": paginated_ids_str if paginated_ids_str else "0"}
        ).fetchall()

        # Format results
        data = [{
            "id": row.id,
            "nom": row.nom,
            "postnom": row.postnom,
            "prenom": row.prenom,
            "denomination": row.denomination,
            "sigle": row.sigle,
            "fk_type_personne": {
                "id": row.type_personne_id,
                "intitule": row.type_personne_intitule
            } if row.type_personne_id else None,
        } for row in person_results]

        return {
            "data": data,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch cartographie data
@router.get("/cartographie", tags=["Cartographie"])
def get_cartographie(
    commune: str = Query(None),
    quartier: str = Query(None),
    avenue: str = Query(None),
    rang: str = Query(None),
    nature: str = Query(None),
    nature_specifique: str = Query(None),
    db: Session = Depends(get_db),
):
    try:
        # Base query with all necessary joins
        base_query = """
            SELECT 
                b.id AS bien_id, b.coordinates AS bien_coordinates,
                b.superficie AS bien_superficie, b.date_create AS bien_date_create,
                p.id AS parcelle_id, p.numero_parcellaire AS parcelle_numero,
                p.coordonnee_geographique AS parcelle_coordinates,
                p.superficie_calculee AS parcelle_superficie,
                p.date_create AS parcelle_date_create
            FROM bien b
            LEFT JOIN parcelle p ON b.fk_parcelle = p.id
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN rang r ON p.fk_rang = r.id
            LEFT JOIN nature_bien nb ON b.fk_nature_bien = nb.id
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
        if nature_specifique:
            filters.append("nb.intitule LIKE :nature_specifique")
            params["nature_specifique"] = f"%{nature_specifique}%"

        # Build final query
        query = base_query
        if filters:
            query += " AND " + " AND ".join(filters)

        # Add ordering
        query += " ORDER BY b.id DESC"

        # Execute query
        results = db.execute(text(query), params).fetchall()

        # Helper function to parse coordinates
        def parse_coordinates(coord_str):
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

        # Format results
        data = []
        for row in results:
            data.append({
                "bien": {
                    "id": row.bien_id,
                    "coordinates": parse_coordinates(row.bien_coordinates),
                    "superficie": row.bien_superficie,
                    "date_create": row.bien_date_create.isoformat() if row.bien_date_create else None,
                },
                "parcelle": {
                    "id": row.parcelle_id,
                    "numero_parcellaire": row.parcelle_numero,
                    "coordinates": parse_coordinates(row.parcelle_coordinates),
                    "superficie_calculee": row.parcelle_superficie,
                    "date_create": row.parcelle_date_create.isoformat() if row.parcelle_date_create else None,
                }
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
            filters.append("CAST(p.date_create AS DATE) >= :date_start")
            params["date_start"] = date_start
        if date_end:
            filters.append("CAST(p.date_create AS DATE) <= :date_end")
            params["date_end"] = date_end

        # Build final queries
        if filters:
            parcelle_query += " AND " + " AND ".join(filters)
            bien_query += " AND " + " AND ".join(filters)

        # Get total parcelles
        total_parcelles = db.execute(text(f"SELECT COUNT(*) FROM ({parcelle_query}) AS total"), params).scalar()

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
            SELECT nb.intitule, COUNT(b.id)
            FROM ({bien_query}) AS filtered_biens
            JOIN bien b ON filtered_biens.id = b.id
            LEFT JOIN nature_bien nb ON b.fk_nature_bien = nb.id
            GROUP BY nb.intitule
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
            SELECT u.intitule, COUNT(b.id)
            FROM ({bien_query}) AS filtered_biens
            JOIN bien b ON filtered_biens.id = b.id
            LEFT JOIN usage u ON b.fk_usage = u.id
            GROUP BY u.intitule
        """
        biens_by_usage = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(biens_by_usage_query), params).fetchall()
        }

        # Get biens by usage_specifique
        biens_by_usage_specifique_query = f"""
            SELECT us.intitule, COUNT(b.id)
            FROM ({bien_query}) AS filtered_biens
            JOIN bien b ON filtered_biens.id = b.id
            LEFT JOIN usage_specifique us ON b.fk_usage_specifique = us.id
            GROUP BY us.intitule
        """
        biens_by_usage_specifique = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(biens_by_usage_specifique_query), params).fetchall()
        }

        # Get parcelles by rang
        parcelles_by_rang_query = f"""
            SELECT r.intitule, COUNT(p.id)
            FROM ({parcelle_query}) AS filtered_parcelles
            JOIN parcelle p ON filtered_parcelles.id = p.id
            LEFT JOIN rang r ON p.fk_rang = r.id
            GROUP BY r.intitule
        """
        parcelles_by_rang = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(parcelles_by_rang_query), params).fetchall()
        }

        # Get parcelles by commune
        parcelles_by_commune_query = f"""
            SELECT c.intitule, COUNT(p.id)
            FROM ({parcelle_query}) AS filtered_parcelles
            JOIN parcelle p ON filtered_parcelles.id = p.id
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            GROUP BY c.intitule
        """
        parcelles_by_commune = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(parcelles_by_commune_query), params).fetchall()
        }

        # Get parcelles by quartier
        parcelles_by_quartier_query = f"""
            SELECT q.intitule, COUNT(p.id)
            FROM ({parcelle_query}) AS filtered_parcelles
            JOIN parcelle p ON filtered_parcelles.id = p.id
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            GROUP BY q.intitule
        """
        parcelles_by_quartier = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(parcelles_by_quartier_query), params).fetchall()
        }

        # Get parcelles by avenue
        parcelles_by_avenue_query = f"""
            SELECT av.intitule, COUNT(p.id)
            FROM ({parcelle_query}) AS filtered_parcelles
            JOIN parcelle p ON filtered_parcelles.id = p.id
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            GROUP BY av.intitule
        """
        parcelles_by_avenue = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(parcelles_by_avenue_query), params).fetchall()
        }

        return {
            "total_parcelles": total_parcelles,
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
    try:
        # Query to find user by code_chasuble
        query = """
            SELECT id, nom, postnom, prenom, date_create, code_chasuble, photo_url, sexe, mail, telephone
            FROM utilisateur
            WHERE code_chasuble = :code_chasuble
        """
        result = db.execute(text(query), {"code_chasuble": code_chasuble}).first()
        
        if not result:
            raise {
                "status": 204,
                "data": None,
                "message": "Utilisateur non trouvé"
            }
            
        return {
            "status": 200,
            "data": {
                "id": result.id,
                "nom": result.nom,
                "postnom": result.postnom,
                "prenom": result.prenom,
                "email": result.mail,
                "telephone": result.telephone,
                "sexe": result.sexe,
                "date_create": result.date_create.isoformat() if result.date_create else None,
                "code_chasuble": result.code_chasuble,
                "photo_url": result.photo_url,
                "fk_adresse": None,
            }
        }
    except Exception as e:
        return {
            "status": 500,
            "data": None,
            "message": f"Erreur lors de la recherche: {str(e)}"
        }
    

@router.post("/teams", tags=["Teams"])
def create_team(
    intitule: str = Query(...),
    fk_quartier: int = Query(...),
    db: Session = Depends(get_db)
):
    try:
        # Create new team
        new_team = Equipe(intitule=intitule, fk_quartier=fk_quartier)
        db.add(new_team)
        db.commit()
        db.refresh(new_team)
        return {"id": new_team.id, "intitule": new_team.intitule, "fk_quartier": new_team.fk_quartier}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/teams", tags=["Teams"])
def get_teams(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    date_start: str = Query(None),
    date_end: str = Query(None),
    fk_quartier: int = Query(None),
    intitule: str = Query(None),
    db: Session = Depends(get_db)
):
    try:
        # Base query
        query = """
            SELECT e.id, e.intitule, e.fk_quartier, q.intitule AS quartier_intitule
            FROM equipe e
            LEFT JOIN quartier q ON e.fk_quartier = q.id
            WHERE 1=1
        """

        # Add filters
        filters = []
        params = {}
        if date_start:
            filters.append("CAST(e.date_create AS DATE) >= :date_start")
            params["date_start"] = date_start
        if date_end:
            filters.append("CAST(e.date_create AS DATE) <= :date_end")
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

        # Format results
        teams = [{
            "id": row.id,
            "intitule": row.intitule,
            "fk_quartier": row.fk_quartier,
            "quartier_intitule": row.quartier_intitule
        } for row in results]

        return {
            "data": teams,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


