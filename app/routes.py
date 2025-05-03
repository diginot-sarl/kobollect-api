import json
from datetime import timedelta
from typing import Annotated, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, select, Date
from fastapi import APIRouter, Request, HTTPException, Depends, Query, status
from fastapi.security import OAuth2PasswordRequestForm
from app.auth import (
    Token,
    authenticate_user,
    create_access_token,
    get_current_active_user,
    User,
    ACCESS_TOKEN_EXPIRE_MINUTES
)
from app.service import process_kobo_data, create_user
from app.schemas import UserCreate, PaginatedUserResponse
from app.database import get_db
from app.models import (
    Province,
    Ville,
    Commune,
    Quartier,
    Avenue,
    Rang,
    NatureBien,
    Bien,
    Utilisateur,
    Personne,
    Adresse,
    Parcelle
)

router = APIRouter()

# Process Kobo data from Kobotoolbox
@router.post("/import-from-kobo", tags=["Kobo"])
async def process_kobo(request: Request, db: Session = Depends(get_db)):
    try:
        # Parse the raw JSON body
        payload = await request.json()
        
        # Validate that payload is a dictionary
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid payload format. Expected a JSON object.")
                
        # Process the payload using the service function
        return process_kobo_data(payload, db)
    
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")


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
    query = db.query(Utilisateur)

    # Filter by name (prenom, nom, postnom)
    if name:
        like_pattern = f"%{name}%"
        query = query.filter(
            (Utilisateur.nom.ilike(like_pattern)) |
            (Utilisateur.postnom.ilike(like_pattern)) |
            (Utilisateur.prenom.ilike(like_pattern))
        )

    # Filter by date range (compare only the date part)
    if date_start:
        try:
            query = query.filter(func.cast(Utilisateur.date_creat, Date) >= date_start)
        except Exception:
            pass

    if date_end:
        try:
            query = query.filter(func.cast(Utilisateur.date_creat, Date) <= date_end)
        except Exception:
            pass

    total = query.count()
    users = query.order_by(Utilisateur.id.desc()).offset((page - 1) * page_size).limit(page_size).all()

    return {
        "data": users,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


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
    province: str = Query(None),
    ville: str = Query(None),
    commune: str = Query(None),
    quartier: str = Query(None),
    avenue: str = Query(None),
    rang: str = Query(None),
    nature: str = Query(None),
    # current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Base query for bien
        query = (
            db.query(
                Bien,
                NatureBien.intitule.label("nature"),
                func.concat(
                    Utilisateur.nom, " ", Utilisateur.prenom
                ).label("recensé_par"),
                func.concat(
                    Personne.nom, " ", Personne.prenom
                ).label("propriétaire_nom"),
                Personne.denomination.label("propriétaire_denomination"),
                func.concat(
                    Adresse.numero, ", ",
                    Avenue.intitule, ", Q/",
                    Quartier.intitule, ", C/",
                    Commune.intitule, ", V/",
                    Ville.intitule, ", ",
                    Province.intitule
                ).label("adresse"),
            )
            .join(NatureBien, Bien.fk_nature_bien == NatureBien.id, isouter=True)
            .join(Utilisateur, Bien.fk_agent == Utilisateur.id, isouter=True)
            .join(Parcelle, Bien.fk_parcelle == Parcelle.id, isouter=True)
            .join(Personne, Parcelle.fk_proprietaire == Personne.id, isouter=True)
            .join(Adresse, Parcelle.fk_adresse == Adresse.id, isouter=True)
            .join(Avenue, Adresse.fk_avenue == Avenue.id, isouter=True)
            .join(Quartier, Avenue.fk_quartier == Quartier.id, isouter=True)
            .join(Commune, Quartier.fk_commune == Commune.id, isouter=True)
            .join(Ville, Commune.fk_ville == Ville.id, isouter=True)
            .join(Province, Ville.fk_province == Province.id, isouter=True)
        )

        # Apply filters
        if date_start:
            query = query.filter(Bien.date_create >= date_start)
        if date_end:
            query = query.filter(Bien.date_create <= date_end)
        if province:
            query = query.filter(Province.intitule == province)
        if ville:
            query = query.filter(Ville.intitule == ville)
        if commune:
            query = query.filter(Commune.intitule == commune)
        if quartier:
            query = query.filter(Quartier.intitule == quartier)
        if avenue:
            query = query.filter(Avenue.intitule == avenue)
        if rang:
            query = query.filter(Rang.intitule == rang).join(Rang, Parcelle.fk_rang == Rang.id, isouter=True)
        if nature:
            query = query.filter(NatureBien.intitule == nature)

        # Get total count for pagination
        total = query.count()

        # Apply pagination
        query = query.offset((page - 1) * page_size).limit(page_size)

        # Execute query
        results = query.all()

        # Format the response
        data = [
            {
                "id": str(bien.id),  # Convert to string to match frontend
                "coordinates": bien.coordinates,
                "recensé_par": recensé_par,
                "nature": nature,
                "propriétaire": propriétaire_denomination if propriétaire_denomination else propriétaire_nom,
                "adresse": adresse,
                "date": bien.date_create.isoformat() if bien.date_create else None,
            }
            for bien, nature, recensé_par, propriétaire_nom, propriétaire_denomination, adresse in results
        ]

        return {
            "data": data,
            "total": total,
            "page": page,
            "page_size": page_size,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/provinces", tags=["GeoJSON"])
def get_provinces(
    # current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = db.execute(
            select(Province.id, Province.intitule)
        )
        data = query.all()
        return [item._asdict() for item in data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch villes by province
@router.get("/villes", tags=["GeoJSON"])
async def get_villes(
    province: str = Query(...),
    # current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = db.execute(
            select(Ville.id, Ville.intitule).where(Ville.fk_province == province)
        )
        data = query.all()
        return [item._asdict() for item in data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch communes by ville
@router.get("/communes", tags=["GeoJSON"])
def get_communes(
    ville: str = Query(...),
    # current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = db.execute(
            select(Commune.id, Commune.intitule).where(Commune.fk_ville == ville)
        )
        data = query.all()
        return [item._asdict() for item in data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch quartiers by commune
@router.get("/quartiers", tags=["GeoJSON"])
def get_quartiers(
    commune: str = Query(...),
    # current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = db.execute(
            select(Quartier.id, Quartier.intitule).where(Quartier.fk_commune == commune)
        )
        data = query.all()
        return [item._asdict() for item in data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch avenues by quartier
@router.get("/avenues", tags=["GeoJSON"])
def get_avenues(
    quartier: str = Query(...),
    # current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = db.execute(
            select(Avenue.id, Avenue.intitule).where(Avenue.fk_quartier == quartier)
        )
        data = query.all()
        return [item._asdict() for item in data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    
# Fetch rangs
@router.get("/rangs", tags=["GeoJSON"])
def get_rangs(
    # current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = db.execute(
            select(Rang.id, Rang.intitule)
        )
        data = query.all()
        return [item._asdict() for item in data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch natures (nature_bien)
@router.get("/natures", tags=["GeoJSON"])
def get_natures(
    # current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        query = db.execute(
            select(NatureBien.id, NatureBien.intitule)
        )
        data = query.all()
        return [item._asdict() for item in data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    
    