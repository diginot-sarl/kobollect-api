from datetime import timedelta
from typing import Annotated

from sqlalchemy.orm import Session
from sqlalchemy import func
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
from app.schemas import UserCreate
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


@router.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
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


@router.post("/import-kobo-data", tags=["KOBO"])
async def import_kobo_data(
    request: Request,
    # current_user: Annotated[User, Depends(get_current_active_user)]
):
    try:
        data = await request.json()
        if "received_data" not in data or "_id" not in data["received_data"]:
            raise HTTPException(status_code=400, detail="Champ '_id' manquant dans les données reçues.")
        return process_kobo_data(data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/users/me/", response_model=User, tags=["Users"])
async def read_users_me(current_user: Annotated[User, Depends(get_current_active_user)]):
    return current_user


@router.post("/users/create", tags=["Users"])
async def create_user_endpoint(
    user: UserCreate,
    # current_user: Annotated[User, Depends(get_current_active_user)]
):
    try:
        create_user(user.model_dump())
        return {"message": "User created successfully"}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
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
async def get_provinces(
    # current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        provinces = db.query(Province).all()
        return [{"id": province.id, "name": province.intitule} for province in provinces]
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
        province_obj = db.query(Province).filter(Province.intitule == province).first()
        if not province_obj:
            raise HTTPException(status_code=404, detail="Province not found")

        villes = db.query(Ville).filter(Ville.fk_province == province_obj.id).all()
        return {"villes": [{"id": ville.id, "name": ville.intitule} for ville in villes]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch communes by ville
@router.get("/communes", tags=["GeoJSON"])
async def get_communes(
    ville: str = Query(...),
    # current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        ville_obj = db.query(Ville).filter(Ville.intitule == ville).first()
        if not ville_obj:
            raise HTTPException(status_code=404, detail="Ville not found")

        communes = db.query(Commune).filter(Commune.fk_ville == ville_obj.id).all()
        return [{"id": commune.id, "name": commune.intitule} for commune in communes]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch quartiers by commune
@router.get("/quartiers", tags=["GeoJSON"])
async def get_quartiers(
    commune: str = Query(...),
    # current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        commune_obj = db.query(Commune).filter(Commune.intitule == commune).first()
        if not commune_obj:
            raise HTTPException(status_code=404, detail="Commune not found")

        quartiers = db.query(Quartier).filter(Quartier.fk_commune == commune_obj.id).all()
        return {"quartiers": [{"id": quartier.id, "name": quartier.intitule} for quartier in quartiers]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch avenues by quartier
@router.get("/avenues", tags=["GeoJSON"])
async def get_avenues(
    quartier: str = Query(...),
    # current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        quartier_obj = db.query(Quartier).filter(Quartier.intitule == quartier).first()
        if not quartier_obj:
            raise HTTPException(status_code=404, detail="Quartier not found")

        avenues = db.query(Avenue).filter(Avenue.fk_quartier == quartier_obj.id).all()
        return {"avenues": [{"id": avenue.id, "name": avenue.intitule} for avenue in avenues]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    
# Fetch rangs
@router.get("/rangs", tags=["GeoJSON"])
async def get_rangs(
    # current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        rangs = db.query(Rang).all()
        return [{"id": rang.id, "name": rang.intitule} for rang in rangs]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Fetch natures (nature_bien)
@router.get("/natures", tags=["GeoJSON"])
async def get_natures(
    # current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        natures = db.query(NatureBien).all()
        return [{"id": nature.id, "name": nature.intitule} for nature in natures]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
