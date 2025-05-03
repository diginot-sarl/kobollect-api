import json
from datetime import timedelta
from typing import Annotated, Optional
from sqlalchemy.orm import Session, aliased
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
    Parcelle,
    Unite,
    Menage,
    LocationBien,
    MembreMenage,
    TypePersonne
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
            query = query.filter(Province.id == province)
        if ville:
            query = query.filter(Ville.id == ville)
        if commune:
            query = query.filter(Commune.id == commune)
        if quartier:
            query = query.filter(Quartier.id == quartier)
        if avenue:
            query = query.filter(Avenue.id == avenue)
        if rang:
            query = query.filter(Rang.id == rang).join(Rang, Parcelle.fk_rang == Rang.id, isouter=True)
        if nature:
            query = query.filter(NatureBien.id == nature)

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


@router.get("/parcelles", tags=["Parcelles"])
def get_parcelles(
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
    query = (
        db.query(
            Parcelle,
            Personne.id.label("proprietaire_id"),
            Personne.nom.label("proprietaire_nom"),
            Personne.postnom.label("proprietaire_postnom"),
            Personne.prenom.label("proprietaire_prenom"),
            Personne.denomination.label("proprietaire_denomination"),
            Personne.sigle.label("proprietaire_sigle"),
            Personne.fk_type_personne.label("proprietaire_type_id"),
            Rang.id.label("rang_id"),
            Rang.intitule.label("rang_intitule"),
        )
        .join(Personne, Parcelle.fk_proprietaire == Personne.id, isouter=True)
        .join(Rang, Parcelle.fk_rang == Rang.id, isouter=True)
        .join(Adresse, Parcelle.fk_adresse == Adresse.id, isouter=True)
        .join(Avenue, Adresse.fk_avenue == Avenue.id, isouter=True)
        .join(Quartier, Avenue.fk_quartier == Quartier.id, isouter=True)
        .join(Commune, Quartier.fk_commune == Commune.id, isouter=True)
        .join(Ville, Commune.fk_ville == Ville.id, isouter=True)
        .join(Province, Ville.fk_province == Province.id, isouter=True)
    )

    # Filtering
    if date_start:
        try:
            query = query.filter(func.cast(Parcelle.date_create, Date) >= date_start)
        except Exception:
            pass
    if date_end:
        try:
            query = query.filter(func.cast(Parcelle.date_create, Date) <= date_end)
        except Exception:
            pass
    if province:
        query = query.filter(Province.id == province)
    if ville:
        query = query.filter(Ville.id == ville)
    if commune:
        query = query.filter(Commune.id == commune)
    if quartier:
        query = query.filter(Quartier.id == quartier)
    if avenue:
        query = query.filter(Avenue.id == avenue)
    if rang:
        query = query.filter(Rang.id == rang)
    # If you want to filter by nature, you may need to join NatureBien via Bien, depending on your model

    total = query.count()
    results = (
        query.order_by(Parcelle.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    data = []
    for (
        parcelle,
        proprietaire_id,
        proprietaire_nom,
        proprietaire_postnom,
        proprietaire_prenom,
        proprietaire_denomination,
        proprietaire_sigle,
        proprietaire_type_id,
        rang_id,
        rang_intitule,
    ) in results:
        # Optionally, fetch type_personne intitule if needed
        type_personne = None
        if proprietaire_type_id:
            type_personne_obj = db.execute(
                select(Unite.id, Unite.intitule)
                .where(Unite.id == proprietaire_type_id)
            ).first()
            if type_personne_obj:
                type_personne = {
                    "id": type_personne_obj.id,
                    "intitule": type_personne_obj.intitule,
                }

        data.append({
            "id": parcelle.id,
            "numero_parcellaire": parcelle.numero_parcellaire,
            "superficie_calculee": parcelle.superficie_calculee,
            "coordonnee_geographique": parcelle.coordonnee_geographique,
            "date_create": parcelle.date_create.isoformat() if parcelle.date_create else None,
            "proprietaire": {
                "id": proprietaire_id,
                "nom": proprietaire_nom,
                "postnom": proprietaire_postnom,
                "prenom": proprietaire_prenom,
                "denomination": proprietaire_denomination,
                "sigle": proprietaire_sigle,
                "fk_type_personne": type_personne,
            },
            "rang": {
                "id": rang_id,
                "intitule": rang_intitule,
            },
        })

    return {
        "data": data,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/parcelles/{parcelle_id}", tags=["Parcelles"])
def get_parcelle_details(
    parcelle_id: int,
    db: Session = Depends(get_db),
):
    # Fetch the parcelle and its address
    parcelle = (
        db.query(Parcelle)
        .filter(Parcelle.id == parcelle_id)
        .first()
    )
    if not parcelle:
        raise HTTPException(status_code=404, detail="Parcelle not found")

    # Fetch the owner (proprietaire) of the parcelle
    proprietaire = db.query(Personne).filter(Personne.id == parcelle.fk_proprietaire).first()

    def format_personne(personne):
        if not personne:
            return None
        type_personne = None
        if personne.fk_type_personne:
            type_personne_obj = db.execute(
                select(TypePersonne.id, TypePersonne.intitule)
                .where(TypePersonne.id == personne.fk_type_personne)
            ).first()
            if type_personne_obj:
                type_personne = {
                    "id": type_personne_obj.id,
                    "intitule": type_personne_obj.intitule,
                }
        return {
            "id": personne.id,
            "nom": personne.nom,
            "postnom": personne.postnom,
            "prenom": personne.prenom,
            "denomination": personne.denomination,
            "sigle": personne.sigle,
            "sexe": personne.sexe,
            "numero_impot": personne.numero_impot,
            "rccm": personne.rccm,
            "id_nat": personne.id_nat,
            "domaine_activite": personne.domaine_activite,
            "lieu_naissance": personne.lieu_naissance,
            "date_naissance": personne.date_naissance,
            "province_origine": personne.province_origine,
            "district": personne.district,
            "territoire": personne.territoire,
            "secteur": personne.secteur,
            "village": personne.village,
            "fk_nationalite": personne.fk_nationalite,
            "profession": personne.profession,
            "type_piece_identite": personne.type_piece_identite,
            "numero_piece_identite": personne.numero_piece_identite,
            "nom_du_pere": personne.nom_du_pere,
            "nom_de_la_mere": personne.nom_de_la_mere,
            "etat_civil": personne.etat_civil,
            "lieu_parente": personne.lieu_parente,
            "telephone": personne.telephone,
            "adresse_mail": personne.adresse_mail,
            "nombre_enfant": personne.nombre_enfant,
            "niveau_etude": personne.niveau_etude,
            "fk_type_personne": type_personne,
        }

    # Fetch address hierarchy
    adresse = db.query(Adresse).filter(Adresse.id == parcelle.fk_adresse).first()
    avenue = quartier = commune = ville = province = None
    if adresse:
        avenue = db.query(Avenue).filter(Avenue.id == adresse.fk_avenue).first()
        if avenue:
            quartier = db.query(Quartier).filter(Quartier.id == avenue.fk_quartier).first()
            if quartier:
                commune = db.query(Commune).filter(Commune.id == quartier.fk_commune).first()
                if commune:
                    ville = db.query(Ville).filter(Ville.id == commune.fk_ville).first()
                    if ville:
                        province = db.query(Province).filter(Province.id == ville.fk_province).first()
    address_info = {
        "numero": adresse.numero if adresse else None,
        "avenue": avenue.intitule if avenue else None,
        "quartier": quartier.intitule if quartier else None,
        "commune": commune.intitule if commune else None,
        "ville": ville.intitule if ville else None,
        "province": province.intitule if province else None,
    }

    # Fetch all biens (apartments) in this parcelle
    biens = db.query(Bien).filter(Bien.fk_parcelle == parcelle_id).all()
    bien_list = []
    for bien in biens:
        # Find the menage for this bien (should be one per bien)
        menage = db.query(Menage).filter(Menage.fk_bien == bien.id).first()
        bien_owner = None
        if menage:
            bien_owner = db.query(Personne).filter(Personne.id == menage.fk_personne).first()
        # Check if this bien is rented (LocationBien)
        location = db.query(LocationBien).filter(LocationBien.fk_bien == bien.id).first()
        locataire = None
        if location:
            locataire = db.query(Personne).filter(Personne.id == location.fk_personne).first()
        # Members of the menage (MembreMenage)
        membres = []
        if menage:
            membres_menage = db.query(MembreMenage).filter(MembreMenage.fk_menage == menage.id).all()
            for membre in membres_menage:
                personne = db.query(Personne).filter(Personne.id == membre.fk_personne).first()
                membres.append(format_personne(personne))
        bien_list.append({
            "id": bien.id,
            "ref_bien": bien.ref_bien,
            "coordinates": bien.coordinates,
            "superficie": bien.superficie,
            "date_create": bien.date_create.isoformat() if bien.date_create else None,
            "owner": format_personne(bien_owner),
            "locataire": format_personne(locataire) if locataire else None,
            "membres_menage": membres,
        })

    return {
        "parcelle": {
            "id": parcelle.id,
            "numero_parcellaire": parcelle.numero_parcellaire,
            "superficie_calculee": parcelle.superficie_calculee,
            "coordonnee_geographique": parcelle.coordonnee_geographique,
            "date_create": parcelle.date_create.isoformat() if parcelle.date_create else None,
            "adresse": address_info,
        },
        "proprietaire": format_personne(proprietaire),
        "biens": bien_list,
    }


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
    # Build a base query for parcelles with filters
    parcelle_query = db.query(Parcelle.id)

    if commune:
        parcelle_query = parcelle_query.join(Adresse, Parcelle.fk_adresse == Adresse.id)\
                                       .join(Avenue, Adresse.fk_avenue == Avenue.id)\
                                       .join(Quartier, Avenue.fk_quartier == Quartier.id)\
                                       .join(Commune, Quartier.fk_commune == Commune.id)\
                                       .filter(Commune.id == commune)
    if quartier:
        parcelle_query = parcelle_query.join(Adresse, Parcelle.fk_adresse == Adresse.id)\
                                       .join(Avenue, Adresse.fk_avenue == Avenue.id)\
                                       .join(Quartier, Avenue.fk_quartier == Quartier.id)\
                                       .filter(Quartier.id == quartier)
    if avenue:
        parcelle_query = parcelle_query.join(Adresse, Parcelle.fk_adresse == Adresse.id)\
                                       .join(Avenue, Adresse.fk_avenue == Avenue.id)\
                                       .filter(Avenue.id == avenue)
    if rang:
        parcelle_query = parcelle_query.filter(Parcelle.fk_rang == rang)

    parcelle_ids = [row[0] for row in parcelle_query.distinct().all()]

    # Collect all unique person IDs from filtered parcelles, biens, menages, locations, membres_menage
    person_ids = set()

    if parcelle_ids:
        # 1. Propriétaires de parcelles
        parcelle_proprietaires = db.query(Parcelle.fk_proprietaire)\
            .filter(Parcelle.id.in_(parcelle_ids), Parcelle.fk_proprietaire != None).all()
        person_ids.update([pid[0] for pid in parcelle_proprietaires if pid[0]])

        # 2. Owners of biens (menage.fk_personne)
        bien_ids = [row[0] for row in db.query(Bien.id).filter(Bien.fk_parcelle.in_(parcelle_ids)).all()]
        if bien_ids:
            bien_owners = db.query(Menage.fk_personne)\
                .filter(Menage.fk_bien.in_(bien_ids), Menage.fk_personne != None).all()
            person_ids.update([pid[0] for pid in bien_owners if pid[0]])

            # 3. Locataires (LocationBien.fk_personne)
            locataires = db.query(LocationBien.fk_personne)\
                .filter(LocationBien.fk_bien.in_(bien_ids), LocationBien.fk_personne != None).all()
            person_ids.update([pid[0] for pid in locataires if pid[0]])

            # 4. Membres de ménage (MembreMenage.fk_personne)
            menage_ids = [row[0] for row in db.query(Menage.id).filter(Menage.fk_bien.in_(bien_ids)).all()]
            if menage_ids:
                membres = db.query(MembreMenage.fk_personne)\
                    .filter(MembreMenage.fk_menage.in_(menage_ids), MembreMenage.fk_personne != None).all()
                person_ids.update([pid[0] for pid in membres if pid[0]])
    else:
        # No parcelle matches, return empty result
        return {
            "data": [],
            "total": 0,
            "page": page,
            "page_size": page_size,
        }

    # Pagination
    person_ids = list(person_ids)
    total = len(person_ids)
    start = (page - 1) * page_size
    end = start + page_size
    paginated_ids = person_ids[start:end]

    # Fetch personnes
    personnes = db.query(Personne).filter(Personne.id.in_(paginated_ids)).all()

    # Helper to get type_personne
    def format_personne(personne):
        if not personne:
            return None
        type_personne = None
        if personne.fk_type_personne:
            type_personne_obj = db.execute(
                select(TypePersonne.id, TypePersonne.intitule)
                .where(TypePersonne.id == personne.fk_type_personne)
            ).first()
            if type_personne_obj:
                type_personne = {
                    "id": type_personne_obj.id,
                    "intitule": type_personne_obj.intitule,
                }
        return {
            "id": personne.id,
            "nom": personne.nom,
            "postnom": personne.postnom,
            "prenom": personne.prenom,
            "denomination": personne.denomination,
            "sigle": personne.sigle,
            "fk_type_personne": type_personne,
        }

    data = [format_personne(p) for p in personnes]

    return {
        "data": data,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


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
    # Base query for biens and parcelles
    biens_query = (
        db.query(
            Bien.id.label("bien_id"),
            Bien.coordinates.label("bien_coordinates"),
            Bien.superficie.label("bien_superficie"),
            Bien.date_create.label("bien_date_create"),
            Parcelle.id.label("parcelle_id"),
            Parcelle.numero_parcellaire.label("parcelle_numero"),
            Parcelle.coordonnee_geographique.label("parcelle_coordinates"),
            Parcelle.superficie_calculee.label("parcelle_superficie"),
            Parcelle.date_create.label("parcelle_date_create"),
        )
        .join(Parcelle, Bien.fk_parcelle == Parcelle.id, isouter=True)
        .join(Adresse, Parcelle.fk_adresse == Adresse.id, isouter=True)
        .join(Avenue, Adresse.fk_avenue == Avenue.id, isouter=True)
        .join(Quartier, Avenue.fk_quartier == Quartier.id, isouter=True)
        .join(Commune, Quartier.fk_commune == Commune.id, isouter=True)
        .join(Rang, Parcelle.fk_rang == Rang.id, isouter=True)
        .join(NatureBien, Bien.fk_nature_bien == NatureBien.id, isouter=True)
    )

    # Apply filters
    if commune:
        biens_query = biens_query.filter(Commune.id == commune)
    if quartier:
        biens_query = biens_query.filter(Quartier.id == quartier)
    if avenue:
        biens_query = biens_query.filter(Avenue.id == avenue)
    if rang:
        biens_query = biens_query.filter(Rang.id == rang)
    if nature:
        biens_query = biens_query.filter(NatureBien.id == nature)
    if nature_specifique:
        biens_query = biens_query.filter(NatureBien.intitule.ilike(f"%{nature_specifique}%"))

    # Add explicit order_by for consistency
    biens_query = biens_query.order_by(Bien.id.desc())

    results = biens_query.all()

    def parse_coordinates(coord_str):
        """
        Convert a string like "-4.337054 15.291452 0 0;-4.337097 15.291641 0 0" to
        a list of [lat, lng] pairs: [[-4.337054, 15.291452], ...]
        """
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
    # Build base filters for parcelles and biens
    parcelle_query = (
        db.query(Parcelle)
        .join(Adresse, Parcelle.fk_adresse == Adresse.id, isouter=True)
        .join(Avenue, Adresse.fk_avenue == Avenue.id, isouter=True)
        .join(Quartier, Avenue.fk_quartier == Quartier.id, isouter=True)
        .join(Commune, Quartier.fk_commune == Commune.id, isouter=True)
        .join(Rang, Parcelle.fk_rang == Rang.id, isouter=True)
    )
    bien_query = (
        db.query(Bien)
        .join(Parcelle, Bien.fk_parcelle == Parcelle.id, isouter=True)
        .join(Adresse, Parcelle.fk_adresse == Adresse.id, isouter=True)
        .join(Avenue, Adresse.fk_avenue == Avenue.id, isouter=True)
        .join(Quartier, Avenue.fk_quartier == Quartier.id, isouter=True)
        .join(Commune, Quartier.fk_commune == Commune.id, isouter=True)
        .join(Rang, Parcelle.fk_rang == Rang.id, isouter=True)
        .join(NatureBien, Bien.fk_nature_bien == NatureBien.id, isouter=True)
    )

    # Apply filters
    if commune:
        parcelle_query = parcelle_query.filter(Commune.id == commune)
        bien_query = bien_query.filter(Commune.id == commune)
    if quartier:
        parcelle_query = parcelle_query.filter(Quartier.id == quartier)
        bien_query = bien_query.filter(Quartier.id == quartier)
    if avenue:
        parcelle_query = parcelle_query.filter(Avenue.id == avenue)
        bien_query = bien_query.filter(Avenue.id == avenue)
    if rang:
        parcelle_query = parcelle_query.filter(Rang.id == rang)
        bien_query = bien_query.filter(Rang.id == rang)
    if nature:
        bien_query = bien_query.filter(NatureBien.id == nature)
    if date_start:
        parcelle_query = parcelle_query.filter(func.cast(Parcelle.date_create, Date) >= date_start)
        bien_query = bien_query.filter(func.cast(Bien.date_create, Date) >= date_start)
    if date_end:
        parcelle_query = parcelle_query.filter(func.cast(Parcelle.date_create, Date) <= date_end)
        bien_query = bien_query.filter(func.cast(Bien.date_create, Date) <= date_end)

    # Total parcelles
    total_parcelles = parcelle_query.count()

    # Total biens
    total_biens = bien_query.count()

    # Total propriétaires (unique proprietaire ids from parcelles)
    proprietaire_ids = parcelle_query.with_entities(Parcelle.fk_proprietaire).distinct().all()
    total_proprietaires = len([pid[0] for pid in proprietaire_ids if pid[0]])

    # Total population (unique personnes from parcelles, biens, menages, etc.)
    # Reuse the logic from /populations route
    parcelle_ids = [row.id for row in parcelle_query.with_entities(Parcelle.id).distinct().all()]
    person_ids = set()
    if parcelle_ids:
        # 1. Propriétaires de parcelles
        parcelle_proprietaires = db.query(Parcelle.fk_proprietaire)\
            .filter(Parcelle.id.in_(parcelle_ids), Parcelle.fk_proprietaire != None).all()
        person_ids.update([pid[0] for pid in parcelle_proprietaires if pid[0]])

        # 2. Owners of biens (menage.fk_personne)
        bien_ids = [row[0] for row in db.query(Bien.id).filter(Bien.fk_parcelle.in_(parcelle_ids)).all()]
        if bien_ids:
            bien_owners = db.query(Menage.fk_personne)\
                .filter(Menage.fk_bien.in_(bien_ids), Menage.fk_personne != None).all()
            person_ids.update([pid[0] for pid in bien_owners if pid[0]])

            # 3. Locataires (LocationBien.fk_personne)
            locataires = db.query(LocationBien.fk_personne)\
                .filter(LocationBien.fk_bien.in_(bien_ids), LocationBien.fk_personne != None).all()
            person_ids.update([pid[0] for pid in locataires if pid[0]])

            # 4. Membres de ménage (MembreMenage.fk_personne)
            menage_ids = [row[0] for row in db.query(Menage.id).filter(Menage.fk_bien.in_(bien_ids)).all()]
            if menage_ids:
                membres = db.query(MembreMenage.fk_personne)\
                    .filter(MembreMenage.fk_menage.in_(menage_ids), MembreMenage.fk_personne != None).all()
                person_ids.update([pid[0] for pid in membres if pid[0]])
    total_population = len(person_ids)

    # Biens by nature
    biens_by_nature = {}
    nature_counts = (
        bien_query.with_entities(NatureBien.intitule, func.count(Bien.id))
        .group_by(NatureBien.intitule)
        .all()
    )
    for nature_label, count in nature_counts:
        biens_by_nature[nature_label or "Inconnu"] = count

    # Parcelles by rang
    parcelles_by_rang = {}
    rang_counts = (
        parcelle_query.with_entities(Rang.intitule, func.count(Parcelle.id))
        .group_by(Rang.intitule)
        .all()
    )
    for rang_label, count in rang_counts:
        parcelles_by_rang[rang_label or "Inconnu"] = count

    # Parcelles by commune
    CommuneAlias = aliased(Commune)
    QuartierAlias = aliased(Quartier)
    AvenueAlias = aliased(Avenue)

    commune_counts = (
        parcelle_query
        .join(AvenueAlias, Adresse.fk_avenue == AvenueAlias.id)
        .join(QuartierAlias, AvenueAlias.fk_quartier == QuartierAlias.id)
        .join(CommuneAlias, QuartierAlias.fk_commune == CommuneAlias.id)
        .with_entities(CommuneAlias.intitule, func.count(Parcelle.id))
        .group_by(CommuneAlias.intitule)
        .all()
    )
    parcelles_by_commune = {}
    for commune_label, count in commune_counts:
        parcelles_by_commune[commune_label or "Inconnu"] = count

    # Parcelles by quartier
    QuartierAlias = aliased(Quartier)
    AvenueAlias = aliased(Avenue)
    quartier_counts = (
        parcelle_query
        .join(AvenueAlias, Adresse.fk_avenue == AvenueAlias.id)
        .join(QuartierAlias, AvenueAlias.fk_quartier == QuartierAlias.id)
        .with_entities(QuartierAlias.intitule, func.count(Parcelle.id))
        .group_by(QuartierAlias.intitule)
        .all()
    )
    parcelles_by_quartier = {}
    for quartier_label, count in quartier_counts:
        parcelles_by_quartier[quartier_label or "Inconnu"] = count

    # Parcelles by avenue
    AvenueAlias = aliased(Avenue)
    avenue_counts = (
        parcelle_query
        .join(AvenueAlias, Adresse.fk_avenue == AvenueAlias.id)
        .with_entities(AvenueAlias.intitule, func.count(Parcelle.id))
        .group_by(AvenueAlias.intitule)
        .all()
    )
    parcelles_by_avenue = {}
    for avenue_label, count in avenue_counts:
        parcelles_by_avenue[avenue_label or "Inconnu"] = count

    return {
        "total_parcelles": total_parcelles,
        "total_biens": total_biens,
        "total_proprietaires": total_proprietaires,
        "total_population": total_population,
        "biens_by_nature": biens_by_nature,
        "parcelles_by_rang": parcelles_by_rang,
        "parcelles_by_commune": parcelles_by_commune,
        "parcelles_by_quartier": parcelles_by_quartier,
        "parcelles_by_avenue": parcelles_by_avenue,
    }

    
    