import zipfile
import json
import logging

from io import BytesIO
from typing import Optional, Dict, Any
from collections import defaultdict

from sqlalchemy import text, func, distinct, Date
from sqlalchemy.orm import Session, joinedload, aliased
from fastapi import APIRouter, Depends, HTTPException, File, Query, UploadFile, BackgroundTasks

from app.auth import get_current_active_user
from app.service import update_to_erecettes
from app.database import get_db
from app.models import Bien, Parcelle, Usage, UsageSpecifique, Adresse, Avenue, Quartier, Commune, Rang, NatureBien, Utilisateur, Personne, TypePersonne, Ville, Province, Unite, Menage


router = APIRouter()

logging.basicConfig(format='%(asctime)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


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

        if not bien_id:
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
    

def build_parcelle_filters_and_params(
    commune: Optional[str] = None,
    quartier: Optional[str] = None,
    avenue: Optional[str] = None,
    rang: Optional[str] = None,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
) -> tuple[list[str], dict]:
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
    if date_start:
        filters.append("CAST(p.date_create AS DATE) >= CAST(:date_start AS DATE)")
        params["date_start"] = date_start
    if date_end:
        filters.append("CAST(p.date_create AS DATE) <= CAST(:date_end AS DATE)")
        params["date_end"] = date_end
    return filters, params


def build_bien_filters_and_params(
    commune: Optional[str] = None,
    quartier: Optional[str] = None,
    avenue: Optional[str] = None,
    rang: Optional[str] = None,
    nature: Optional[str] = None,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
) -> tuple[list[str], dict]:
    filters, params = build_parcelle_filters_and_params(commune, quartier, avenue, rang, date_start, date_end)
    if nature:
        filters.append("nb.id = :nature")
        params["nature"] = nature
    return filters, params


def build_population_filters_and_params(
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
) -> tuple[list[str], dict]:
    filters = []
    params = {}
    if date_start:
        filters.append("CAST(per.date_create AS DATE) >= CAST(:date_start AS DATE)")
        params["date_start"] = date_start
    if date_end:
        filters.append("CAST(per.date_create AS DATE) <= CAST(:date_end AS DATE)")
        params["date_end"] = date_end
    return filters, params


@router.get("/geojson", tags=["GeoJSON"])
async def get_geojson(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=10000),
    date_start: str = Query(None),
    date_end: str = Query(None),
    province: int = Query(None),
    ville: int = Query(None),
    commune: int = Query(None),
    quartier: int = Query(None),
    avenue: int = Query(None),
    rang: int = Query(None),
    nature: int = Query(None),
    db: Session = Depends(get_db),
    current_user = Depends(get_current_active_user)
):
    try:
        # ==========================
        # 1. Parcelle IDs + filters
        # ==========================
        parcelle_q = db.query(Parcelle.id) \
            .outerjoin(Adresse, Parcelle.fk_adresse == Adresse.id) \
            .outerjoin(Avenue, Adresse.fk_avenue == Avenue.id) \
            .outerjoin(Quartier, Avenue.fk_quartier == Quartier.id) \
            .outerjoin(Commune, Quartier.fk_commune == Commune.id) \
            .outerjoin(Ville, Commune.fk_ville == Ville.id) \
            .outerjoin(Province, Ville.fk_province == Province.id) \
            .outerjoin(Rang, Parcelle.fk_rang == Rang.id)

        if date_start:
            parcelle_q = parcelle_q.filter(func.cast(Parcelle.date_create, Date) >= date_start)
        if date_end:
            parcelle_q = parcelle_q.filter(func.cast(Parcelle.date_create, Date) <= date_end)
        if province: parcelle_q = parcelle_q.filter(Province.id == province)
        if ville: parcelle_q = parcelle_q.filter(Ville.id == ville)
        if commune: parcelle_q = parcelle_q.filter(Commune.id == commune)
        if quartier: parcelle_q = parcelle_q.filter(Quartier.id == quartier)
        if avenue: parcelle_q = parcelle_q.filter(Avenue.id == avenue)
        if rang: parcelle_q = parcelle_q.filter(Rang.id == rang)

        total = parcelle_q.distinct(Parcelle.id).count()

        offset = (page - 1) * page_size
        parcelle_ids = [
            row[0] for row in parcelle_q
            .distinct(Parcelle.id)
            .order_by(Parcelle.id.desc())
            .offset(offset)
            .limit(page_size)
            .all()
        ]

        if not parcelle_ids:
            return {"data": [], "total": total, "page": page, "page_size": page_size}

        # ==========================
        # 2. Load parcelles with relationships
        # ==========================
        parcelles = (
            db.query(Parcelle)
            .options(
                joinedload(Parcelle.adresse)
                    .joinedload(Adresse.avenue)
                    .joinedload(Avenue.quartier)
                    .joinedload(Quartier.commune)
                    .joinedload(Commune.ville)
                    .joinedload(Ville.province),
                joinedload(Parcelle.rang),
                joinedload(Parcelle.proprietaire)
                    .joinedload(Personne.type_personne),
            )
            .filter(Parcelle.id.in_(parcelle_ids))
            .order_by(Parcelle.id.desc())
            .all()
        )

        # ==========================
        # 3. Load Biens + propriétaire fallback via Menage (with aliases!)
        # ==========================
        # Two different Personne tables → must be aliased
        PersonneDirect = aliased(Personne)   # For Bien.fk_proprietaire
        PersonneMenage = aliased(Personne)   # For Menage.fk_personne

        biens_q = (
            db.query(
                Bien,
                # Direct propriétaire
                PersonneDirect.nom.label("prop_nom"),
                PersonneDirect.postnom.label("prop_postnom"),
                PersonneDirect.prenom.label("prop_prenom"),
                # Via ménage
                PersonneMenage.nom.label("menage_prop_nom"),
                PersonneMenage.postnom.label("menage_prop_postnom"),
                PersonneMenage.prenom.label("menage_prop_prenom"),
                # Others
                Utilisateur.nom.label("agent_nom"),
                Utilisateur.prenom.label("agent_prenom"),
                Usage.intitule.label("usage_intitule"),
                UsageSpecifique.intitule.label("usage_specifique_intitule"),
                NatureBien.intitule.label("nature_bien_intitule"),
                Unite.intitule.label("unite_intitule")
            )
            .outerjoin(PersonneDirect, Bien.fk_proprietaire == PersonneDirect.id)
            .outerjoin(Menage, Bien.id == Menage.fk_bien)
            .outerjoin(PersonneMenage, Menage.fk_personne == PersonneMenage.id)
            .outerjoin(Utilisateur, Bien.fk_agent == Utilisateur.id)
            .outerjoin(NatureBien, Bien.fk_nature_bien == NatureBien.id)
            .outerjoin(Usage, Bien.fk_usage == Usage.id)
            .outerjoin(UsageSpecifique, Bien.fk_usage_specifique == UsageSpecifique.id)
            .outerjoin(Unite, Bien.fk_unite == Unite.id)
            .filter(Bien.fk_parcelle.in_(parcelle_ids))
        )

        if nature:
            biens_q = biens_q.filter(NatureBien.id == nature)

        biens_rows = biens_q.all()

        # ==========================
        # 4. Build biens map with correct propriétaire
        # ==========================
        biens_map = defaultdict(list)
        seen = set()

        for row in biens_rows:
            b = row[0]
            if b.id in seen:
                continue
            seen.add(b.id)

            # Priority: direct propriétaire → ménage → none
            if any([row.prop_nom, row.prop_postnom, row.prop_prenom]):
                prop_name = f"{row.prop_nom or ''} {row.prop_postnom or ''} {row.prop_prenom or ''}".strip()
            elif any([row.menage_prop_nom, row.menage_prop_postnom, row.menage_prop_prenom]):
                prop_name = f"{row.menage_prop_nom or ''} {row.menage_prop_postnom or ''} {row.menage_prop_prenom or ''}".strip()
            else:
                prop_name = None

            biens_map[b.fk_parcelle].append({
                "id": str(b.id),
                "superficie": float(b.superficie) if b.superficie else None,
                "superficie_corrige": float(b.superficie_corrige) if b.superficie_corrige else None,
                "nombre_etage": b.nombre_etage,
                "numero_etage": b.numero_etage,
                "proprietaire": prop_name or None,
                "nature_bien": row.nature_bien_intitule,
                "unite": row.unite_intitule,
                "usage": row.usage_intitule,
                "usage_specifique": row.usage_specifique_intitule,
                "coordinates": b.coord_corrige or b.coordinates,
                "date": b.date_create.isoformat() if b.date_create else None,
                "recense_par": f"{row.agent_prenom or ''} {row.agent_nom or ''}".strip() or None,
            })

        # ==========================
        # 5. Final response
        # ==========================
        data = []
        for p in parcelles:
            adr = p.adresse
            av = adr.avenue if adr else None
            q = av.quartier if av else None
            c = q.commune if q else None
            v = c.ville if c else None
            prov = v.province if v else None

            data.append({
                "id": str(p.id),
                "parcelle": {
                    "coordinates": p.coord_corrige or p.coordonnee_geographique,
                    "rang": p.rang.intitule if p.rang else None,
                    "superficie": float(p.superficie_calculee) if p.superficie_calculee else None,
                    "superficie_corrige": float(p.superficie_corrige) if p.superficie_corrige else None,
                },
                "date": p.date_create.isoformat() if p.date_create else None,
                "adresse": {
                    "numero": adr.numero if adr else None,
                    "avenue": av.intitule if av else None,
                    "quartier": q.intitule if q else None,
                    "commune": c.intitule if c else None,
                    "ville": v.intitule if v else None,
                    "province": prov.intitule if prov else None,
                },
                "proprietaire": {
                    "nom": p.proprietaire.nom if p.proprietaire else None,
                    "postnom": p.proprietaire.postnom if p.proprietaire else None,
                    "prenom": p.proprietaire.prenom if p.proprietaire else None,
                    "denomination": p.proprietaire.denomination if p.proprietaire else None,
                    "type_personne": p.proprietaire.type_personne.intitule if p.proprietaire and p.proprietaire.type_personne else None,
                },
                "biens": biens_map.get(p.id, [])
            })

        return {
            "data": data,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        import logging
        logging.exception("Error in get_geojson")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/geojson-summary", tags=["GeoJSON"])
async def get_geojson_summary(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=10000),
    date_start: str = Query(None),
    date_end: str = Query(None),
    db: Session = Depends(get_db),
    current_user = Depends(get_current_active_user),
):
    """
    Returns only parcelles SUCCESSFULLY synced to e-recettes (date_erecettes NOT NULL)
    Ordered by most recent sync first.
    """
    try:
        # ==========================
        # 1. Base query: ONLY synced parcelles
        # ==========================
        query = db.query(Parcelle).filter(Parcelle.date_erecettes.isnot(None))

        # Optional date_create filters
        if date_start:
            query = query.filter(func.cast(Parcelle.date_create, Date) >= date_start)
        if date_end:
            query = query.filter(func.cast(Parcelle.date_create, Date) <= date_end)

        # ==========================
        # 2. Total count (only synced ones)
        # ==========================
        total = query.count()

        # ==========================
        # 3. Pagination + ordering (most recent sync first)
        # ==========================
        offset_val = (page - 1) * page_size

        parcelles_paginated = (
            query
            .order_by(
                Parcelle.date_erecettes.desc(),   # Most recently synced first
                Parcelle.id.desc()                # Stable sort
            )
            .offset(offset_val)
            .limit(page_size)
            .all()
        )

        if not parcelles_paginated:
            return {
                "data": [],
                "total": total,
                "page": page,
                "page_size": page_size,
            }

        parcelle_ids = [p.id for p in parcelles_paginated]

        # ==========================
        # 4. Load full relations
        # ==========================
        parcelles = (
            db.query(Parcelle)
            .options(
                joinedload(Parcelle.adresse)
                    .joinedload(Adresse.avenue)
                    .joinedload(Avenue.quartier)
                    .joinedload(Quartier.commune)
                    .joinedload(Commune.ville)
                    .joinedload(Ville.province),
                joinedload(Parcelle.rang),
                joinedload(Parcelle.proprietaire)
                    .joinedload(Personne.type_personne),
            )
            .filter(Parcelle.id.in_(parcelle_ids))
            .order_by(
                Parcelle.date_erecettes.desc(),
                Parcelle.id.desc()
            )
            .all()
        )

        # ==========================
        # 5. Count biens
        # ==========================
        biens_count = (
            db.query(Bien.fk_parcelle, func.count(Bien.id))
            .filter(Bien.fk_parcelle.in_(parcelle_ids))
            .group_by(Bien.fk_parcelle)
            .all()
        )
        biens_count_map = {pid: cnt for pid, cnt in biens_count}

        # ==========================
        # 6. Build response
        # ==========================
        data = []
        for p in parcelles:
            adr = p.adresse
            av = adr.avenue if adr else None
            q = av.quartier if av else None
            c = q.commune if q else None
            v = c.ville if c else None
            prov = v.province if v else None

            data.append({
                "id": str(p.id),
                "parcelle": {
                    "coordinates": p.coord_corrige or p.coordonnee_geographique,
                    "rang": p.rang.intitule if p.rang else None,
                    "superficie": float(p.superficie_calculee) if p.superficie_calculee else None,
                    "superficie_corrige": float(p.superficie_corrige) if p.superficie_corrige else None,
                },
                "date": p.date_create.isoformat() if p.date_create else None,
                "date_erecettes": p.date_erecettes.isoformat(),  # Always exists here
                "adresse": {
                    "numero": adr.numero if adr else None,
                    "avenue": av.intitule if av else None,
                    "quartier": q.intitule if q else None,
                    "commune": c.intitule if c else None,
                    "ville": v.intitule if v else None,
                    "province": prov.intitule if prov else None,
                },
                "proprietaire": {
                    "nom": p.proprietaire.nom if p.proprietaire else None,
                    "postnom": p.proprietaire.postnom if p.proprietaire else None,
                    "prenom": p.proprietaire.prenom if p.proprietaire else None,
                    "denomination": p.proprietaire.denomination if p.proprietaire else None,
                    "type_personne": p.proprietaire.type_personne.intitule if p.proprietaire and p.proprietaire.type_personne else None,
                    "nif": p.proprietaire.nif if p.proprietaire else None,
                },
                "nombre_biens": biens_count_map.get(p.id, 0),
            })

        return {
            "data": data,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    except Exception as e:
        import logging
        logging.exception("Error in get_geojson_summary")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/import-geojson", tags=["GeoJSON"])
async def import_geojson_zip(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    # current_user = Depends(get_current_active_user),
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


# Group 1: Core stats - separate simple queries for each count
@router.get("/stats/dashboard/core", tags=["Stats"])
def get_core_stats(
    commune: Optional[str] = Query(None),
    quartier: Optional[str] = Query(None),
    avenue: Optional[str] = Query(None),
    rang: Optional[str] = Query(None),
    nature: Optional[str] = Query(None),
    date_start: Optional[str] = Query(None),
    date_end: Optional[str] = Query(None),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Accessible parcelles
        parcelle_filters, parcelle_params = build_parcelle_filters_and_params(commune, quartier, avenue, rang, date_start, date_end)
        # filter_clause = " AND " + " AND ".join(parcelle_filters) if parcelle_filters else ""
        accessible_query = f"""
            SELECT COUNT(p.id) 
            FROM parcelle p
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN rang r ON p.fk_rang = r.id
            WHERE p.statut = 1
        """
            # WHERE p.statut = 1{filter_clause}
        total_parcelles_accessibles = db.execute(text(accessible_query), parcelle_params).scalar() or 0

        # Inaccessible parcelles (similar, but statut=2)
        inaccessible_query = accessible_query.replace("p.statut = 1", "p.statut = 2")
        total_parcelles_inaccessibles = db.execute(text(inaccessible_query), parcelle_params).scalar() or 0

        # Total biens
        bien_filters, bien_params = build_bien_filters_and_params(commune, quartier, avenue, rang, nature, date_start, date_end)
        # bien_filter_clause = " AND " + " AND ".join(bien_filters) if bien_filters else ""
        bien_query = f"""
            SELECT COUNT(p.id) 
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
            # WHERE 1=1{bien_filter_clause}
        total_biens = db.execute(text(bien_query), bien_params).scalar() or 0

        # Total proprietaires: distinct fk_proprietaire from parcelles (no nature) and biens (with nature)
        parcelle_subquery = f"""
            SELECT p.id 
            FROM parcelle p
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN rang r ON p.fk_rang = r.id
            WHERE 1=1
        """
            # WHERE 1=1{filter_clause}
            
        # For biens subquery, remove nature filter for proprietaires
        parcelle_bien_filters = [f for f in bien_filters if "nb.id" not in f]
        parcelle_bien_params = {k: v for k, v in bien_params.items() if k != "nature"}
        parcelle_bien_filter_clause = " AND " + " AND ".join(parcelle_bien_filters) if parcelle_bien_filters else ""
        bien_subquery = f"""
            SELECT b.id 
            FROM bien b
            LEFT JOIN parcelle p ON b.fk_parcelle = p.id
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN rang r ON p.fk_rang = r.id
            WHERE 1=1
        """
            # WHERE 1=1{parcelle_bien_filter_clause}
            
        proprietaire_query = f"""
            SELECT COUNT(DISTINCT person_id)
            FROM (
                SELECT p.fk_proprietaire AS person_id
                FROM ({parcelle_subquery}) fp
                JOIN parcelle p ON fp.id = p.id
                WHERE p.fk_proprietaire IS NOT NULL
                UNION
                SELECT b.fk_proprietaire AS person_id
                FROM ({bien_subquery}) fb
                JOIN bien b ON fb.id = b.id
                WHERE b.fk_proprietaire IS NOT NULL
            ) AS all_owners
        """
        all_params = {**parcelle_params, **parcelle_bien_params}
        total_proprietaires = db.execute(text(proprietaire_query), all_params).scalar() or 0

        # Total population: simple count from personne where fk_type_personne = 1, with date filters only
        pop_filters, pop_params = build_population_filters_and_params(date_start, date_end)
        pop_filter_clause = " AND " + " AND ".join(pop_filters) if pop_filters else ""
        population_query = f"""
            SELECT COUNT(per.id) 
            FROM personne per
        """
            # WHERE per.id is not null {pop_filter_clause}
        total_population = db.execute(text(population_query), pop_params).scalar() or 0

        return {
            "total_parcelles_accessibles": total_parcelles_accessibles,
            "total_parcelles_inaccessibles": total_parcelles_inaccessibles,
            "total_biens": total_biens,
            "total_proprietaires": total_proprietaires,
            "total_population": total_population,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Group 2: Biens breakdowns - separate simple queries
@router.get("/stats/dashboard/biens", tags=["Stats"])
def get_biens_breakdowns(
    commune: Optional[str] = Query(None),
    quartier: Optional[str] = Query(None),
    avenue: Optional[str] = Query(None),
    rang: Optional[str] = Query(None),
    nature: Optional[str] = Query(None),
    date_start: Optional[str] = Query(None),
    date_end: Optional[str] = Query(None),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        bien_filters, params = build_bien_filters_and_params(commune, quartier, avenue, rang, nature, date_start, date_end)
        filter_clause = " AND " + " AND ".join(bien_filters) if bien_filters else ""
        base_bien_query = f"""
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
            WHERE 1=1{filter_clause}
        """

        # biens_by_nature
        biens_by_nature_query = f"""
            SELECT COALESCE(nb.intitule, 'Inconnu'), COUNT(b.id)
            {base_bien_query}
            GROUP BY nb.id, nb.intitule
        """
        biens_by_nature = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(biens_by_nature_query), params).fetchall()
        }

        # biens_by_rang
        biens_by_rang_query = biens_by_nature_query.replace("COALESCE(nb.intitule, 'Inconnu')", "COALESCE(r.intitule, 'Inconnu')").replace("nb.id, nb.intitule", "r.id, r.intitule")
        biens_by_rang = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(biens_by_rang_query), params).fetchall()
        }

        # biens_by_usage
        biens_by_usage_query = biens_by_nature_query.replace("nb.intitule", "u.intitule").replace("nb.id, nb.intitule", "u.id, u.intitule")
        biens_by_usage = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(biens_by_usage_query), params).fetchall()
        }

        # biens_by_usage_specifique
        biens_by_usage_specifique_query = biens_by_nature_query.replace("nb.intitule", "us.intitule").replace("nb.id, nb.intitule", "us.id, us.intitule")
        biens_by_usage_specifique = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(biens_by_usage_specifique_query), params).fetchall()
        }

        return {
            "biens_by_nature": biens_by_nature,
            "biens_by_rang": biens_by_rang,
            "biens_by_usage": biens_by_usage,
            "biens_by_usage_specifique": biens_by_usage_specifique,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Group 3: Parcelles breakdowns - separate simple queries (no nature)
@router.get("/stats/dashboard/parcelles", tags=["Stats"])
def get_parcelles_breakdowns(
    commune: Optional[str] = Query(None),
    quartier: Optional[str] = Query(None),
    avenue: Optional[str] = Query(None),
    rang: Optional[str] = Query(None),
    date_start: Optional[str] = Query(None),
    date_end: Optional[str] = Query(None),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        parcelle_filters, params = build_parcelle_filters_and_params(commune, quartier, avenue, rang, date_start, date_end)
        filter_clause = " AND " + " AND ".join(parcelle_filters) if parcelle_filters else ""
        base_parcelle_query = f"""
            FROM parcelle p
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            LEFT JOIN commune c ON q.fk_commune = c.id
            LEFT JOIN rang r ON p.fk_rang = r.id
            WHERE 1=1{filter_clause}
        """

        # parcelles_by_rang
        parcelles_by_rang_query = f"""
            SELECT COALESCE(r.intitule, 'Inconnu'), COUNT(p.id)
            {base_parcelle_query}
            GROUP BY r.id, r.intitule
        """
        parcelles_by_rang = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(parcelles_by_rang_query), params).fetchall()
        }

        # parcelles_by_commune (only fk_ville=1)
        parcelles_by_commune_query = f"""
            SELECT c.intitule, COUNT(p.id)
            FROM parcelle p
            LEFT JOIN adresse a ON p.fk_adresse = a.id
            LEFT JOIN avenue av ON a.fk_avenue = av.id
            LEFT JOIN quartier q ON av.fk_quartier = q.id
            INNER JOIN commune c ON q.fk_commune = c.id AND c.fk_ville = 1
            LEFT JOIN rang r ON p.fk_rang = r.id
            WHERE 1=1{filter_clause}
            GROUP BY c.id, c.intitule
        """
        parcelles_by_commune = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(parcelles_by_commune_query), params).fetchall()
        }

        # parcelles_by_quartier
        parcelles_by_quartier_query = f"""
            SELECT CONCAT(COALESCE(q.intitule, ''), ' (', COALESCE(c.intitule, ''), ')'), COUNT(p.id)
            {base_parcelle_query}
            GROUP BY q.id, q.intitule, c.id, c.intitule
        """
        parcelles_by_quartier = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(parcelles_by_quartier_query), params).fetchall()
        }

        # parcelles_by_avenue
        parcelles_by_avenue_query = f"""
            SELECT CONCAT(COALESCE(av.intitule, ''), ' (', COALESCE(c.intitule, ''), ')'), COUNT(p.id)
            {base_parcelle_query}
            GROUP BY av.id, av.intitule, c.id, c.intitule
        """
        parcelles_by_avenue = {
            row[0] if row[0] else "Inconnu": row[1]
            for row in db.execute(text(parcelles_by_avenue_query), params).fetchall()
        }

        return {
            "parcelles_by_rang": parcelles_by_rang,
            "parcelles_by_commune": parcelles_by_commune,
            "parcelles_by_quartier": parcelles_by_quartier,
            "parcelles_by_avenue": parcelles_by_avenue,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))