import zipfile
import json
import logging
from io import BytesIO
from typing import Optional
from collections import defaultdict
from sqlalchemy import text, func, distinct, Date, case, desc
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.sql import text, func, cast, and_, or_  # Add Date and or_ here
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    File,
    Query,
    UploadFile,
    BackgroundTasks
)
from app.auth import get_current_active_user
from app.service import (
    update_to_erecettes
)
from app.database import get_db
from app.models import (
    Bien,
    Parcelle,
    Usage,
    UsageSpecifique,
    Adresse,
    Avenue,
    Quartier,
    Commune,
    Rang,
    NatureBien,
    Utilisateur,
    Personne,
    TypePersonne,
    Ville,
    Province,
    Unite,
)


router = APIRouter()

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

        # Group biens under parcelles with dedup by bien_id
        parcelles_dict = defaultdict(lambda: {
            "id": None,
            "coordinates": None,
            "date": None,
            "adresse": {},
            "proprietaire": {},
            "biens": [],
            "_seen_bien_ids": set()  # track seen biens for this parcelle
        })

        for row in results:
            parcelle_id = row.parcelle_id
            entry = parcelles_dict[parcelle_id]

            if entry["id"] is None:
                entry["id"] = str(parcelle_id)
                entry["coordinates"] = row.coordonnee_geographique
                entry["date"] = row.parcelle_date_create.isoformat() if row.parcelle_date_create else None
                entry["adresse"] = {
                    "numero": row.adresse_numero,
                    "avenue": row.avenue,
                    "quartier": row.quartier,
                    "commune": row.commune
                }
                entry["proprietaire"] = {
                    "nom": row.proprietaire_nom,
                    "postnom": row.proprietaire_postnom,
                    "prenom": row.proprietaire_prenom,
                    "denomination": row.proprietaire_denomination,
                    "type_personne": row.type_personne
                }

            # only append bien if not seen for this parcelle
            if row.bien_id:
                if row.bien_id not in entry["_seen_bien_ids"]:
                    entry["biens"].append({
                        "id": str(row.bien_id),
                        "coordinates": row.bien_coordinates,
                        "date": row.bien_date_create.isoformat() if row.bien_date_create else None,
                        "nature": row.nature,
                        "recense_par": row.recense_par
                    })
                    entry["_seen_bien_ids"].add(row.bien_id)

        # remove internal _seen_bien_ids before returning
        for v in parcelles_dict.values():
            v.pop("_seen_bien_ids", None)

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
'''


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
        # Étape 1. Construction filtres de base (avec propriétaire)
        # ==========================
        parcelle_q = db.query(Parcelle.id)
        parcelle_q = parcelle_q \
            .outerjoin(Adresse, Parcelle.fk_adresse == Adresse.id) \
            .outerjoin(Avenue, Adresse.fk_avenue == Avenue.id) \
            .outerjoin(Quartier, Avenue.fk_quartier == Quartier.id) \
            .outerjoin(Commune, Quartier.fk_commune == Commune.id) \
            .outerjoin(Ville, Commune.fk_ville == Ville.id) \
            .outerjoin(Province, Ville.fk_province == Province.id) \
            .outerjoin(Rang, Parcelle.fk_rang == Rang.id) \
            .outerjoin(Personne, Parcelle.fk_proprietaire == Personne.id)  # Ajouté

        # Application des filtres
        if date_start:
            parcelle_q = parcelle_q.filter(func.cast(Parcelle.date_create, Date) >= func.cast(date_start, Date))
        if date_end:
            parcelle_q = parcelle_q.filter(func.cast(Parcelle.date_create, Date) <= func.cast(date_end, Date))
        if province:
            parcelle_q = parcelle_q.filter(Province.id == province)
        if ville:
            parcelle_q = parcelle_q.filter(Ville.id == ville)
        if commune:
            parcelle_q = parcelle_q.filter(Commune.id == commune)
        if quartier:
            parcelle_q = parcelle_q.filter(Quartier.id == quartier)
        if avenue:
            parcelle_q = parcelle_q.filter(Avenue.id == avenue)
        if rang:
            parcelle_q = parcelle_q.filter(Rang.id == rang)

        # ==========================
        # Étape 2. Total distinct (avec propriétaire)
        # ==========================
        total = db.query(func.count(distinct(Parcelle.id))) \
            .select_from(Parcelle) \
            .outerjoin(Adresse, Parcelle.fk_adresse == Adresse.id) \
            .outerjoin(Avenue, Adresse.fk_avenue == Avenue.id) \
            .outerjoin(Quartier, Avenue.fk_quartier == Quartier.id) \
            .outerjoin(Commune, Quartier.fk_commune == Commune.id) \
            .outerjoin(Ville, Commune.fk_ville == Ville.id) \
            .outerjoin(Province, Ville.fk_province == Province.id) \
            .outerjoin(Rang, Parcelle.fk_rang == Rang.id) \
            .outerjoin(Personne, Parcelle.fk_proprietaire == Personne.id)  # Ajouté

        if date_start:
            total = total.filter(func.cast(Parcelle.date_create, Date) >= func.cast(date_start, Date))
        if date_end:
            total = total.filter(func.cast(Parcelle.date_create, Date) <= func.cast(date_end, Date))
        if province:
            total = total.filter(Province.id == province)
        if ville:
            total = total.filter(Ville.id == ville)
        if commune:
            total = total.filter(Commune.id == commune)
        if quartier:
            total = total.filter(Quartier.id == quartier)
        if avenue:
            total = total.filter(Avenue.id == avenue)
        if rang:
            total = total.filter(Rang.id == rang)

        total = total.scalar() or 0

        # ==========================
        # Étape 3. Pagination & IDs distincts
        # ==========================
        offset = (page - 1) * page_size
        parcelle_ids = [
            row[0] for row in parcelle_q.distinct(Parcelle.id)
            .order_by(Parcelle.id.desc())
            .offset(offset).limit(page_size).all()
        ]

        if not parcelle_ids:
            return {"data": [], "total": total, "page": page, "page_size": page_size}

        # ==========================
        # Étape 4. Charger Parcelles avec relations (propriétaire activé)
        # ==========================
        parcelles = (
            db.query(Parcelle)
            .options(
                # Adresse complète
                joinedload(Parcelle.adresse)
                    .joinedload(Adresse.avenue)
                    .joinedload(Avenue.quartier)
                    .joinedload(Quartier.commune)
                    .joinedload(Commune.ville)
                    .joinedload(Ville.province),
                # Rang
                joinedload(Parcelle.rang),
                # Propriétaire de la parcelle + type personne
                joinedload(Parcelle.proprietaire)
                    .joinedload(Personne.type_personne),
            )
            .filter(Parcelle.id.in_(parcelle_ids))
            .order_by(Parcelle.id.desc())
            .all()
        )

        # ==========================
        # Étape 5. Charger Biens en batch
        # ==========================
        biens_q = (
            db.query(
                Bien,
                Personne.nom.label("proprietaire_nom"),
                Personne.postnom.label("proprietaire_postnom"),
                Personne.prenom.label("proprietaire_prenom"),
                Utilisateur.nom.label("agent_nom"),
                Utilisateur.prenom.label("agent_prenom"),
                Usage.intitule.label("usage_intitule"),
                UsageSpecifique.intitule.label("usage_specifique_intitule"),
                NatureBien.intitule.label("nature_bien_intitule"),
                Unite.intitule.label("unite_intitule")
            )
            .outerjoin(Personne, Bien.fk_proprietaire == Personne.id)
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

        biens_map = defaultdict(list)
        seen_bien = set()
        for (
            b, proprietaire_nom, proprietaire_postnom, proprietaire_prenom,
            agent_nom, agent_prenom, usage_intitule, usage_specifique_intitule,
            nature_bien_intitule, unite_intitule
        ) in biens_rows:
            if b.id in seen_bien:
                continue

            proprietaire_full_name = (
                f"{proprietaire_nom or ''} {proprietaire_postnom or ''} {proprietaire_prenom or ''}".strip()
                if b.fk_proprietaire else None
            )

            seen_bien.add(b.id)
            biens_map[b.fk_parcelle].append({
                "id": str(b.id),
                "superficie": b.superficie,
                "superficie_corrige": b.superficie_corrige,
                "nombre_etage": b.nombre_etage,
                "numero_etage": b.numero_etage,
                "proprietaire": proprietaire_full_name,
                "nature_bien": nature_bien_intitule if b.fk_nature_bien else None,
                "unite": unite_intitule if b.fk_unite else None,
                "usage": usage_intitule if b.fk_usage else None,
                "usage_specifique": usage_specifique_intitule if b.fk_usage_specifique else None,
                "coordinates": b.coordinates,
                "date": b.date_create.isoformat() if b.date_create else None,
                "recense_par": None
            })

        # ==========================
        # Étape 6. Assembler la réponse finale
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
                    "coordinates": p.coordonnee_geographique,
                    "rang": p.rang.intitule if p.rang else None,
                    "superficie": p.superficie_calculee,
                    "superficie_corrige": p.superficie_corrige,
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
                    "type_personne": p.proprietaire.type_personne.intitule if (
                        p.proprietaire and p.proprietaire.type_personne
                    ) else None
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
        import traceback, logging
        logging.exception("Error in get_geojson ORM version")
        raise HTTPException(status_code=500, detail=str(e))


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
    
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from app.models import Parcelle, Bien, Personne  # Import relevant models if using ORM, but sticking to raw for perf
# Assuming imports for router, get_db, get_current_active_user

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