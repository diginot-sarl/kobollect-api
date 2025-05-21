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
        # Parse date filters
        start_date = datetime.strptime(date_start, "%Y-%m-%d").date()
        end_date = datetime.strptime(date_end, "%Y-%m-%d").date()

        # Base query for parcelles
        parcelle_query = (
            db.query(Parcelle.id)
            .join(Adresse, Parcelle.fk_adresse == Adresse.id)
            .join(Avenue, Adresse.fk_avenue == Avenue.id)
            .join(Quartier, Avenue.fk_quartier == Quartier.id)
            .join(Commune, Quartier.fk_commune == Commune.id)
            .join(Personne, Parcelle.fk_proprietaire == Personne.id)
            .filter(Personne.fk_type_personne == 1)
        )

        # Apply filters
        if commune:
            parcelle_query = parcelle_query.filter(Commune.id == commune)
        if quartier:
            parcelle_query = parcelle_query.filter(Quartier.id == quartier)
        if avenue:
            parcelle_query = parcelle_query.filter(Avenue.id == avenue)
        if rang:
            parcelle_query = parcelle_query.filter(Parcelle.fk_rang == rang)
        if keyword:
            parcelle_query = parcelle_query.filter(
                or_(
                    Personne.nom.ilike(f"%{keyword}%"),
                    Personne.postnom.ilike(f"%{keyword}%"),
                    Personne.prenom.ilike(f"%{keyword}%"),
                    Personne.denomination.ilike(f"%{keyword}%"),
                    Personne.sigle.ilike(f"%{keyword}%")
                )
            )

        # Get distinct parcelle IDs
        parcelle_ids = [row[0] for row in parcelle_query.distinct().all()]

        if not parcelle_ids:
            return {
                "data": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
            }

        # Subquery to get all person IDs
        owner_subquery = (
            db.query(Personne.id.label("person_id"))
            .join(Parcelle, Parcelle.fk_proprietaire == Personne.id)
            .filter(Parcelle.id.in_(parcelle_ids))
            .filter(Personne.fk_type_personne == 1)
        )

        menage_subquery = (
            db.query(Personne.id.label("person_id"))
            .join(Menage, Menage.fk_personne == Personne.id)
            .join(Bien, Menage.fk_bien == Bien.id)
            .filter(Bien.fk_parcelle.in_(parcelle_ids))
            .filter(Personne.fk_type_personne == 1)
        )

        location_subquery = (
            db.query(Personne.id.label("person_id"))
            .join(LocationBien, LocationBien.fk_personne == Personne.id)
            .join(Bien, LocationBien.fk_bien == Bien.id)
            .filter(Bien.fk_parcelle.in_(parcelle_ids))
            .filter(Personne.fk_type_personne == 1)
        )

        membre_menage_subquery = (
            db.query(Personne.id.label("person_id"))
            .join(MembreMenage, MembreMenage.fk_personne == Personne.id)
            .join(Menage, MembreMenage.fk_menage == Menage.id)
            .join(Bien, Menage.fk_bien == Bien.id)
            .filter(Bien.fk_parcelle.in_(parcelle_ids))
        )

        # Union all person IDs and apply date filter
        person_query = (
            owner_subquery
            .union(menage_subquery, location_subquery, membre_menage_subquery)
            .subquery()
        )

        person_ids_query = (
            db.query(Personne.id, Personne.date_create)
            .join(person_query, Personne.id == person_query.c.person_id)
            .filter(
                func.cast(Personne.date_create, Date) >= start_date,
                func.cast(Personne.date_create, Date) < end_date
            )
            .order_by(Personne.date_create.desc())
        )

        # Get person IDs
        person_ids = [row[0] for row in person_ids_query.all()]

        # Pagination
        total = len(person_ids)
        start = (page - 1) * page_size
        end = start + page_size
        paginated_ids = person_ids[start:end]

        # Subquery for responsible person name
        responsable_subquery = (
            db.query(
                func.concat(Personne.nom, " ", Personne.prenom).label("nom_responsable"),
                MembreMenage.fk_personne.label("membre_personne_id")
            )
            .join(Menage, Menage.fk_personne == Personne.id)
            .join(MembreMenage, MembreMenage.fk_menage == Menage.id)
            .subquery()
        )

        # Main query for person details
        person_details_query = (
            db.query(
                Personne.id,
                Personne.nom,
                Personne.postnom,
                Personne.prenom,
                Personne.fk_lien_parente,
                Personne.nif,
                Personne.lieu_naissance,
                Personne.date_naissance,
                Personne.profession,
                Personne.etat_civil,
                Personne.telephone,
                Personne.adresse_mail,
                Personne.niveau_etude,
                Personne.date_create,
                TypePersonne.id.label("type_personne_id"),
                TypePersonne.intitule.label("type_personne_intitule"),
                Nationalite.intitule.label("nationalite"),
                Adresse.numero.label("adresse_numero"),
                Avenue.intitule.label("avenue"),
                Quartier.intitule.label("quartier"),
                Commune.intitule.label("commune"),
                case(
                    (
                        Personne.id.in_(
                            db.query(Parcelle.fk_proprietaire).filter(Parcelle.id.in_(parcelle_ids))
                        ),
                        "Propriétaire",
                    ),
                    (
                        Personne.id.in_(
                            db.query(Menage.fk_personne)
                            .join(Bien, Menage.fk_bien == Bien.id)
                            .filter(Bien.fk_parcelle.in_(parcelle_ids))
                        ),
                        "Responsable menage",
                    ),
                    (
                        Personne.id.in_(
                            db.query(MembreMenage.fk_personne)
                            .join(Menage, MembreMenage.fk_menage == Menage.id)
                            .join(Bien, Menage.fk_bien == Bien.id)
                            .filter(Bien.fk_parcelle.in_(parcelle_ids))
                        ),
                        "Membre menage",
                    ),
                    else_="Inconnu",
                ).label("categorie"),
                responsable_subquery.c.nom_responsable
            )
            .outerjoin(TypePersonne, Personne.fk_type_personne == TypePersonne.id)
            .outerjoin(Nationalite, Personne.fk_nationalite == Nationalite.id)
            .outerjoin(Adresse, Personne.fk_adresse == Adresse.id)
            .outerjoin(Avenue, Adresse.fk_avenue == Avenue.id)
            .outerjoin(Quartier, Avenue.fk_quartier == Quartier.id)
            .outerjoin(Commune, Quartier.fk_commune == Commune.id)
            .outerjoin(responsable_subquery, Personne.id == responsable_subquery.c.membre_personne_id)
            .filter(Personne.id.in_(paginated_ids))
        )

        # Execute query and fetch results
        person_results = person_details_query.all()

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
            "date_create": row.date_create.isoformat() if row.date_create else None,
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