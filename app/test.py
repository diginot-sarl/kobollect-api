@router.get("/population-by-age-pyramid", tags=["Populations"])
def get_age_pyramid(
    commune: Optional[str] = Query(None),
    quartier: Optional[str] = Query(None),
    avenue: Optional[str] = Query(None),
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        # Define age groups
        age_groups = [
            {"min": 0, "max": 4},
            {"min": 5, "max": 9},
            {"min": 10, "max": 14},
            {"min": 15, "max": 19},
            {"min": 20, "max": 24},
            {"min": 25, "max": 29},
            {"min": 30, "max": 34},
            {"min": 35, "max": 39},
            {"min": 40, "max": 44},
            {"min": 45, "max": 49},
            {"min": 50, "max": 54},
            {"min": 55, "max": 59},
            {"min": 60, "max": 64},
            {"min": 65, "max": 69},
            {"min": 70, "max": 74},
            {"min": 75, "max": 200}  # 75+ with a high max age
        ]

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

        if not parcelle_ids:
            return []

        # Convert parcelle_ids to a comma-separated string
        parcelle_ids_str = ",".join(str(id) for id in parcelle_ids)

        # Query to get population statistics by age group and sex
        population_query = """
            WITH age_groups AS (
                SELECT * FROM (VALUES
                    (0, 4), (5, 9), (10, 14), (15, 19),
                    (20, 24), (25, 29), (30, 34), (35, 39),
                    (40, 44), (45, 49), (50, 54), (55, 59),
                    (60, 64), (65, 69), (70, 74), (75, 200)
                ) AS t(min_age, max_age)
            ),
            population AS (
                SELECT 
                    p.id,
                    p.sexe,
                    CASE 
                        WHEN DATEADD(YEAR, DATEDIFF(YEAR, p.date_naissance, GETDATE()), p.date_naissance) > GETDATE()
                        THEN DATEDIFF(YEAR, p.date_naissance, GETDATE()) - 1
                        ELSE DATEDIFF(YEAR, p.date_naissance, GETDATE())
                    END AS age
                FROM personne p
                WHERE p.date_naissance IS NOT NULL
                AND p.id IN (
                    SELECT p.fk_proprietaire FROM parcelle WHERE id IN (SELECT * FROM string_split(:parcelle_ids, ',')) AND p.fk_proprietaire IS NOT NULL
                    UNION
                    SELECT m.fk_personne FROM bien b JOIN menage m ON b.id = m.fk_bien WHERE b.fk_parcelle IN (SELECT * FROM string_split(:parcelle_ids, ','))
                    UNION
                    SELECT lb.fk_personne FROM bien b JOIN location_bien lb ON b.id = lb.fk_bien WHERE b.fk_parcelle IN (SELECT * FROM string_split(:parcelle_ids, ','))
                    UNION
                    SELECT mm.fk_personne FROM bien b JOIN menage m ON b.id = m.fk_bien JOIN membre_menage mm ON m.id = mm.fk_menage WHERE b.fk_parcelle IN (SELECT * FROM string_split(:parcelle_ids, ','))
                )
            )
            SELECT 
                ag.min_age,
                ag.max_age,
                SUM(CASE WHEN pop.sexe = 'M' THEN 1 ELSE 0 END) AS male_count,
                SUM(CASE WHEN pop.sexe = 'F' THEN 1 ELSE 0 END) AS female_count
            FROM age_groups ag
            LEFT JOIN population pop ON pop.age BETWEEN ag.min_age AND ag.max_age
            GROUP BY ag.min_age, ag.max_age
            ORDER BY ag.min_age
        """

        # Execute query
        results = db.execute(text(population_query), {"parcelle_ids": parcelle_ids_str}).fetchall()

        # Format results
        age_pyramid = []
        for row in results:
            min_age = row[0]
            max_age = row[1]
            age_group = f"{min_age}-{max_age}" if max_age < 200 else "75+"
            
            male_count = row[2] or 0
            female_count = row[3] or 0
            total = male_count + female_count
            
            male_proportion = round((male_count / total) * 100, 1) if total > 0 else 0
            female_proportion = round((female_count / total) * 100, 1) if total > 0 else 0

            age_pyramid.append({
                "ageGroup": age_group,
                "masculinCount": male_count,
                "masculinProportion": male_proportion,
                "femininCount": female_count,
                "femininProportion": female_proportion
            })

        return age_pyramid

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))