# app/service.py
from sqlalchemy.orm import Session
from fastapi import HTTPException, status
import logging
from app.models import Adresse, Personne, Parcelle, Bien, LocationBien, Utilisateur, Logs, Menage, MembreMenage, RapportRecensement

logger = logging.getLogger(__name__)

def process_recensement_form(payload: dict, db: Session):
    kobo: dict = payload
    record_id = kobo.get("id", kobo.get("_id"))
    
    logger.info(f"Données kobo : {kobo}")

    try:
        existing_agent = db.query(Utilisateur).filter(Utilisateur.id_kobo == kobo["_submitted_by"]).first()
        if existing_agent:
            fk_agent = existing_agent.id
        else:
            fk_agent = 1  # Default agent ID if not found
        
        # Check if the ID already exists in the logs table
        existing_log = db.query(Logs).filter(Logs.id_kobo == record_id).first()
        if existing_log:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Le formulaire avec ID {record_id} déjà existante.")

        # Initialize variables
        fk_proprietaire = None
        
        if kobo.get("parcelle_accessible_ou_non") == "oui":

            # 1. Insert into Adresse
            adresse = Adresse(
                fk_avenue=int(kobo["adresse_de_la_parcelle/avenue"]),  # Assuming this is an ID
                numero=kobo["adresse_de_la_parcelle/numero_parcellaire"],
                fk_agent=fk_agent,
            )
            db.add(adresse)
            db.flush()  # Flush to get the inserted ID
            fk_adresse = adresse.id

            # 2. Insert Propriétaire into Personne
            
            proprietaire = Personne(
                nom=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nom_2"),
                postnom=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/post_nom_2"),
                prenom=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/prenom_2"),
                sexe=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/genre_2"),
                
                lieu_naissance=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Lieu_de_naissance_001"),
                date_naissance=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Date_de_naissance_001"),
                
                fk_type_personne=int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/tp")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/tp") else None,
                
                fk_nationalite=int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nationalite_5")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nationalite_5") else None,
                
                fk_province=int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/province_d_origine_5")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/province_d_origine_5") else None,
                
                district=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/district_5"),
                territoire=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/territoire_5"),
                secteur=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/secteur_5"),
                village=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/village_5"),
                profession=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/profession_5"),
                type_piece_identite=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/type_de_piece_d_identite_5"),
                numero_piece_identite=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/numero_piece_d_identite_5"),
                nom_du_pere=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nom_du_pere_5"),
                nom_de_la_mere=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nom_de_la_mere_5"),
                nombre_enfant=int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Nombre_d_enfant")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Nombre_d_enfant") else None,
                etat_civil=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/etat_civil_5"),
                fk_lien_parente=int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/lien_de_parente_5")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/lien_de_parente_5") else None,
                niveau_etude=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/niveau_d_etudes_5"),
                
                telephone=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/n_telephone_2"),
                adresse_mail=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/adresse_email_2"),
                
                denomination=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/denomination_2"),
                sigle=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/sigle_2"),
                numero_impot=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/numero_d_impot_2"),
                rccm=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/rccm_2"),
                id_nat=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/id_nat_2"),
                domaine_activite=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/domaine_d_activite_2"),
                fk_adresse=fk_adresse,
                fk_agent=fk_agent,
            )
            db.add(proprietaire)
            db.flush()
            fk_proprietaire = proprietaire.id

            # 3. Insert into Parcelle
            parcelle = Parcelle(
                numero_parcellaire=kobo.get("adresse_de_la_parcelle/numero_parcellaire"),
                fk_unite=int(kobo.get("adresse_de_la_parcelle/unite_de_la_superficie")) if kobo.get("adresse_de_la_parcelle/unite_de_la_superficie") else None,
                longueur=float(kobo.get("adresse_de_la_parcelle/longueur")) if kobo.get("adresse_de_la_parcelle/longueur") else None,
                largeur=float(kobo.get("adresse_de_la_parcelle/largeur")) if kobo.get("adresse_de_la_parcelle/largeur") else None,
                superficie_calculee=float(kobo.get("adresse_de_la_parcelle/calculation")) if kobo.get("adresse_de_la_parcelle/calculation") else None,
                coordonnee_geographique=kobo.get("adresse_de_la_parcelle/coordonne_geographique"),
                fk_rang=int(kobo.get("adresse_de_la_parcelle/rang")) if kobo.get("adresse_de_la_parcelle/rang") else None,
                fk_proprietaire=fk_proprietaire,
                fk_adresse=fk_adresse,
                fk_agent=fk_agent,
            )
            db.add(parcelle)
            db.flush()
            fk_parcelle = parcelle.id
            
            
            for menage in kobo.get("informations_du_menage", []):
                menage: dict = menage
                
                fk_responsable = fk_proprietaire if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/le_proprietaire_habite_t_il_dans_la_parcelle") == "oui" else None
                                
                # 4. Insert into Bien
                bien = Bien(                    
                    coordinates=menage.get("informations_du_menage/informations_du_bien/coordonnee_geographique"),
                    superficie=menage.get("informations_du_menage/informations_du_bien/superficie"),
                    fk_parcelle=fk_parcelle,
                    fk_nature_bien=int(menage.get("informations_du_menage/informations_du_bien/nature")) 
                                    if menage.get("informations_du_menage/informations_du_bien/nature") else None,
                    fk_unite=int(menage.get("informations_du_menage/informations_du_bien/unite_de_la_superficie_1")) 
                              if menage.get("informations_du_menage/informations_du_bien/unite_de_la_superficie_1") else None,
                    fk_usage=int(menage.get("informations_du_menage/informations_du_bien/usage")) 
                              if menage.get("informations_du_menage/informations_du_bien/usage") else None,
                    fk_usage_specifique=int(menage.get("informations_du_menage/informations_du_bien/usage_specifique")) 
                                         if menage.get("informations_du_menage/informations_du_bien/usage_specifique") else None,
                    fk_agent=fk_agent,
                )
                db.add(bien)
                db.flush()
                fk_bien = bien.id
                
                # 4. Insert Locataire into Personne (if the occupant is a locataire)
                if menage.get("informations_du_menage/informations_de_l_occupant/occupant_est_locataire_ou_proprietaire_2") == "locataire":
                    
                    responsable = Personne(
                        nom=menage.get("informations_du_menage/informations_de_l_occupant/nom"),
                        postnom=menage.get("informations_du_menage/informations_de_l_occupant/post_nom"),
                        prenom=menage.get("informations_du_menage/informations_de_l_occupant/prenom"),
                        sexe=menage.get("informations_du_menage/informations_de_l_occupant/genre"),
                        fk_type_personne=int(menage.get("informations_du_menage/informations_de_l_occupant/type_de_personne")) if menage.get("informations_du_menage/informations_de_l_occupant/type_de_personne") else None,
                        
                        lieu_naissance=menage.get("informations_du_menage/informations_de_l_occupant/lieu_de_naissance"),
                        date_naissance=menage.get("informations_du_menage/informations_de_l_occupant/date_de_naissance"),
                        fk_province=int(menage.get("informations_du_menage/informations_de_l_occupant/province_4")) if menage.get("informations_du_menage/informations_de_l_occupant/province_4") else None,
                        district=menage.get("informations_du_menage/informations_de_l_occupant/district_4"),
                        territoire=menage.get("informations_du_menage/informations_de_l_occupant/territoire_4"),
                        secteur=menage.get("informations_du_menage/informations_de_l_occupant/secteur_4"),
                        village=menage.get("informations_du_menage/informations_de_l_occupant/village_4"),
                        
                        fk_nationalite=int(menage.get("informations_du_menage/informations_de_l_occupant/nationalite_4")) if menage.get("informations_du_menage/informations_de_l_occupant/nationalite_4") else None,
                        profession=menage.get("informations_du_menage/informations_de_l_occupant/profession"),
                        
                        type_piece_identite=menage.get("informations_du_menage/informations_de_l_occupant/type_de_piece_d_identite_4"),
                        numero_piece_identite=menage.get("informations_du_menage/informations_de_l_occupant/numero_piece_d_identite_4"),
                        nom_du_pere=menage.get("informations_du_menage/informations_de_l_occupant/nom_du_pere_4"),
                        nom_de_la_mere=menage.get("informations_du_menage/informations_de_l_occupant/nom_de_la_mere_4"),
                        etat_civil=menage.get("informations_du_menage/informations_de_l_occupant/etat_civil_4"),
                        fk_lien_parente=int(menage.get("informations_du_menage/informations_de_l_occupant/lien_de_parente_4")) if menage.get("informations_du_menage/informations_de_l_occupant/lien_de_parente_4") else None,
                        
                        telephone=menage.get("informations_du_menage/informations_de_l_occupant/n_telephone"),
                        adresse_mail=menage.get("informations_du_menage/informations_de_l_occupant/adresse_email"),
                        
                        nombre_enfant=int(menage.get("informations_du_menage/informations_de_l_occupant/nombre_d_enfants")) if menage.get("informations_du_menage/informations_de_l_occupant/nombre_d_enfants") else None,
                        niveau_etude=menage.get("informations_du_menage/informations_de_l_occupant/niveau_d_etudes"),
                        
                        denomination=menage.get("informations_du_menage/informations_de_l_occupant/denomination"),
                        sigle=menage.get("informations_du_menage/informations_de_l_occupant/sigle"),
                        numero_impot=menage.get("informations_du_menage/informations_de_l_occupant/numero_d_impot"),
                        rccm=menage.get("informations_du_menage/informations_de_l_occupant/rccm"),
                        id_nat=menage.get("informations_du_menage/informations_de_l_occupant/id_nat"),
                        domaine_activite=menage.get("informations_du_menage/informations_de_l_occupant/domaine_d_activite"),
                        fk_adresse=fk_adresse,
                        fk_agent=fk_agent,
                    )
                    db.add(responsable)
                    db.flush()
                    fk_responsable = responsable.id

                    # 6. Insert into LocationBien (if the occupant is a locataire)
                    location_bien = LocationBien(
                        fk_personne=fk_responsable,
                        fk_bien=fk_bien,
                        fk_agent=fk_agent,
                    )
                    db.add(location_bien)
                    db.flush()
                    
                elif menage.get("informations_du_menage/informations_de_l_occupant/occupant_est_locataire_ou_proprietaire_2") == "proprietaire":
                    pass
                    
                for famille in kobo.get([]):
                    # 5. Insert into Personne (if the occupant is a locataire)
                    menage_bien = Menage(
                        fk_personne=fk_responsable,
                        fk_bien=fk_bien,
                    )
                    db.add(menage_bien)
                    db.flush()
                    fk_menage = menage_bien.id
                    
                    # 7. Insert additional Personnes
                    for personne in menage.get("informations_du_menage/information_sur_les_personnes", []):
                        
                        personne: dict = personne
                        
                        if personne.get("informations_du_menage/information_sur_les_personnes/nom_3") and personne.get("informations_du_menage/information_sur_les_personnes/prenom_3"):
                            
                            # Insert into Personne
                            new_personne = Personne(
                                nom=personne.get("informations_du_menage/information_sur_les_personnes/nom_3"),
                                postnom=personne.get("informations_du_menage/information_sur_les_personnes/post_nom_3"),
                                prenom=personne.get("informations_du_menage/information_sur_les_personnes/prenom_3"),
                                sexe=personne.get("informations_du_menage/information_sur_les_personnes/gr"),
                                lieu_naissance=personne.get("informations_du_menage/information_sur_les_personnes/lieu_de_naissance_1"),
                                date_naissance=personne.get("informations_du_menage/information_sur_les_personnes/date_de_naissance_1"),
                                fk_province=(int(personne.get("informations_du_menage/information_sur_les_personnes/province_d_origine")) if personne.get("informations_du_menage/information_sur_les_personnes/province_d_origine") else None),
                                district=personne.get("informations_du_menage/information_sur_les_personnes/district"),
                                territoire=personne.get("informations_du_menage/information_sur_les_personnes/territoire"),
                                secteur=personne.get("informations_du_menage/information_sur_les_personnes/secteur"),
                                village=personne.get("informations_du_menage/information_sur_les_personnes/village"),
                                profession=personne.get("informations_du_menage/information_sur_les_personnes/profession_2"),
                                type_piece_identite=personne.get("informations_du_menage/information_sur_les_personnes/type_de_piece_d_identite"),
                                numero_piece_identite=personne.get("informations_du_menage/information_sur_les_personnes/numero_piece_d_identite"),
                                nom_du_pere=personne.get("informations_du_menage/information_sur_les_personnes/nom_du_pere"),
                                nom_de_la_mere=personne.get("informations_du_menage/information_sur_les_personnes/nom_de_la_mere"),
                                etat_civil=personne.get("informations_du_menage/information_sur_les_personnes/etat_civil"),
                                fk_lien_parente=int(personne.get("informations_du_menage/information_sur_les_personnes/lien_de_parente")) if personne.get("informations_du_menage/information_sur_les_personnes/lien_de_parente") else None,
                                telephone=personne.get("informations_du_menage/information_sur_les_personnes/n_telphone"),
                                adresse_mail=personne.get("informations_du_menage/information_sur_les_personnes/adresse_email_3"),
                                nombre_enfant=int(personne.get("informations_du_menage/information_sur_les_personnes/nombre_d_enfants_001")) if personne.get("informations_du_menage/information_sur_les_personnes/nombre_d_enfants_001") else None,
                                niveau_etude=personne.get("informations_du_menage/information_sur_les_personnes/niveau_d_etudes_001"),
                                fk_nationalite=int(personne.get("informations_du_menage/information_sur_les_personnes/nationalite_3")) 
                                                if personne.get("informations_du_menage/information_sur_les_personnes/nationalite_3") else None,
                                fk_adresse=fk_adresse,
                                fk_agent=fk_agent,
                            )
                            db.add(new_personne)
                            db.flush()
                            fk_personne = new_personne.id
                            
                            # 8. Insert into MembreMenage
                            membre_menage = MembreMenage(
                                fk_menage=fk_menage,
                                fk_personne=fk_personne,
                                fk_filiation=int(personne.get("informations_du_menage/information_sur_les_personnes/lien_de_parente")) if personne.get("informations_du_menage/information_sur_les_personnes/lien_de_parente") else None,
                            )
                            db.add(membre_menage)

        else:
            # 1. Insert into Adresse
            adresse = Adresse(
                fk_avenue=int(kobo["Parcelle_non_accessible/avenue_1"]),  # Assuming this is an ID
                numero=kobo["Parcelle_non_accessible/numero_parcellaire_1"],
                fk_agent=fk_agent,
            )
            db.add(adresse)
            db.flush()  # Flush to get the inserted ID
            fk_adresse = adresse.id
            
            # 2. Insert into Parcelle
            parcelle = Parcelle(
                numero_parcellaire=kobo.get("Parcelle_non_accessible/numero_parcellaire_1"),
                coordonnee_geographique=kobo.get("Parcelle_non_accessible/coordonne_geographique_1"),
                fk_rang=int(kobo.get("Parcelle_non_accessible/rang_1")) if kobo.get("Parcelle_non_accessible/rang_1") else None,
                fk_proprietaire=fk_proprietaire,
                fk_adresse=fk_adresse,
                fk_agent=fk_agent,
                statut=2
            )
            db.add(parcelle)
            db.flush()
            fk_parcelle = parcelle.id
            
    
        # 8. Insert into Logs
        log = Logs(
            logs="Processed Kobo data",
            id_kobo=record_id,
            data_json=str(payload)
        )
        db.add(log)

        # Commit the transaction
        db.commit()
        logger.info(f"Données insérées pour l'entrée _id={record_id}")
        return {"status": "success", "message": f"Données insérées pour l'entrée _id={record_id}"}

    except Exception as e:
        db.rollback()
        logger.error(f"Erreur lors de l'insertion des données _id={record_id} : {str(e)}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Erreur lors de l'insertion des données : {str(e)}")


def process_rapport_superviseur_form(payload: dict, db: Session):
    kobo: dict = payload
    record_id = kobo.get("id", kobo.get("_id"))
    
    id_kobo = f"rapports_superviseurs_{record_id}"
    
    logger.info(f"Données kobo : {kobo}")

    try:
        existing_agent = db.query(Utilisateur).filter(Utilisateur.id_kobo == kobo["_submitted_by"]).first()
        if existing_agent:
            fk_agent = existing_agent.id
        else:
            fk_agent = 1  # Default agent ID if not found
        
        # Check if the ID already exists in the logs table
        # existing_log = db.query(Logs).filter(Logs.id_kobo == id_kobo).first()
        
        # if existing_log:
        #     raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Le formulaire avec ID {record_id} déjà existante.")

        rapport_recensement = RapportRecensement(
            heure_debut=kobo.get('group_bd9mw82/Heure_de_d_but'),
            heure_fin=kobo.get('group_bd9mw82/Heure_de_fin'),
            fk_agent=fk_agent,
            effectif_present=kobo.get('group_di3ui02/Effectif_pr_sent'),
            effectif_absent=kobo.get('group_di3ui02/Effectif_absent'),
            observation=kobo.get('group_di3ui02/Remarques_sur_l_quipe'),
            tache_effectue=kobo.get(''),
            nombre_parcelles_accessibles=kobo.get('group_dk3nu62/nombre_parcelles_accessibles'),
            nombre_parcelles_non_accessibles=kobo.get('group_dk3nu62/nombre_parcelles_nonaccessible'),
            incident_description=kobo.get('group_gt4dp59/Description_de_l_incident'),
            incident_heure=kobo.get('group_gt4dp59/Heure_de_l_incident'),
            incident_recommandations=kobo.get('group_gt4dp59/Suggestions_Recommandations'),
            incident_actions_correctives=kobo.get('group_gt4dp59/Actions_correctives_prises'),
            incident_personnes_impliquees=kobo.get('group_gt4dp59/Personnes_impliqu_es'),
        )
        db.add(rapport_recensement)
        
        # 8. Insert into Logs
        log = Logs(
            logs="Processed Kobo data",
            id_kobo=id_kobo,
            data_json=str(payload)
        )
        db.add(log)

        # Commit the transaction
        db.commit()
        
        logger.info(f"Données insérées pour l'entrée _id={record_id}")
        return {"status": "success", "message": f"Données insérées pour l'entrée _id={record_id}"}
        
    except Exception as e:
        db.rollback()
        logger.error(f"Erreur lors de l'insertion des données _id={record_id} : {str(e)}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Erreur lors de l'insertion des données : {str(e)}")

