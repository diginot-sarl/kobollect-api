# app/service.py
import pendulum
import pytz
from sqlalchemy.orm import Session
from fastapi import BackgroundTasks, HTTPException, status
import logging
from app.models import (
    Adresse, Personne, Parcelle, Bien, LocationBien, Utilisateur, Logs, Menage, MembreMenage, RapportRecensement)
from app.utils import generate_nif, safe_int
from datetime import datetime

logger = logging.getLogger(__name__)

def process_recensement_form(payload: dict, db: Session, background_tasks: BackgroundTasks):
    kobo: dict = payload
    record_id = kobo.get("id", kobo.get("_id"))
    
    logger.info(f"Données kobo : {kobo}")
    
    # Parse _submission_time, assuming it's in UTC
    date_create_str: str = kobo.get("_submission_time")
    try:
        # Parse as UTC and keep timezone-aware for DATETIMEOFFSET
        date_create = datetime.fromisoformat(date_create_str.replace("Z", "+00:00")).replace(tzinfo=pytz.UTC)
    except (ValueError, TypeError):
        logger.error(f"Invalid date format for _submission_time: {date_create_str}")
        date_create = datetime.now(pytz.UTC)  # Fallback to current UTC time

    try:
        existing_agent = db.query(Utilisateur).filter(Utilisateur.login == kobo["_submitted_by"]).first()
        if existing_agent:
            fk_agent = existing_agent.id
        else:
            fk_agent = 1  # Default agent ID if not found
        
        # Check if the ID already exists in the logs table
        # existing_log = db.query(Logs).filter(Logs.id_kobo == record_id).first()
        # if existing_log:
        #     raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Le formulaire avec ID {record_id} déjà existante.")

        # Initialize variables
        fk_proprietaire = None
        
        coordonnee_geographique = None
        superficie_parcelle_egale_bien = False
        if kobo.get("adresse_de_la_parcelle/La_maison_occupe_t_elle_toute_") == "oui" or kobo.get("adresse_de_la_parcelle/coordonne_geographique") is None:
            first_bien: dict = kobo.get("informations_du_menage", [])[0]
            superficie_parcelle_egale_bien = True
            coordonnee_geographique = (first_bien.get("informations_du_menage/informations_du_bien/coordonnee_geographique") if first_bien.get("informations_du_menage/informations_du_bien/coordonnee_geographique") else None)
        
        
        if kobo.get("parcelle_accessible_ou_non") == "oui":

            # 1. Insert into Adresse
            adresse = Adresse(
                fk_avenue=safe_int(kobo.get("adresse_de_la_parcelle/avenue")),  # Assuming this is an ID
                numero=kobo.get("adresse_de_la_parcelle/numero_parcellaire"),
                fk_agent=fk_agent,
                date_create=date_create,
            )
            db.add(adresse)
            db.flush()  # Flush to get the inserted ID
            fk_adresse = adresse.id

            # 2. Insert Propriétaire into Personne
            
            nif = generate_nif()
            
            proprietaire = Personne(
                
                nif=nif,
                
                nom=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nom_2"),
                
                postnom=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/post_nom_2"),
                
                prenom=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/prenom_2"),
                
                sexe=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/genre_2"),
                
                lieu_naissance=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Lieu_de_naissance_001"),
                date_naissance=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Date_de_naissance_001"),
                
                fk_type_personne=safe_int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/tp")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/tp") else None,
                
                fk_nationalite=safe_int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nationalite_5")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nationalite_5") else None,
                
                fk_province=safe_int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/province_d_origine_5")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/province_d_origine_5") else None,
                
                district=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/district_5"),
                territoire=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/territoire_5"),
                secteur=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/secteur_5"),
                village=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/village_5"),
                profession=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/profession_5"),
                type_piece_identite=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/type_de_piece_d_identite_5"),
                numero_piece_identite=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/numero_piece_d_identite_5"),
                nom_du_pere=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nom_du_pere_5"),
                nom_de_la_mere=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nom_de_la_mere_5"),
                nombre_enfant=safe_int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Nombre_d_enfant")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Nombre_d_enfant") else None,
                etat_civil=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/etat_civil_5"),
                
                fk_lien_parente=safe_int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/lien_de_parente_5")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/lien_de_parente_5") else None,
                
                niveau_etude=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/niveau_d_etudes_5"),
                
                telephone=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/n_telephone_2"),
                
                adresse_mail=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/adresse_email_2"),
                
                denomination=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/denomination_2"),
                
                fk_forme_juridique=(safe_int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/forme_juridique_2")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/forme_juridique_2") else None),
                
                sigle=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/sigle_2"),
                
                numero_impot=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/numero_d_impot_2"),
                
                rccm=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/rccm_2"),
                
                id_nat=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/id_nat_2"),
                
                domaine_activite=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/domaine_d_activite_2"),
                
                etranger=(1 if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/est_il_etranger_5") is not None and kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/est_il_etranger_5") == "oui" else None),
                
                fk_adresse=fk_adresse,
                fk_agent=fk_agent,
                
                date_create=date_create,
            )
            db.add(proprietaire)
            db.flush()
            fk_proprietaire = proprietaire.id

            # 3. Insert into Parcelle
            parcelle = Parcelle(
                numero_parcellaire=kobo.get("adresse_de_la_parcelle/numero_parcellaire"),
                fk_unite=safe_int(kobo.get("adresse_de_la_parcelle/unite_de_la_superficie")) if kobo.get("adresse_de_la_parcelle/unite_de_la_superficie") else None,
                longueur=float(kobo.get("adresse_de_la_parcelle/longueur")) if kobo.get("adresse_de_la_parcelle/longueur") else None,
                largeur=float(kobo.get("adresse_de_la_parcelle/largeur")) if kobo.get("adresse_de_la_parcelle/largeur") else None,
                superficie_calculee=float(kobo.get("adresse_de_la_parcelle/calculation")) if kobo.get("adresse_de_la_parcelle/calculation") else None,
                coordonnee_geographique=coordonnee_geographique if superficie_parcelle_egale_bien else kobo.get("adresse_de_la_parcelle/coordonne_geographique"),
                fk_rang=safe_int(kobo.get("adresse_de_la_parcelle/rang")) if kobo.get("adresse_de_la_parcelle/rang") else None,
                fk_proprietaire=fk_proprietaire,
                fk_adresse=fk_adresse,
                fk_agent=fk_agent,
                date_create=date_create,
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
                    
                    fk_nature_bien=(safe_int(menage.get("informations_du_menage/informations_du_bien/nature")) 
                                    if menage.get("informations_du_menage/informations_du_bien/nature") else None),
                    
                    fk_unite=(safe_int(menage.get("informations_du_menage/informations_du_bien/unite_de_la_superficie_1")) 
                              if menage.get("informations_du_menage/informations_du_bien/unite_de_la_superficie_1") else None),
                    
                    fk_usage=(safe_int(menage.get("informations_du_menage/informations_du_bien/usage")) 
                              if menage.get("informations_du_menage/informations_du_bien/usage") else None),
                    
                    fk_usage_specifique=(safe_int(menage.get("informations_du_menage/informations_du_bien/usage_specifique")) 
                                         if menage.get("informations_du_menage/informations_du_bien/usage_specifique") else None),
                    
                    nombre_etage=(safe_int(kobo.get("informations_du_menage/informations_du_bien/nombre_d_etages")) if kobo.get("informations_du_menage/informations_du_bien/nombre_d_etages") else None),
                    
                    fk_agent=fk_agent,
                    
                    numero_bien=menage.get("informations_du_menage/informations_du_bien/numero_bien"),
                    
                    date_create=date_create,
                )
                db.add(bien)
                db.flush()
                fk_bien = bien.id
                
                for group_menage in menage.get("informations_du_menage/group_ex5mk47", []):
                    group_menage: dict = group_menage
                
                    # 4. Insert Locataire into Personne (if the occupant is a locataire)
                    if group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/occupant_est_locataire_ou_proprietaire_2") != "bailleur":
                        
                        nif = generate_nif()
                        
                        responsable = Personne(
                            
                            nif=nif,
                            
                            nom=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/nom"),##
                            
                            postnom=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/post_nom"),##
                            
                            prenom=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/prenom"),##
                            
                            sexe=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/genre"),##
                            
                            fk_type_personne=(safe_int(group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/type_de_personne")) if group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/type_de_personne") else None),##
                            
                            lieu_naissance=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/lieu_de_naissance"),
                            
                            date_naissance=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/date_de_naissance"),
                            
                            fk_province=safe_int(group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/province_4")) if 
                            group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/province_4") else None,
                            
                            district=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/district_4"),
                            
                            territoire=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/territoire_4"),
                            
                            secteur=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/secteur_4"),
                            
                            village=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/village_4"),
                            
                            fk_nationalite=safe_int(group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/nationalite_4")) if group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/nationalite_4") else None,
                            
                            profession=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/profession"),
                            
                            type_piece_identite=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/type_de_piece_d_identite_4"),
                            
                            numero_piece_identite=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/numero_piece_d_identite_4"),
                            
                            nom_du_pere=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/nom_du_pere_4"),
                            
                            nom_de_la_mere=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/nom_de_la_mere_4"),
                            
                            etat_civil=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/etat_civil_4"),
                            
                            fk_lien_parente=safe_int(group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/lien_de_parente_4")) if group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/lien_de_parente_4") else None,
                            
                            telephone=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/n_telephone"),
                            
                            adresse_mail=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/adresse_email"),
                            
                            nombre_enfant=safe_int(group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/nombre_d_enfants")) if group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/nombre_d_enfants") else None,
                            
                            niveau_etude=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/niveau_d_etudes"),
                            
                            denomination=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/denomination"),
                            
                            sigle=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/sigle"),
                            
                            numero_impot=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/numero_d_impot"),
                            
                            rccm=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/rccm"),
                            
                            id_nat=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/id_nat"),
                            
                            domaine_activite=group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/domaine_d_activite"),
                            
                            etranger=(1 if kobo.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/est_il_etranger_4") is not None and kobo.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/est_il_etranger_4") == "oui" else None),
                            
                            fk_adresse=fk_adresse,
                            fk_agent=fk_agent,
                            
                            date_create=date_create,
                        )
                        db.add(responsable)
                        db.flush()
                        fk_responsable = responsable.id

                        # 6. Insert into LocationBien (if the occupant is a locataire)
                        if group_menage.get("informations_du_menage/group_ex5mk47/informations_de_l_occupant/occupant_est_locataire_ou_proprietaire_2") == "locataire":
                            location_bien = LocationBien(
                                fk_personne=fk_responsable,
                                fk_bien=fk_bien,
                                fk_agent=fk_agent,
                            )
                            db.add(location_bien)
                        
                    # 5. Insert into Personne (if the occupant is a locataire)
                    menage_bien = Menage(
                        fk_personne=fk_responsable,
                        fk_bien=fk_bien,
                        fk_agent=fk_agent,
                        date_create=date_create,
                    )
                    db.add(menage_bien)
                    db.flush()
                    fk_menage = menage_bien.id
                    
                    # 7. Insert additional Personnes
                    for personne in group_menage.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes", []):
                        personne: dict = personne
                            
                        # Insert into Personne
                        new_personne = Personne(
                            nom=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/nom_3"),
                            postnom=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/post_nom_3"),
                            prenom=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/prenom_3"),
                            sexe=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/gr"),
                            lieu_naissance=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/lieu_de_naissance_1"),
                            date_naissance=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/date_de_naissance_1"),
                            fk_province=(safe_int(personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/province_d_origine")) if personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/province_d_origine") else None),
                            district=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/district"),
                            territoire=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/territoire"),
                            secteur=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/secteur"),
                            village=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/village"),
                            profession=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/profession_2"),
                            type_piece_identite=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/type_de_piece_d_identite"),
                            numero_piece_identite=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/numero_piece_d_identite"),
                            nom_du_pere=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/nom_du_pere"),
                            nom_de_la_mere=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/nom_de_la_mere"),
                            etat_civil=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/etat_civil"),
                            fk_lien_parente=safe_int(personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/lien_de_parente")) if personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/lien_de_parente") else None,
                            telephone=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/n_telphone"),
                            adresse_mail=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/adresse_email_3"),
                            nombre_enfant=safe_int(personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/nombre_d_enfants_001")) if personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/nombre_d_enfants_001") else None,
                            niveau_etude=personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/niveau_d_etudes_001"),
                            fk_nationalite=safe_int(personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/nationalite_3")) 
                                            if personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/nationalite_3") else None,
                            fk_adresse=fk_adresse,
                            fk_agent=fk_agent,
                            date_create=date_create,
                            etranger=(1 if personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/est_il_etranger") is not None and personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/est_il_etranger") == "oui" else None)
                        )
                        db.add(new_personne)
                        db.flush()
                        fk_personne = new_personne.id
                        
                        # 8. Insert into MembreMenage
                        membre_menage = MembreMenage(
                            fk_menage=fk_menage,
                            fk_personne=fk_personne,
                            fk_filiation=safe_int(personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/lien_de_parente")) if personne.get("informations_du_menage/group_ex5mk47/information_sur_les_personnes/lien_de_parente") else None,
                            fk_agent=fk_agent,
                            date_create=date_create,
                        )
                        db.add(membre_menage)

        else:
            # 1. Insert into Adresse
            adresse = Adresse(
                fk_avenue=safe_int(kobo.get("Parcelle_non_accessible/avenue_1")),  # Assuming this is an ID
                numero=kobo.get("Parcelle_non_accessible/numero_parcellaire_1"),
                fk_agent=fk_agent,
                date_create=date_create,
            )
            db.add(adresse)
            db.flush()  # Flush to get the inserted ID
            fk_adresse = adresse.id
            
            # 2. Insert into Parcelle
            parcelle = Parcelle(
                numero_parcellaire=kobo.get("Parcelle_non_accessible/numero_parcellaire_1"),
                coordonnee_geographique=kobo.get("Parcelle_non_accessible/coordonne_geographique_1"),
                fk_rang=safe_int(kobo.get("Parcelle_non_accessible/rang_1")) if kobo.get("Parcelle_non_accessible/rang_1") else None,
                fk_proprietaire=fk_proprietaire,
                fk_adresse=fk_adresse,
                fk_agent=fk_agent,
                statut=2,
                date_create=date_create,
            )
            db.add(parcelle)
            db.flush()
            fk_parcelle = parcelle.id
            
    
        # 8. Insert into Logs
        log = Logs(
            logs="process_recensement_form",
            id_kobo=record_id,
            data_json=str(payload),
            fk_agent=fk_agent,
            date_submission=date_create,
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
    
    # Parse _submission_time, assuming it's in UTC
    date_create_str: str = kobo.get("_submission_time")
    try:
        # Parse as UTC and keep timezone-aware for DATETIMEOFFSET
        date_create = datetime.fromisoformat(date_create_str.replace("Z", "+00:00")).replace(tzinfo=pytz.UTC)
    except (ValueError, TypeError):
        logger.error(f"Invalid date format for _submission_time: {date_create_str}")
        date_create = datetime.now(pytz.UTC)  # Fallback to current UTC time

    try:
        existing_agent = db.query(Utilisateur).filter(Utilisateur.login == kobo["_submitted_by"]).first()
        if existing_agent:
            fk_agent = existing_agent.id
        else:
            fk_agent = 1  # Default agent ID if not found
        
        # Extract just the time part (HH:MM:SS) from the incident_heure
        incident_heure = kobo.get('group_gt4dp59/Heure_de_l_incident')
        if incident_heure:
            incident_heure = incident_heure.split('T')[-1].split('.')[0]  # Get just the time part
            incident_heure = incident_heure[:8]  # Truncate to HH:MM:SS format
        
        rapport_recensement = RapportRecensement(
            heure_debut=str(kobo.get('group_bd9mw82/Heure_de_d_but')) if kobo.get('group_bd9mw82/Heure_de_d_but') else None,
            heure_fin=str(kobo.get('group_bd9mw82/Heure_de_fin')) if kobo.get('group_bd9mw82/Heure_de_fin') else None,
            fk_agent=fk_agent,
            effectif_present=safe_int(kobo.get('group_di3ui02/Effectif_pr_sent')) if kobo.get('group_di3ui02/Effectif_pr_sent') else None,
            effectif_absent=safe_int(kobo.get('group_di3ui02/Effectif_absent')) if kobo.get('group_di3ui02/Effectif_absent') else None,
            observation=kobo.get('group_di3ui02/Remarques_sur_l_quipe'),
            tache_effectue=kobo.get('group_dk3nu62/R_sum_des_t_ches_effectu_es'),
            nombre_parcelles_accessibles=safe_int(kobo.get('group_dk3nu62/nombre_parcelles_accessibles')) if kobo.get('group_dk3nu62/nombre_parcelles_accessibles') else None,
            nombre_parcelles_non_accessibles=(safe_int(kobo.get('group_dk3nu62/nombre_parcelles_nonaccessible')) if kobo.get('group_dk3nu62/nombre_parcelles_nonaccessible') else None),
            incident_description=kobo.get('group_gt4dp59/Description_de_l_incident'),
            incident_heure=incident_heure,  # Use the truncated time
            incident_recommandations=kobo.get('group_gt4dp59/Suggestions_Recommandations'),
            incident_actions_correctives=kobo.get('group_gt4dp59/Actions_correctives_prises'),
            incident_personnes_impliquees=kobo.get('group_gt4dp59/Personnes_impliqu_es'),
            date=str(kobo.get('group_bd9mw82/Date')) if kobo.get('group_bd9mw82/Date') else None,
            objectif_atteint=(1 if kobo.get('group_dk3nu62/Objectifs_journaliers_atteints') is not None and kobo.get('group_dk3nu62/Objectifs_journaliers_atteints') == "oui" else 0)
        )
        db.add(rapport_recensement)
        
        # 8. Insert into Logs
        log = Logs(
            logs="process_rapport_superviseur_form",
            id_kobo=record_id,
            data_json=str(payload),
            fk_agent=fk_agent,
            date_submission=date_create,
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


def process_parcelles_non_baties_form(payload: dict, db: Session, background_tasks: BackgroundTasks):
    kobo: dict = payload
    record_id = kobo.get("id", kobo.get("_id"))
    
    # Parse _submission_time, assuming it's in UTC
    date_create_str: str = kobo.get("_submission_time")
    try:
        # Parse as UTC and keep timezone-aware for DATETIMEOFFSET
        date_create = datetime.fromisoformat(date_create_str.replace("Z", "+00:00")).replace(tzinfo=pytz.UTC)
    except (ValueError, TypeError):
        logger.error(f"Invalid date format for _submission_time: {date_create_str}")
        date_create = datetime.now(pytz.UTC)  # Fallback to current UTC time

    try:
        existing_agent = db.query(Utilisateur).filter(Utilisateur.login == kobo["_submitted_by"]).first()
        if existing_agent:
            fk_agent = existing_agent.id
        else:
            fk_agent = 1  # Default agent ID if not found

        # Initialize variables
        nif = generate_nif()
        
        coordonnee_geographique = None
        superficie_parcelle_egale_bien = False
        
        if kobo.get("adresse_de_la_parcelle/La_maison_occupe_t_elle_toute_") == "oui":
            superficie_parcelle_egale_bien = True
            coordonnee_geographique = kobo.get("informations_du_menage/coordonnee_geographique")

        # 1. Insert into Adresse
        adresse = Adresse(
            fk_avenue=safe_int(kobo.get("adresse_de_la_parcelle/avenue")),  # Assuming this is an ID
            numero=kobo.get("adresse_de_la_parcelle/numero_parcellaire"),
            fk_agent=fk_agent,
            date_create=date_create,
        )
        db.add(adresse)
        db.flush()  # Flush to get the inserted ID
        fk_adresse = adresse.id

        # 2. Insert Propriétaire into Personne
        
        proprietaire = Personne(
            
            nif=nif,
            
            nom=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nom_2"),
            
            postnom=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/post_nom_2"),
            
            prenom=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/prenom_2"),
            
            sexe=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/genre_2"),
            
            lieu_naissance=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Lieu_de_naissance_001"),
            
            date_naissance=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Date_de_naissance_001"),
            
            fk_type_personne=(safe_int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/tp")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/tp") else None),
            
            fk_nationalite=(safe_int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nationalite_5")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nationalite_5") else None),
            
            fk_province=(safe_int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/province_d_origine_5")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/province_d_origine_5") else None),
            
            district=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/district_5"),
            
            territoire=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/territoire_5"),
            
            secteur=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/secteur_5"),
            
            village=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/village_5"),
            
            profession=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/profession_5"),
            
            type_piece_identite=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/type_de_piece_d_identite_5"),
            
            numero_piece_identite=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/numero_piece_d_identite_5"),
            
            nom_du_pere=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nom_du_pere_5"),
            
            nom_de_la_mere=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nom_de_la_mere_5"),
            
            nombre_enfant=(safe_int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Nombre_d_enfant")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Nombre_d_enfant") else None),
            
            etat_civil=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/etat_civil_5"),
            
            fk_lien_parente=(safe_int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/lien_de_parente_5")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/lien_de_parente_5") else None),
            
            niveau_etude=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/niveau_d_etudes_5"),
            
            telephone=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/n_telephone_2"),
            
            adresse_mail=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/adresse_email_2"),
            
            denomination=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/denomination_2"),
            
            fk_forme_juridique=(safe_int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/forme_juridique_2")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/forme_juridique_2") else None),
            
            sigle=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/sigle_2"),
            
            numero_impot=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/numero_d_impot_2"),
            
            rccm=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/rccm_2"),
            
            id_nat=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/id_nat_2"),
            
            domaine_activite=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/domaine_d_activite_2"),
            
            etranger=(1 if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/est_il_etranger_5") is not None and kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/est_il_etranger_5") == "oui" else None),
            
            fk_adresse=fk_adresse,
            fk_agent=fk_agent,
            
            date_create=date_create,
        )
        db.add(proprietaire)
        db.flush()
        fk_proprietaire = proprietaire.id

        # 3. Insert into Parcelle
        parcelle = Parcelle(
            
            numero_parcellaire=kobo.get("adresse_de_la_parcelle/numero_parcellaire"),
            
            fk_unite=safe_int(kobo.get("adresse_de_la_parcelle/unite_de_la_superficie")) if kobo.get("adresse_de_la_parcelle/unite_de_la_superficie") else None,
            
            longueur=float(kobo.get("adresse_de_la_parcelle/longueur")) if kobo.get("adresse_de_la_parcelle/longueur") else None,
            
            largeur=float(kobo.get("adresse_de_la_parcelle/largeur")) if kobo.get("adresse_de_la_parcelle/largeur") else None,
            
            superficie_calculee=float(kobo.get("adresse_de_la_parcelle/calculation")) if kobo.get("adresse_de_la_parcelle/calculation") else None,
            
            coordonnee_geographique=coordonnee_geographique if superficie_parcelle_egale_bien else kobo.get("adresse_de_la_parcelle/coordonne_geographique"),
            
            fk_rang=safe_int(kobo.get("adresse_de_la_parcelle/rang")) if kobo.get("adresse_de_la_parcelle/rang") else None,
            
            fk_proprietaire=fk_proprietaire,
            
            fk_adresse=fk_adresse,
            
            fk_agent=fk_agent,
            
            date_create=date_create,
        )
        db.add(parcelle)
        db.flush()
        fk_parcelle = parcelle.id
        
        
        bien = Bien(                    
            coordinates=kobo.get("informations_du_menage/informations_du_bien/coordonnee_geographique"),
            
            superficie=kobo.get("informations_du_menage/informations_du_bien/superficie"),
            
            fk_parcelle=fk_parcelle,
            
            fk_nature_bien=safe_int(kobo.get("informations_du_menage/informations_du_bien/nature")) 
                            if kobo.get("informations_du_menage/informations_du_bien/nature") else None,
            
            fk_unite=safe_int(kobo.get("informations_du_menage/informations_du_bien/unite_de_la_superficie_1")) 
                        if kobo.get("informations_du_menage/informations_du_bien/unite_de_la_superficie_1") else None,
            
            fk_usage=safe_int(kobo.get("informations_du_menage/informations_du_bien/usage")) 
                        if kobo.get("informations_du_menage/informations_du_bien/usage") else None,
            
            fk_usage_specifique=safe_int(kobo.get("informations_du_menage/informations_du_bien/usage_specifique")) 
                                    if kobo.get("informations_du_menage/informations_du_bien/usage_specifique") else None,
            fk_agent=fk_agent,
            
            numero_bien=kobo.get("informations_du_menage/informations_du_bien/numero_bien"),
            
            date_create=date_create,
        )
        db.add(bien)
    
        # 8. Insert into Logs
        log = Logs(
            logs="process_parcelles_non_baties_form",
            id_kobo=record_id,
            data_json=str(payload),
            fk_agent=fk_agent,
            date_submission=date_create,
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


def process_immeuble_form(payload: dict, db: Session, background_tasks: BackgroundTasks):
    kobo: dict = payload
    record_id = kobo.get("id", kobo.get("_id"))
    
    # Parse _submission_time, assuming it's in UTC
    date_create_str: str = kobo.get("_submission_time")
    try:
        # Parse as UTC and keep timezone-aware for DATETIMEOFFSET
        date_create = datetime.fromisoformat(date_create_str.replace("Z", "+00:00")).replace(tzinfo=pytz.UTC)
    except (ValueError, TypeError):
        logger.error(f"Invalid date format for _submission_time: {date_create_str}")
        date_create = datetime.now(pytz.UTC)  # Fallback to current UTC time

    try:
        existing_agent = db.query(Utilisateur).filter(Utilisateur.login == kobo["_submitted_by"]).first()
        if existing_agent:
            fk_agent = existing_agent.id
        else:
            fk_agent = 1  # Default agent ID if not found
        
        # Check if the ID already exists in the logs table
        # existing_log = db.query(Logs).filter(Logs.id_kobo == record_id).first()
        # if existing_log:
        #     raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Le formulaire avec ID {record_id} déjà existante.")

        # Initialize variables
        fk_proprietaire = None
        fk_parcelle = None
        
        coordonnee_geographique = None
        superficie_parcelle_egale_bien = False
        if kobo.get("informations_immeuble/adresse_de_la_parcelle/La_maison_occupe_t_elle_toute_") == "oui":
            superficie_parcelle_egale_bien = True
            coordonnee_geographique = kobo.get("informations_immeuble/informations_du_bien/coordonnee_geographique")
        
        if kobo.get("parcelle_accessible_ou_non") == "oui":

            # 1. Insert into Adresse
            adresse = Adresse(
                fk_avenue=safe_int(kobo.get("informations_immeuble/adresse_de_la_parcelle/avenue")),  # Assuming this is an ID
                numero=kobo.get("informations_immeuble/adresse_de_la_parcelle/numero_parcellaire"),
                fk_agent=fk_agent,
                date_create=date_create,
            )
            db.add(adresse)
            db.flush()  # Flush to get the inserted ID
            fk_adresse = adresse.id
            
            if kobo.get("informations_immeuble/Il_y_a_t_il_un_propri_taire_un") == "oui":
                
                nif = generate_nif()
                
                proprietaire = Personne(
                    
                    date_create=date_create,
                    
                    nif=nif,
                    
                    nom=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nom_2"),
                    
                    postnom=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/post_nom_2"),
                    
                    prenom=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/prenom_2"),
                    
                    sexe=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/genre_2"),
                    
                    lieu_naissance=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Lieu_de_naissance_001"),
                    date_naissance=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Date_de_naissance_001"),
                    
                    fk_type_personne=safe_int(kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/tp")) if kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/tp") else None,
                    
                    fk_nationalite=safe_int(kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nationalite_5")) if kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nationalite_5") else None,
                    
                    fk_province=safe_int(kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/province_d_origine_5")) if kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/province_d_origine_5") else None,
                    
                    district=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/district_5"),
                    
                    territoire=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/territoire_5"),
                    
                    secteur=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/secteur_5"),
                    
                    village=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/village_5"),
                    
                    profession=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/profession_5"),
                    
                    type_piece_identite=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/type_de_piece_d_identite_5"),
                    
                    numero_piece_identite=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/numero_piece_d_identite_5"),
                    
                    nom_du_pere=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nom_du_pere_5"),
                    
                    nom_de_la_mere=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nom_de_la_mere_5"),
                    
                    nombre_enfant=safe_int(kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Nombre_d_enfant")) if kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/Nombre_d_enfant") else None,
                    
                    etat_civil=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/etat_civil_5"),
                    
                    fk_lien_parente=safe_int(kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/lien_de_parente_5")) if kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/lien_de_parente_5") else None,
                    
                    niveau_etude=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/niveau_d_etudes_5"),
                    
                    telephone=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/n_telephone_2"),
                    
                    adresse_mail=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/adresse_email_2"),
                    
                    denomination=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/denomination_2"),
                    
                    sigle=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/sigle_2"),
                    
                    numero_impot=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/numero_d_impot_2"),
                    
                    rccm=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/rccm_2"),
                    
                    id_nat=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/id_nat_2"),
                    
                    domaine_activite=kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/domaine_d_activite_2"),
                    
                    etranger=(1 if kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/est_il_etranger_5") is not None and kobo.get("informations_immeuble/informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/est_il_etranger_5") == "oui" else None),
                    
                    fk_adresse=fk_adresse,
                    fk_agent=fk_agent,
                )
                
                db.add(proprietaire)
                db.flush()
                
                fk_proprietaire = proprietaire.id
                
            parcelle = Parcelle(
                numero_parcellaire=kobo.get("informations_immeuble/adresse_de_la_parcelle/numero_parcellaire"),
                fk_unite=safe_int(kobo.get("informations_immeuble/adresse_de_la_parcelle/unite_de_la_superficie")) if kobo.get("informations_immeuble/adresse_de_la_parcelle/unite_de_la_superficie") else None,
                
                longueur=float(kobo.get("informations_immeuble/adresse_de_la_parcelle/longueur")) if kobo.get("informations_immeuble/adresse_de_la_parcelle/longueur") else None,
                
                largeur=float(kobo.get("informations_immeuble/adresse_de_la_parcelle/largeur")) if kobo.get("informations_immeuble/adresse_de_la_parcelle/largeur") else None,
                
                superficie_calculee=float(kobo.get("informations_immeuble/adresse_de_la_parcelle/calculation")) if kobo.get("informations_immeuble/adresse_de_la_parcelle/calculation") else None,
                
                coordonnee_geographique=coordonnee_geographique if superficie_parcelle_egale_bien else kobo.get("informations_immeuble/adresse_de_la_parcelle/coordonne_geographique"),
                
                fk_rang=safe_int(kobo.get("informations_immeuble/adresse_de_la_parcelle/rang")) if kobo.get("informations_immeuble/adresse_de_la_parcelle/rang") else None,
                
                nombre_etage=(safe_int(kobo.get("informations_immeuble/adresse_de_la_parcelle/nombre_d_etages")) if kobo.get("informations_immeuble/adresse_de_la_parcelle/nombre_d_etages") else None),
                
                fk_proprietaire=fk_proprietaire,
                fk_adresse=fk_adresse,
                fk_agent=fk_agent,
                
                date_create=date_create,
            )
            db.add(parcelle)
            db.flush()
            fk_parcelle = parcelle.id

            # Insert into Bien (Immeuble)
            immeuble = Bien(
                
                date_create=date_create,
                
                numero_bien=kobo.get("informations_immeuble/informations_du_bien/numero_bien"),
                
                coordinates=kobo.get("informations_immeuble/informations_du_bien/coordonnee_geographique"),
                
                fk_nature_bien=8,
                
                fk_parcelle=fk_parcelle,
                
                fk_agent=fk_agent,
                
                nombre_etage=(safe_int(kobo.get("informations_immeuble/adresse_de_la_parcelle/nombre_d_etages")) if kobo.get("informations_immeuble/adresse_de_la_parcelle/nombre_d_etages") else None),
            )
            db.add(immeuble)
            db.flush()
            fk_immeuble = immeuble.id
            
            for appartment in kobo.get("informations_immeuble/group_no51r46",[]):
                appartment: dict = appartment
                
                fk_proprietaire_appart = None
                
                nif = generate_nif()
                
                proprietaire_appart = Personne(
                
                    date_create=date_create,
                    
                    nif=nif,
                    
                    nom=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/nom_20"),
                    
                    postnom=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/post_nom_20"),
                    
                    prenom=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/prenom_20"),
                    
                    sexe=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/genre_20"),
                    
                    lieu_naissance=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/Lieu_de_naissance_0010"),
                    
                    date_naissance=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/Date_de_naissance_0010"),
                    
                    fk_type_personne=safe_int(appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/tp_001")) if appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/tp_001") else None,
                    
                    fk_nationalite=safe_int(appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/nationalite_50")) if appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/nationalite_50") else None,
                    
                    fk_province=safe_int(appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/province_d_origine_50")) if appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/province_d_origine_50") else None,
                    
                    district=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/district_50"),
                    
                    territoire=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/territoire_50"),
                    
                    secteur=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/secteur_50"),
                    
                    village=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/village_50"),
                    
                    profession=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/profession_50"),
                    
                    type_piece_identite=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/type_de_piece_d_identite_50"),
                    
                    numero_piece_identite=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/numero_piece_d_identite_50"),
                    
                    nom_du_pere=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/nom_du_pere_50"),
                    
                    nom_de_la_mere=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/nom_de_la_mere_50"),
                    
                    nombre_enfant=safe_int(appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/Nombre_d_enfant_001")) if appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/Nombre_d_enfant_001") else None,
                    
                    etat_civil=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/etat_civil_50"),
                    
                    fk_lien_parente=safe_int(appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/lien_de_parente_50")) if appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/lien_de_parente_50") else None,
                    
                    niveau_etude=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/niveau_d_etudes_50"),
                    
                    telephone=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/n_telephone_20"),
                    
                    adresse_mail=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/adresse_email_20"),
                    
                    denomination=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/denomination_20"),
                    
                    sigle=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/sigle_20"),
                    
                    numero_impot=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/numero_d_impot_20"),
                    
                    rccm=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/rccm_20"),
                    
                    fk_forme_juridique=(safe_int(appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/forme_juridique_20")) if appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/forme_juridique_20") else None),
                    
                    id_nat=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/id_nat_20"),
                    
                    domaine_activite=appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/domaine_d_activite_20"),
                    
                    etranger=(1 if appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/est_il_etranger_50") is not None and appartment.get("informations_immeuble/group_no51r46/group_vv8fm85/est_il_etranger_50") == "oui" else None),
                    
                    fk_adresse=fk_adresse,
                    fk_agent=fk_agent,
                )
                
                db.add(proprietaire_appart)
                db.flush()
                
                fk_proprietaire_appart = proprietaire_appart.id
                
                bien = Bien(
                    numero_bien=appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/Num_ro_Appartement_Local"),
                    
                    numero_etage=(safe_int(appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/L_appartement_se_tro_tage_de_l_immeuble_")) if 
                    appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/L_appartement_se_tro_tage_de_l_immeuble_") else None),
                    
                    fk_usage=(safe_int(appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/usage")) if appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/usage") else None),
                    
                    fk_usage_specifique=(safe_int(appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/usage_specifique")) if appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/usage_specifique") else None),
                    
                    superficie=(float(appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/superficie")) if appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/superficie") else None),
                    
                    fk_unite=(safe_int(appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/unite_de_la_superficie_1")) if appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/unite_de_la_superficie_1") else None),
                    
                    fk_nature_bien=3,
                    
                    fk_parcelle=fk_parcelle,
                    
                    fk_agent=fk_agent,
                    
                    fk_bien_parent=fk_immeuble,
                    
                    fk_proprietaire=fk_proprietaire_appart,
                
                    date_create=date_create,
                )
                db.add(bien)
                db.flush()
                fk_bien = bien.id
                
                for menage in appartment.get("informations_immeuble/group_no51r46/group_if9yu58", []):
                    menage:dict = menage
                    
                    if menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/occupant_est_locataire_ou_proprietaire_2") != "bailleur":
                    
                        responsable = Personne(
                
                            date_create=date_create,
                
                            nom=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/nom"),##
                    
                            postnom=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/post_nom"),##
                            
                            prenom=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/prenom"),##
                            
                            sexe=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/genre"),##
                            
                            lieu_naissance=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/lieu_de_naissance"),##
                            
                            date_naissance=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/date_de_naissance"),##
                            
                            fk_type_personne=safe_int(menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/type_de_personne")) if menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/type_de_personne") else None, ##
                            
                            fk_nationalite=safe_int(menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/nationalite_4")) if menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/nationalite_4") else None,##
                            
                            fk_province=(safe_int(menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/province_4")) if menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/province_4") else None),##
                            
                            district=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/district_4"),##
                            
                            territoire=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/territoire_4"),##
                            
                            secteur=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/secteur_4"),##
                            
                            village=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/village_4"),##
                            
                            profession=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/profession"),##
                            
                            type_piece_identite=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/type_de_piece_d_identite_4"),##
                            
                            numero_piece_identite=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/numero_piece_d_identite_4"),##
                            
                            nom_du_pere=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/nom_du_pere_4"),##
                            
                            nom_de_la_mere=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/nom_de_la_mere_4"),##
                            
                            nombre_enfant=(safe_int(menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/nombre_d_enfants")) if menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/nombre_d_enfants") else None),##
                            
                            etat_civil=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/etat_civil_4"),##
                            
                            fk_lien_parente=(safe_int(menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/lien_de_parente_4")) if menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/lien_de_parente_4") else None),##
                            
                            niveau_etude=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/niveau_d_etudes"),##
                            
                            telephone=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/n_telephone"),##
                            
                            adresse_mail=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/adresse_email"),##
                            
                            denomination=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/denomination"), ##
                            
                            sigle=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/sigle"), ##
                            
                            numero_impot=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/numero_d_impot"), ##
                            
                            rccm=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/rccm"), ##
                            
                            id_nat=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/id_nat"), ##
                            
                            fk_forme_juridique=(safe_int(menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/forme_juridique")) if menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/forme_juridique") else None), ##
                            
                            domaine_activite=menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/domaine_d_activite"), ##
                            
                            etranger=(1 if menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/est_il_etranger_4") is not None and menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/est_il_etranger_4") == "oui" else None),##
                            
                            fk_adresse=fk_adresse,
                            fk_agent=fk_agent,
                        )
                        db.add(responsable)
                        db.flush()
                        fk_proprietaire_appart = responsable.id
                        
                        # 6. Insert into LocationBien (if the occupant is a locataire)
                        if menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/occupant_est_locataire_ou_proprietaire_2") == "locataire":
                            location_bien = LocationBien(
                                fk_personne=fk_proprietaire_appart,
                                fk_bien=fk_bien,
                                fk_agent=fk_agent,
                            )
                            db.add(location_bien)
                    
                    # 5. Insert into Personne (if the occupant is a locataire)
                    menage_bien = Menage(
                        fk_personne=fk_proprietaire_appart,
                        fk_bien=fk_bien,
                        fk_agent=fk_agent,
                        date_create=date_create,
                    )
                    db.add(menage_bien)
                    db.flush()
                    fk_menage = menage_bien.id
                    
                    # 7. Insert additional Personnes
                    for personne in menage.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes", []):
                        personne: dict = personne
                            
                        # Insert into Personne
                        new_personne = Personne(
                
                            date_create=date_create,
                            
                            nom=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/nom_3"),
                            
                            postnom=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/post_nom_3"),
                            
                            prenom=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/prenom_3"),
                            
                            sexe=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/gr"),
                            
                            lieu_naissance=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/lieu_de_naissance_1"),
                            
                            date_naissance=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/date_de_naissance_1"),
                            
                            fk_province=(safe_int(personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/province_d_origine")) if personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/province_d_origine") else None),
                            
                            district=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/district"),
                            
                            territoire=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/territoire"),
                            
                            secteur=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/secteur"),
                            
                            village=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/village"),
                            
                            profession=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/profession_2"),
                            
                            type_piece_identite=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/type_de_piece_d_identite"),
                            
                            numero_piece_identite=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/numero_piece_d_identite"),
                            
                            nom_du_pere=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/nom_du_pere"),
                            
                            nom_de_la_mere=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/nom_de_la_mere"),
                            
                            etat_civil=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/etat_civil"),
                            
                            fk_lien_parente=safe_int(personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/lien_de_parente")) if personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/lien_de_parente") else None,
                            
                            telephone=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/n_telphone"),
                            
                            adresse_mail=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/adresse_email_3"),
                            
                            nombre_enfant=(safe_int(personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/nombre_d_enfants_001")) if personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/nombre_d_enfants_001") else None),
                            
                            niveau_etude=personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/niveau_d_etudes_001"),
                            
                            fk_nationalite=(safe_int(personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/nationalite_3")) if personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/nationalite_3") else None),
                            
                            fk_adresse=fk_adresse,
                            
                            fk_agent=fk_agent,
                            
                            etranger=(1 if personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/est_il_etranger") is not None and personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/est_il_etranger") == "oui" else None)
                        )
                        db.add(new_personne)
                        db.flush()
                        fk_personne = new_personne.id
                        
                        # 8. Insert into MembreMenage
                        membre_menage = MembreMenage(
                            fk_menage=fk_menage,
                            fk_personne=fk_personne,
                            fk_filiation=safe_int(personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/lien_de_parente")) if personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/lien_de_parente") else None,
                            fk_agent=fk_agent,
                            date_create=date_create,
                        )
                        db.add(membre_menage)
                
        else:
            # 1. Insert into Adresse
            adresse = Adresse(
                fk_avenue=safe_int(kobo.get("Parcelle_non_accessible/avenue_1")),  # Assuming this is an ID
                numero=kobo.get("Parcelle_non_accessible/numero_parcellaire_1"),
                fk_agent=fk_agent,
                
                date_create=date_create,
            )
            db.add(adresse)
            db.flush()  # Flush to get the inserted ID
            fk_adresse = adresse.id
            
            # 2. Insert into Parcelle
            parcelle = Parcelle(
                numero_parcellaire=kobo.get("Parcelle_non_accessible/numero_parcellaire_1"),
                coordonnee_geographique=kobo.get("Parcelle_non_accessible/coordonne_geographique_1"),
                fk_rang=safe_int(kobo.get("Parcelle_non_accessible/rang_1")) if kobo.get("Parcelle_non_accessible/rang_1") else None,
                fk_proprietaire=fk_proprietaire,
                fk_adresse=fk_adresse,
                fk_agent=fk_agent,
                statut=2,
                
                date_create=date_create,
            )
            db.add(parcelle)            
    
        # 8. Insert into Logs
        log = Logs(
            logs="process_immeuble_form",
            id_kobo=record_id,
            data_json=str(payload),
            fk_agent=fk_agent,
            date_submission=date_create,
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




