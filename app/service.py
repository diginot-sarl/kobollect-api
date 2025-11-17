# app/service.py
import pytz
import requests
import json
import os
import ast

from sqlalchemy import or_, exists
from sqlalchemy.orm import Session
from sqlalchemy.inspection import inspect

from fastapi import BackgroundTasks, HTTPException, status
from fastapi.responses import JSONResponse
import logging
from app.models import (
    Adresse, Personne, Parcelle, Bien, LocationBien, Utilisateur, Logs, Menage, MembreMenage, RapportRecensement)
from app.utils import generate_nif, safe_int
from datetime import datetime
from typing import List, Dict

from app.models import Parcelle, Bien, Adresse


logging.basicConfig(format='%(asctime)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def process_recensement_form(payload: dict, db: Session, background_tasks: BackgroundTasks):
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
        # Check if the ID already exists in the logs table
        existing_log = db.query(Logs).filter(Logs.id_kobo == record_id, Logs.logs == 'process_recensement_form').first()
        if existing_log:
            # raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Le formulaire avec ID {record_id} déjà existante.")
            return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "failed", "message": f"Le formulaire avec ID {record_id} déjà existante."})
        
        # Check if the agent exists in the database
        existing_agent = db.query(Utilisateur).filter(Utilisateur.login == kobo["_submitted_by"]).first()
        if existing_agent:
            fk_agent = existing_agent.id
        else:
            fk_agent = 1  # Default agent ID if not found

        # Initialize variables
        fk_proprietaire = None    
        coordonnee_geographique = None
        superficie_parcelle_egale_bien = False

        # Check if we need to use household coordinates
        if kobo.get("adresse_de_la_parcelle/La_maison_occupe_t_elle_toute_") == "oui" or \
        kobo.get("adresse_de_la_parcelle/coordonne_geographique") is None:
            
            # Only process if household data exists
            biens = kobo.get("informations_du_menage", [])
            if biens:  # Guard against empty list
                first_bien = biens[0]
                superficie_parcelle_egale_bien = True
                coordonnee_geographique = (first_bien.get("informations_du_menage/informations_du_bien/coordonnee_geographique") if first_bien.get("informations_du_menage/informations_du_bien/coordonnee_geographique") else None)
        
        
        if kobo.get("parcelle_accessible_ou_non") == "oui":
            
            if kobo.get("adresse_de_la_parcelle/avenue") is None:
                raise HTTPException(status_code=status.HTTP_200_OK, detail="Avenue est requise.")
            
            fk_avenue = (
                11606 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 111111111
                else 10627 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 2217
                else 10654 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 2152
                
                else 11670 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18017
                else 11662 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18018
                else 16336 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18019
                else 11663 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18020
                else 11664 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18021
                else 11665 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18024
                else 11666 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18025
                else 16318 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18026
                else 16320 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18027
                else 16314 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18028
                else 11671 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18029
                else 11668 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18030
                else 16312 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18031
                else 11672 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18033
                else 16319 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18034
                else 11673 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18035
                
                else safe_int(kobo.get("adresse_de_la_parcelle/avenue"))
            )

            # 1. Insert into Adresse
            adresse = Adresse(
                fk_avenue=fk_avenue,  # Assuming this is an ID
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
            
            if kobo.get("Parcelle_non_accessible/avenue_1") is None:
                raise HTTPException(status_code=status.HTTP_200_OK, detail="Avenue est requise.")
            
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Erreur lors de l'insertion des données : {str(e)}\n Formulaire: process_recensement_form")


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
        # Check if the ID already exists in the logs table
        existing_log = db.query(Logs).filter(Logs.id_kobo == record_id, Logs.logs == "process_rapport_superviseur_form").first()
        if existing_log:
            # raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Le formulaire avec ID {record_id} déjà existante.")
            return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "failed", "message": f"Le formulaire avec ID {record_id} déjà existante."})
        
        # Check if the agent exists in the database
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Erreur lors de l'insertion des données : {str(e)}\n Formulaire: process_rapport_superviseur_form")


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
        # Check if the ID already exists in the logs table
        existing_log = db.query(Logs).filter(Logs.id_kobo == record_id, Logs.logs == "process_parcelles_non_baties_form").first()
        if existing_log:
            # raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Le formulaire avec ID {record_id} déjà existante.")
            return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "failed", "message": f"Le formulaire avec ID {record_id} déjà existante."})
        
        # Check if the agent exists in the database
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
            coordonnee_geographique = kobo.get("informations_du_menage/informations_du_bien/coordonnee_geographique")
            
        
        if kobo.get("adresse_de_la_parcelle/avenue") is None:
            raise HTTPException(status_code=status.HTTP_200_OK, detail="Avenue est requise.")
            
        fk_avenue = (
            11606 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 111111111
            else 10627 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 2217
            else 10654 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 2152
            
            else 11670 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18017
            else 11662 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18018
            else 16336 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18019
            else 11663 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18020
            else 11664 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18021
            else 11665 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18024
            else 11666 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18025
            else 16318 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18026
            else 16320 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18027
            else 16314 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18028
            else 11671 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18029
            else 11668 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18030
            else 16312 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18031
            else 11672 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18033
            else 16319 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18034
            else 11673 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18035
            
            else safe_int(kobo.get("adresse_de_la_parcelle/avenue"))
        )

        # 1. Insert into Adresse
        adresse = Adresse(
            fk_avenue=fk_avenue,  # Assuming this is an ID
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
            
            fk_nature_bien=(safe_int(kobo.get("informations_du_menage/informations_du_bien/nature")) if kobo.get("informations_du_menage/informations_du_bien/nature") else None),
            
            fk_unite=(safe_int(kobo.get("informations_du_menage/informations_du_bien/unite_de_la_superficie_1")) if kobo.get("informations_du_menage/informations_du_bien/unite_de_la_superficie_1") else None),
            
            fk_usage=(safe_int(kobo.get("informations_du_menage/informations_du_bien/usage")) if kobo.get("informations_du_menage/informations_du_bien/usage") else None),
            
            fk_usage_specifique=(safe_int(kobo.get("informations_du_menage/informations_du_bien/usage_specifique")) if kobo.get("informations_du_menage/informations_du_bien/usage_specifique") else None),
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Erreur lors de l'insertion des données : {str(e)}\n Formulaire: process_parcelles_non_baties_form")


def process_immeuble_plusieurs_proprietaires_form(payload: dict, db: Session, background_tasks: BackgroundTasks): ### Immeuble à plusieurss propriétaires
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
        # Check if the ID already exists in the logs table
        existing_log = db.query(Logs).filter(Logs.id_kobo == record_id, Logs.logs == "process_immeuble_form").first()
        if existing_log:
            # raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Le formulaire avec ID {record_id} déjà existante.")
            return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "failed", "message": f"Le formulaire avec ID {record_id} déjà existante."})
        
        # Check if the agent exists in the database
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
            
            if kobo.get("informations_immeuble/adresse_de_la_parcelle/avenue") is None:
                raise HTTPException(status_code=status.HTTP_200_OK, detail="L'avenue de la parcelle est obligatoire.")
            
            fk_avenue = (
                11606 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 111111111
                else 10627 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 2217
                else 10654 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 2152
                
                else 11670 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18017
                else 11662 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18018
                else 16336 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18019
                else 11663 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18020
                else 11664 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18021
                else 11665 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18024
                else 11666 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18025
                else 16318 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18026
                else 16320 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18027
                else 16314 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18028
                else 11671 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18029
                else 11668 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18030
                else 16312 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18031
                else 11672 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18033
                else 16319 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18034
                else 11673 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18035
                
                else safe_int(kobo.get("adresse_de_la_parcelle/avenue"))
            )
            
            # 1. Insert into Adresse
            adresse = Adresse(
                fk_avenue=fk_avenue,  # Assuming this is an ID
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
                
                est_parent=1,  # Assuming this is an Immeuble
                
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
            
            if kobo.get("Parcelle_non_accessible/avenue_1") is None:
                
                raise HTTPException(status_code=status.HTTP_200_OK, detail="Avenue is required for parcelle non-accessible.")
            
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Erreur lors de l'insertion des données : {str(e)}\n Formulaire: process_immeuble_form")


def process_immeuble_seul_proprietaire_form(payload: dict, db: Session, background_tasks: BackgroundTasks): ### Immeuble à un seul propriétaire
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
        # Check if the ID already exists in the logs table
        existing_log = db.query(Logs).filter(Logs.id_kobo == record_id, Logs.logs == "process_immeuble_form").first()
        if existing_log:
            # raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Le formulaire avec ID {record_id} déjà existante.")
            return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "failed", "message": f"Le formulaire avec ID {record_id} déjà existante."})
        
        # Check if the agent exists in the database
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
            
            if kobo.get("informations_immeuble/adresse_de_la_parcelle/avenue") is None:
                raise HTTPException(status_code=status.HTTP_200_OK, detail="L'avenue de la parcelle est obligatoire.")
            
            fk_avenue = (
                11606 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 111111111
                else 10627 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 2217
                else 10654 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 2152
                
                else 11670 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18017
                else 11662 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18018
                else 16336 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18019
                else 11663 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18020
                else 11664 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18021
                else 11665 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18024
                else 11666 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18025
                else 16318 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18026
                else 16320 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18027
                else 16314 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18028
                else 11671 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18029
                else 11668 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18030
                else 16312 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18031
                else 11672 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18033
                else 16319 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18034
                else 11673 if safe_int(kobo.get("adresse_de_la_parcelle/avenue")) == 18035
                
                else safe_int(kobo.get("adresse_de_la_parcelle/avenue"))
            )
            
            # 1. Insert into Adresse
            adresse = Adresse(
                fk_avenue=fk_avenue,  # Assuming this is an ID
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
                
                est_parent=1,
                
                nombre_etage=(safe_int(kobo.get("informations_immeuble/adresse_de_la_parcelle/nombre_d_etages")) if kobo.get("informations_immeuble/adresse_de_la_parcelle/nombre_d_etages") else None),
            )
            db.add(immeuble)
            db.flush()
            fk_immeuble = immeuble.id
            
            for appartment in kobo.get("informations_immeuble/group_no51r46",[]):
                appartment: dict = appartment
                
                for menage in appartment.get("informations_immeuble/group_no51r46/group_if9yu58", []):
                    menage:dict = menage
                    
                    fk_responsable = None
                                        
                    ### Ajouter un bien pour l'appartement
                    bien = Bien(
                        numero_bien=appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/Num_ro_Appartement_Local"),
                        
                        numero_etage=(safe_int(appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/L_appartement_se_tro_tage_de_l_immeuble_")) if appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/L_appartement_se_tro_tage_de_l_immeuble_") else None),
                        
                        fk_usage=(safe_int(appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/usage")) if appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/usage") else None),
                        
                        fk_usage_specifique=(safe_int(appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/usage_specifique")) if appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/usage_specifique") else None),
                        
                        superficie=(float(appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/superficie")) if appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/superficie") else None),
                        
                        fk_unite=(safe_int(appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/unite_de_la_superficie_1")) if appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/unite_de_la_superficie_1") else None),
                        
                        fk_nature_bien=(safe_int(appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/nature")) if appartment.get("informations_immeuble/group_no51r46/group_gy9pw51/nature") else None),
                        
                        fk_bien_parent=fk_immeuble,
                        
                        fk_parcelle=fk_parcelle,
                        
                        fk_agent=fk_agent,
                    
                        date_create=date_create,
                    )
                    db.add(bien)
                    db.flush()
                    fk_bien = bien.id
                    
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
                        fk_responsable = responsable.id
                        
                        # 6. Insert into LocationBien (if the occupant is a locataire)
                        if menage.get("informations_immeuble/group_no51r46/group_if9yu58/informations_de_l_occupant/occupant_est_locataire_ou_proprietaire_2") == "locataire":
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
                            fk_filiation=(safe_int(personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/lien_de_parente")) if personne.get("informations_immeuble/group_no51r46/group_if9yu58/information_sur_les_personnes/lien_de_parente") else None),
                            fk_agent=fk_agent,
                            date_create=date_create,
                        )
                        db.add(membre_menage)
                
        else:
            
            if kobo.get("Parcelle_non_accessible/avenue_1") is None:
                
                raise HTTPException(status_code=status.HTTP_200_OK, detail="Avenue is required for parcelle non-accessible.")
            
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Erreur lors de l'insertion des données : {str(e)}\n Formulaire: process_immeuble_form")


def update_to_erecettes_v1(updated_keys: list[dict], db: Session):
    
    userCreat = 1908
    """
    Send updated parcelle and bien data to the remote server in the background.
    
    Args:
        updated_keys (list[dict]): List of dictionaries with 'type' ('parcelle' or 'bien') and 'id'.
    """
    try:
        # Step 1: Collect unique parcelle IDs from updated_keys
        parcelle_ids = set()
        for key in updated_keys:
            if key["type"] == "parcelle":
                parcelle = db.query(Parcelle).filter(Parcelle.id == key["id"]).first()
                if parcelle:
                    # Include parcelle if it has fk_proprietaire or any bien with fk_proprietaire
                    has_biens_with_proprietaire = db.query(Bien).filter(
                        Bien.fk_parcelle == parcelle.id,
                        Bien.fk_proprietaire != None
                    ).count() > 0
                    if parcelle.fk_proprietaire or has_biens_with_proprietaire:
                        parcelle_ids.add(parcelle.id)
            elif key["type"] == "bien":
                bien = db.query(Bien).filter(Bien.id == key["id"]).first()
                if bien and bien.fk_parcelle and bien.fk_proprietaire:
                    parcelle_ids.add(bien.fk_parcelle)

        # Step 2: Construct the payload for each unique parcelle
        parcelles_payload = []
        for parcelle_id in parcelle_ids:
            parcelle = db.query(Parcelle).filter(Parcelle.id == parcelle_id).first()
            if not parcelle:
                continue

            # Fetch fk_avenue from Adresse
            adresse = db.query(Adresse).filter(Adresse.id == parcelle.fk_adresse).first()
            fk_avenue = adresse.fk_avenue if adresse else 0

            # Fetch parent biens (fk_bien_parent is None and fk_proprietaire exists)
            parent_biens = db.query(Bien).filter(
                Bien.fk_parcelle == parcelle_id,
                Bien.fk_bien_parent == None,
                Bien.fk_proprietaire != None
            ).all()
            
            # Construct biens payload (second level)
            biens_payload = []
            for bien in parent_biens:
                # Fetch child biens (etage level) where fk_bien_parent matches this bien's id
                child_biens = db.query(Bien).filter(
                    Bien.fk_bien_parent == bien.id,
                    Bien.fk_proprietaire != None
                ).all()
                
                # Third level: etage
                etage_payload = []
                for child in child_biens:
                    etage = {
                        "relance": 0,
                        "niveau_etage": child.numero_etage or None,
                        "fk_contribuable": str(child.fk_proprietaire),
                        "userCreat": userCreat,
                        "valeurUnite": child.nombre_etage or None,
                        "superficieBien": str(child.superficie_corrige or child.superficie or None),
                        "fk_unite": str(child.fk_unite) if child.fk_unite else "",
                        "fk_nature": child.fk_nature_bien or None,
                        "fk_usage": child.fk_usage or None,
                        "coordonnee": child.coord_corrige or "",
                        "intitule": "",
                        "fk_rang": parcelle.fk_rang if parcelle.fk_rang else None,
                        "dataJsonBien": json.dumps(model_to_dict(child)),
                    }
                    etage_payload.append(etage)
                
                # Second level: bien
                bien_payload = {
                    "relance": 0,
                    "niveau_etage": bien.numero_etage or None,
                    "fk_contribuable": str(bien.fk_proprietaire),
                    "userCreat": 1,
                    "valeurUnite": bien.nombre_etage or None,
                    "superficieBien": str(bien.superficie_corrige or bien.superficie or None),
                    "fk_unite": str(bien.fk_unite) if bien.fk_unite else "",
                    "fk_nature": bien.fk_nature_bien or None,
                    "fk_usage": bien.fk_usage or None,
                    "coordonnee": bien.coord_corrige or "",
                    "intitule": "",
                    "fk_rang": parcelle.fk_rang or None,
                    "dataJsonBien": json.dumps(model_to_dict(bien)),
                    "etage": etage_payload,
                }
                biens_payload.append(bien_payload)
            
            # First level: parcelle
            many_contribuable = 1 if len(parent_biens) > 1 else 0
            
            parcelle_payload = {
                "manyContribuable": many_contribuable,
                "fk_contribuable": str(parcelle.fk_proprietaire) if parcelle.fk_proprietaire else "",
                "coordinates": parcelle.coord_corrige or parcelle.coordonnee_geographique or "",
                "largeur": parcelle.largeur or 0,
                "longeur": parcelle.longueur or 0,  # Note: Kept as 'longeur' to match example payload
                "superficie": str(parcelle.superficie_corrige or parcelle.superficie_calculee or None),
                "coordinatesProjected": parcelle.coord_projected or None,
                "agent_Creat": userCreat,
                "fk_avenue": fk_avenue,
                "numero": parcelle.numero_parcellaire or None,
                "dataJsonParcelle": json.dumps(model_to_dict(parcelle)),
                "fk_rang": parcelle.fk_rang or None,
                "adressContribuableExist": 0,
                "fk_parcelle": str(parcelle.id),
                "biens": biens_payload,
            }
            parcelles_payload.append(parcelle_payload)

        # Step 3: Send the POST request if there is data to send
        if parcelles_payload:
            try:
                url = "https://api-dgrk-ms-sig-dev.apps.kubedev.hologram.cd/api/biens/creer"
                payload = {"parcelles": parcelles_payload}
                response = requests.post(url, json=payload)
                # Log response for debugging (in production, use proper logging)
                print(f"Response: {response.status_code}, {response.text}")
            except Exception as e:
               print(f"Error sending data to remote server: {str(e)}")

    finally:
        db.close()


def update_to_erecettes_v2(updated_keys: list[dict], db: Session):
    """
    Send updated parcelle and bien data to the remote server in the background based on the new request body structure.
    
    Args:
        updated_keys (list[dict]): List of dictionaries with 'type' ('parcelle' or 'bien') and 'id'.
        db (Session): SQLAlchemy database session.
    """
    userCreat = 1908  # Hardcoded user ID for agent_Creat
    try:
        # Step 1: Collect unique proprietaire IDs from updated_keys
        proprietaire_ids = set()
        for key in updated_keys:
            if key["type"] == "parcelle":
                parcelle = db.query(Parcelle).filter(Parcelle.id == key["id"]).first()
                if parcelle and parcelle.fk_proprietaire:
                    proprietaire_ids.add(parcelle.fk_proprietaire)
            elif key["type"] == "bien":
                bien = db.query(Bien).filter(Bien.id == key["id"]).first()
                if bien and bien.fk_proprietaire:
                    proprietaire_ids.add(bien.fk_proprietaire)

        # Step 2: For each unique proprietaire, construct the payload
        for proprietaire_id in proprietaire_ids:
            proprietaire = db.query(Personne).filter(Personne.id == proprietaire_id).first()
            if not proprietaire:
                continue

            # Fetch all parcelles for this proprietaire
            parcelles = db.query(Parcelle).filter(Parcelle.fk_proprietaire == proprietaire_id).all() ### probleme
            parcelles_payload = []
            
            if len(parcelles) > 0:
                adresse = db.query(Adresse).filter(Adresse.id == parcelles[0].fk_adresse).first()
                fk_avenue = adresse.fk_avenue if adresse else None
                
                numero_parcellaire = parcelles[0].numero_parcellaire or None
                
                # Construct contribuable payload
                contribuable_payload = {
                    "nom": proprietaire.nom or "",
                    "prenom": proprietaire.prenom or "",
                    "postnom": proprietaire.postnom or "",
                    "fk_forme": proprietaire.fk_type_personne or None,
                    "idnat": proprietaire.id_nat or "",
                    "rccm": proprietaire.rccm or "",
                    "sigle": proprietaire.sigle or "",
                    "sexe": proprietaire.sexe or "",
                    "agent_Creat": userCreat,
                    "fk_pays": proprietaire.fk_nationalite or None,
                    "fk_avenue": fk_avenue,  # No direct link in Personne model
                    "numero": numero_parcellaire,   # No direct link in Personne model
                    "telephone": proprietaire.telephone or "",
                    "email": proprietaire.adresse_mail or ""
                }
            
            for parcelle in parcelles:
                # Fetch fk_avenue from Adresse
                adresse = db.query(Adresse).filter(Adresse.id == parcelle.fk_adresse).first()
                fk_avenue = adresse.fk_avenue if adresse else 0

                # Fetch parent biens (fk_bien_parent is None)
                parent_biens = db.query(Bien).filter(
                    Bien.fk_parcelle == parcelle.id,
                    Bien.fk_bien_parent == None,
                    Bien.fk_proprietaire != None
                ).all()

                # Construct biens payload
                biens_payload = []
                for bien in parent_biens:
                    # Fetch child biens (sous_biens)
                    child_biens = db.query(Bien).filter(Bien.fk_bien_parent == bien.id).all()

                    # Construct sous_biens payload
                    sous_biens_payload = []
                    for child in child_biens:
                        sous_bien = {
                            "intitule": "",  # No intitule field in Bien model
                            "fk_nature": child.fk_nature_bien or 0,
                            "fk_usage": child.fk_usage or 0,
                            "coordinates": child.coord_corrige or "",
                            "superficie": str(child.superficie_corrige or child.superficie or 0),
                            "valeur_unite": child.nombre_etage or 0,
                            "fk_unite": child.fk_unite or 0,
                            "fk_rang": parcelle.fk_rang or 0,
                            "niveau_etage": child.numero_etage or 0
                        }
                        sous_biens_payload.append(sous_bien)

                    # Construct bien payload
                    bien_payload = {
                        "intitule": "",  # No intitule field in Bien model
                        "fk_nature": bien.fk_nature_bien or 0,
                        "fk_usage": bien.fk_usage or 0,
                        "coordinates": bien.coord_corrige or "",
                        "superficie": str(bien.superficie_corrige or bien.superficie or 0),
                        "valeur_unite": bien.nombre_etage or 0,
                        "fk_unite": bien.fk_unite or 0,
                        "fk_rang": parcelle.fk_rang or 0,
                        "niveau_etage": bien.numero_etage or 0,
                        "sous_biens": sous_biens_payload
                    }
                    biens_payload.append(bien_payload)

                # Construct parcelle payload
                parcelle_payload = {
                    "coordinates": parcelle.coord_corrige or parcelle.coordonnee_geographique or "",
                    "largeur": parcelle.largeur or 0,
                    "longeur": parcelle.longueur or 0,
                    "superficie": str(parcelle.superficie_corrige or parcelle.superficie_calculee or 0),
                    "fk_avenue": fk_avenue,
                    "numero": parcelle.numero_parcellaire or "",
                    "fk_rang": parcelle.fk_rang or 0,
                    "biens": biens_payload
                }
                parcelles_payload.append(parcelle_payload)

            # Step 3: Send the POST request for this contribuable and their parcelles
            if parcelles_payload:
                url = "https://api-dgrk-ms-sig-dev.apps.kubedev.hologram.cd/api/contribuables/creer-avec-biens"
                payload = {
                    "contribuable": contribuable_payload or None,
                    "parcelles": parcelles_payload
                }
                response = requests.post(url, json=payload)
                print(f"Payload: {payload}")
                print(f"Response: {response.status_code}, {response.text}")

    finally:
        db.close()
        
        
def update_to_erecettes_v3(updated_keys: list[dict], db: Session):
    """
    Send updated parcelle and bien data to the remote server in the background based on the new request body structure.
    
    Args:
        updated_keys (list[dict]): List of dictionaries with 'type' ('parcelle' or 'bien') and 'id'.
        db (Session): SQLAlchemy database session.
    """
    userCreat = 1908  # Hardcoded user ID for agent_Creat
    try:
        # Step 1: Collect updated parcelle and bien IDs
        updated_parcelle_ids = {key["id"] for key in updated_keys if key["type"] == "parcelle"}
        updated_bien_ids = {key["id"] for key in updated_keys if key["type"] == "bien"}

        # Step 2: Collect parcelles to include: directly updated or associated with updated biens
        parcelle_ids_from_biens = db.query(Bien.fk_parcelle).filter(Bien.id.in_(updated_bien_ids)).distinct().all()
        parcelle_ids_to_include = updated_parcelle_ids.union({id for (id,) in parcelle_ids_from_biens})

        # Step 3: Collect unique proprietaire IDs from updated parcelles and biens
        proprietaire_ids = set()
        for key in updated_keys:
            if key["type"] == "parcelle":
                parcelle = db.query(Parcelle).filter(Parcelle.id == key["id"]).first()
                if parcelle and parcelle.fk_proprietaire:
                    proprietaire_ids.add(parcelle.fk_proprietaire)
            elif key["type"] == "bien":
                bien = db.query(Bien).filter(Bien.id == key["id"]).first()
                if bien and bien.fk_proprietaire:
                    proprietaire_ids.add(bien.fk_proprietaire)

        # Step 4: For each proprietaire, construct the payload
        for proprietaire_id in proprietaire_ids:
            proprietaire = db.query(Personne).filter(Personne.id == proprietaire_id).first()
            if not proprietaire:
                continue

            # Fetch only parcelles that were updated or have updated biens
            parcelles = db.query(Parcelle).filter(
                Parcelle.fk_proprietaire == proprietaire_id,
                Parcelle.id.in_(parcelle_ids_to_include)
            ).all()
            if not parcelles:
                continue

            # Use first parcelle's adresse for contribuable details
            adresse = db.query(Adresse).filter(Adresse.id == parcelles[0].fk_adresse).first()
            fk_avenue = adresse.fk_avenue if adresse else None
            numero_parcellaire = parcelles[0].numero_parcellaire or None

            # Construct contribuable payload
            contribuable_payload = {
                "nom": proprietaire.nom or "",
                "prenom": proprietaire.prenom or "",
                "postnom": proprietaire.postnom or "",
                "fk_forme": proprietaire.fk_type_personne or None,
                "idnat": proprietaire.id_nat or "",
                "rccm": proprietaire.rccm or "",
                "sigle": proprietaire.sigle or "",
                "sexe": proprietaire.sexe or "",
                "agent_Creat": userCreat,
                "fk_pays": proprietaire.fk_nationalite or None,
                "fk_avenue": fk_avenue,
                "numero": numero_parcellaire,
                "telephone": proprietaire.telephone or "",
                "email": proprietaire.adresse_mail or ""
            }

            parcelles_payload = []
            for parcelle in parcelles:
                # Fetch parent biens to include: updated or with updated child biens
                parent_bien_ids_to_include = db.query(Bien.id).filter(
                    Bien.fk_parcelle == parcelle.id,
                    Bien.fk_bien_parent == None,
                    Bien.fk_proprietaire != None,
                    or_(
                        Bien.id.in_(updated_bien_ids),
                        exists().where(
                            Bien.id == Bien.fk_bien_parent,
                            Bien.id.in_(updated_bien_ids)
                        )
                    )
                ).all()
                parent_bien_ids_to_include = [id for (id,) in parent_bien_ids_to_include]

                parent_biens = db.query(Bien).filter(Bien.id.in_(parent_bien_ids_to_include)).all()

                biens_payload = []
                for bien in parent_biens:
                    # Fetch only updated child biens
                    child_biens = db.query(Bien).filter(
                        Bien.fk_bien_parent == bien.id,
                        Bien.id.in_(updated_bien_ids)
                    ).all()

                    # Construct sous_biens payload
                    sous_biens_payload = []
                    for child in child_biens:
                        sous_bien = {
                            "intitule": "",
                            "fk_nature": child.fk_nature_bien or None,
                            "fk_usage": child.fk_usage or None,
                            "coordinates": child.coord_corrige or "",
                            "superficie": str(child.superficie_corrige or child.superficie or None),
                            "valeur_unite": child.nombre_etage or None,
                            "fk_unite": child.fk_unite or None,
                            "fk_rang": parcelle.fk_rang or None,
                            "niveau_etage": child.numero_etage or None
                        }
                        sous_biens_payload.append(sous_bien)

                    # Construct bien payload
                    bien_payload = {
                        "intitule": "",
                        "fk_nature": bien.fk_nature_bien or None,
                        "fk_usage": bien.fk_usage or None,
                        "coordinates": bien.coord_corrige or "",
                        "superficie": str(bien.superficie_corrige or bien.superficie or None),
                        "valeur_unite": bien.nombre_etage or None,
                        "fk_unite": bien.fk_unite or None,
                        "fk_rang": parcelle.fk_rang or None,
                        "niveau_etage": bien.numero_etage or None,
                        "sous_biens": sous_biens_payload
                    }
                    biens_payload.append(bien_payload)

                # Fetch fk_avenue for parcelle
                adresse = db.query(Adresse).filter(Adresse.id == parcelle.fk_adresse).first()
                fk_avenue = adresse.fk_avenue if adresse else 0

                # Construct parcelle payload
                parcelle_payload = {
                    "coordinates": parcelle.coord_corrige or parcelle.coordonnee_geographique or "",
                    "largeur": parcelle.largeur or None,
                    "longeur": parcelle.longueur or None,
                    "superficie": str(parcelle.superficie_corrige or parcelle.superficie_calculee or None),
                    "fk_avenue": fk_avenue,
                    "numero": parcelle.numero_parcellaire or "",
                    "fk_rang": parcelle.fk_rang or None,
                    "biens": biens_payload
                }
                parcelles_payload.append(parcelle_payload)

            # Step 5: Send the POST request if there are parcelles
            if parcelles_payload:
                url = "https://api-dgrk-ms-sig-dev.apps.kubedev.hologram.cd/api/contribuables/creer-avec-biens"
                payload = {
                    "contribuable": contribuable_payload,
                    "parcelles": parcelles_payload
                }
                response = requests.post(url, json=payload)
                print(f"Payload: {json.dumps(payload, indent=2)}")
                print(f"Response: {response.status_code}, {response.text}")

    finally:
        db.close()
        

def format_coordinates_erecettes(coord_str: str) -> str:
    """
    Converts a GeoJSON-like polygon string (stored as string in DB)
    Example input:
        '[[[15.317859935860959, pata -4.417842795135316], [15.317957045165862, -4.417813899012001], ...]]'
    
    Output required by e-recettes:
        '-4.417998333333333 15.317995 384.2 1.8; -4.418055 15.31798 384.9 1.6; ...; <first_point_again>'
    
    Note: The 3rd value seems to be altitude (~384-385m in Kinshasa)
          The 4th value seems to be a quality/confidence index or number of floors
    → Since your current data doesn't have Z or this 4th value, we use realistic defaults.
    """
    if not coord_str or str(coord_str).strip() == "":
        return ""

    coord_str = str(coord_str).strip()

    # Try to parse the coordinate string
    try:
        coords = json.loads(coord_str)
    except json.JSONDecodeError:
        try:
            coords = ast.literal_eval(coord_str)
        except:
            print(f"[WARN] Impossible to parse coordinates: {coord_str[:100]}...")
            return ""

    # Extract the first ring (Polygon → [[[...]]])
    while isinstance(coords, list) and len(coords) == 1 and isinstance(coords[0], list):
        coords = coords[0]

    if not coords or not isinstance(coords[0], list) or len(coords[0]) < 2:
        return ""

    result = []
    for i, point in enumerate(coords):
        if not isinstance(point, (list, tuple)) or len(point) < 2:
            continue
        lon, lat = float(point[0]), float(point[1])

        # Realistic values for Kinshasa (Plateau des Batéké ≈ 300–400m)
        # 3rd value: altitude in meters → we use a plausible fixed value
        altitude = 384.5

        # 4th value: unknown → from your example: 1.8, 1.6, 1.5 → likely quality or floor count
        # We'll use 1.5 as default (common in similar systems)
        quality = 1.5

        result.append(f"{lat:.12f} {lon:.12f} {altitude:.1f} {quality}")

    # Close the polygon: repeat the first point at the end
    if len(result) > 2:
        result.append(result[0])

    return "; ".join(result)


def update_to_erecettes(updated_keys: List[Dict], db: Session):
    """
    Send parcelle + biens to the remote e-recettes server.
    When the remote returns 201 → update the local Personne.nif.
    """
    userCreat = 1908  # Hard-coded agent
    
    print("-------- Updating to eRecettes --------")

    try:
        for key in updated_keys:
            parcelle_id = key.get("parcelle")
            bien_ids = key.get("biens", [])

            # ------------------------------------------------------------------
            # 1. Load parcelle + propriétaire
            # ------------------------------------------------------------------
            parcelle = db.query(Parcelle).filter(Parcelle.id == parcelle_id).first()
            if not parcelle:
                print(f"[WARN] Parcelle {parcelle_id} not found – skipping")
                continue

            proprietaire = (
                db.query(Personne)
                .filter(Personne.id == parcelle.fk_proprietaire)
                .first()
            )
            if not proprietaire:
                print(f"[WARN] Propriétaire for parcelle {parcelle_id} missing – skipping")
                continue

            # ------------------------------------------------------------------
            # 2. Build payload (unchanged)
            # ------------------------------------------------------------------
            adresse = db.query(Adresse).filter(Adresse.id == parcelle.fk_adresse).first()
            fk_avenue = adresse.fk_avenue if adresse else None
            numero_parcellaire = parcelle.numero_parcellaire

            # ---- biens payload -------------------------------------------------
            biens_payload = []
            biens = (
                db.query(Bien)
                .filter(Bien.id.in_(bien_ids), Bien.fk_parcelle == parcelle.id)
                .all()
            )
            for bien in biens:
                biens_payload.append(
                    {
                        "intitule": "*",
                        "fk_nature": bien.fk_nature_bien,
                        "fk_usage": bien.fk_usage,
                        "coordinates": bien.coord_corrige or bien.coordinates or "",
                        "superficie": str(bien.superficie_corrige or bien.superficie or 0),
                        "valeur_unite": bien.nombre_etage,
                        "fk_unite": bien.fk_unite,
                        "fk_rang": parcelle.fk_rang,
                        "niveau_etage": bien.numero_etage,
                        "sous_biens": [],
                    }
                )

            # ---- contribuable payload -----------------------------------------
            contribuable_payload = {
                "nom": proprietaire.nom or "",
                "prenom": proprietaire.prenom or "",
                "postnom": proprietaire.postnom or "",
                "fk_forme": proprietaire.fk_type_personne,
                "idnat": proprietaire.id_nat or "",
                "rccm": proprietaire.rccm or "",
                "sigle": proprietaire.sigle or "",
                "sexe": proprietaire.sexe or "",
                "agent_Creat": userCreat,
                "fk_pays": proprietaire.fk_nationalite,
                "fk_avenue": fk_avenue,
                "numero": numero_parcellaire,
                "telephone": proprietaire.telephone or "",
                "email": proprietaire.adresse_mail or "",
                "src": "hids_collect",
            }

            # ---- parcelle payload ---------------------------------------------
            raw_coords = parcelle.coord_corrige or parcelle.coordonnee_geographique or ""
            formatted_coords = format_coordinates_erecettes(raw_coords)
            
            parcelle_payload = {
                "coordinates": formatted_coords,  # This is now in the correct format!
                "largeur": parcelle.largeur or 0,
                "longueur": parcelle.longueur or 0,
                "superficie": str(parcelle.superficie_corrige or parcelle.superficie_calculee or 0),
                "fk_avenue": fk_avenue,
                "numero": numero_parcellaire,
                "fk_rang": parcelle.fk_rang,
                "biens": biens_payload,
            }

            payload = {
                "contribuable": contribuable_payload,
                "parcelles": [parcelle_payload],
            }
            
            print(f"Contribuable:{payload['contribuable']}")

            # ------------------------------------------------------------------
            # 3. POST to e-recettes
            # ------------------------------------------------------------------
            url = os.getenv("ERECETTES_URL")
            if not url:
                print("[ERROR] ERECETTES_URL not configured")
                continue

            print(f"[INFO] Sending to {url} – parcelle {parcelle_id}")
            response = requests.post(url, json=payload)

            # ------------------------------------------------------------------
            # 4. Handle response
            # ------------------------------------------------------------------
            if response.status_code == 201:
                try:
                    resp_json = response.json()
                    nif = resp_json.get("data", {}).get("contribuable", {}).get("nif")

                    if nif and nif != proprietaire.nif:
                        proprietaire.nif = nif
                        parcelle.date_erecettes = datetime.now(pytz.UTC)
                        
                        db.add(proprietaire)
                        db.add(parcelle)
            
                        db.commit()
                        print(f"[SUCCESS] NIF updated → Personne {proprietaire.id} = {nif}")
                    else:
                        # Optional: even if NIF didn't change, still mark as synced?
                        parcelle.date_erecettes = datetime.now(pytz.UTC)
                        db.add(parcelle)
                        db.commit()
                        print(f"[INFO] NIF unchanged for Personne {proprietaire.id}")
                        print(f"[INFO] Already up to date – sync timestamp updated for parcelle {parcelle_id}")

                except Exception as parse_err:
                    print(f"[ERROR] Failed to parse 201 response: {parse_err}")
                    print(f"Raw response: {response.text}")
                    db.rollback()

            else:
                # ------------------------------------------------------------------
                # 5. Non-201 → log but do NOT update NIF
                # ------------------------------------------------------------------
                print(
                    f"[ERROR] Remote failed (parcelle {parcelle_id}) – "
                    f"status {response.status_code}: {response.text}"
                )
                # optional: db.rollback() if you want to undo anything done before POST

    except Exception as e:
        print(f"[FATAL] update_to_erecettes crashed: {str(e)}")
        db.rollback()
    finally:
        db.close()

        
def model_to_dict(model):
    """Convert a SQLAlchemy model instance to a dictionary of column attributes."""
    return {c.key: getattr(model, c.key) for c in inspect(model).mapper.column_attrs}
        
        
        
        
        
        
        
        