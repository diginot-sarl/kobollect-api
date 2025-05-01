# app/service.py
from sqlalchemy.orm import Session
from fastapi import HTTPException
import logging
from app.models import Adresse, Personne, Parcelle, Bien, LocationBien, Utilisateur, Logs
from app.auth import get_password_hash
from app.schemas import UserCreate
import datetime

logger = logging.getLogger(__name__)

def process_kobo_data(payload: dict, db: Session):
    kobo: dict = payload["received_data"]
    record_id = kobo["_id"]

    try:
        # Check if the ID already exists in the logs table
        existing_log = db.query(Logs).filter(Logs.id_kobo == record_id).first()
        if existing_log:
            return {"status": "duplicate", "message": f"Donnée avec _id {record_id} déjà existante."}

        # Initialize variables
        fk_agent = 1  # Hardcoded for now; you might want to derive this dynamically
        fk_parcelle = None
        responsable_est_locataire = True if kobo.get("informations_de_l_occupant/occupant_est_locataire_ou_proprietaire_2") == "locataire" else False
        fk_proprietaire = None
        fk_locataire = None

        # 1. Insert into Adresse
        adresse = Adresse(
            fk_avenue=int(kobo["adresse_de_la_parcelle/avenue"]),  # Assuming this is an ID
            numero=kobo["adresse_de_la_parcelle/numero_parcellaire"],
            fk_agent=fk_agent,
            etat=True  # Assuming default value for etat
        )
        db.add(adresse)
        db.flush()  # Flush to get the inserted ID
        fk_adresse = adresse.id

        # 2. Insert Occupant (Propriétaire/Locataire) into Personne
        occupant = Personne(
            nom=kobo.get("informations_de_l_occupant/nom"),
            postnom=kobo.get("informations_de_l_occupant/post_nom"),
            prenom=kobo.get("informations_de_l_occupant/prenom"),
            sexe=kobo.get("informations_de_l_occupant/genre"),
            fk_type_personne=int(kobo.get("informations_de_l_occupant/type_de_personne")) if kobo.get("informations_de_l_occupant/type_de_personne") else None,
            lieu_naissance=kobo.get("informations_de_l_occupant/lieu_de_naissance"),
            date_naissance=kobo.get("informations_de_l_occupant/date_de_naissance"),
            province_origine=None,
            district=None,
            territoire=None,
            secteur=None,
            village=None,
            fk_nationalite=int(kobo.get("informations_de_l_occupant/nationalite")) if kobo.get("informations_de_l_occupant/nationalite") else None,
            profession=kobo.get("informations_de_l_occupant/profession"),
            type_piece_identite=None,
            numero_piece_identite=None,
            nom_du_pere=None,
            nom_de_la_mere=None,
            etat_civil=None,
            lieu_parente=None,
            telephone=kobo.get("informations_de_l_occupant/n_telephone"),
            adresse_mail=kobo.get("informations_de_l_occupant/adresse_email"),
            nombre_enfant=int(kobo.get("informations_de_l_occupant/nombre_d_enfants")) if kobo.get("informations_de_l_occupant/nombre_d_enfants") else None,
            niveau_etude=kobo.get("informations_de_l_occupant/niveau_d_etudes"),
            denomination=kobo.get("informations_de_l_occupant/denomination"),
            sigle=kobo.get("informations_de_l_occupant/sigle"),
            numero_impot=kobo.get("informations_de_l_occupant/numero_d_impot"),
            rccm=kobo.get("informations_de_l_occupant/rccm"),
            id_nat=kobo.get("informations_de_l_occupant/id_nat"),
            domaine_activite=kobo.get("informations_de_l_occupant/domaine_d_activite"),
            fk_adresse=fk_adresse,
            fk_agent=fk_agent,
        )
        db.add(occupant)
        db.flush()
        
        if kobo["informations_de_l_occupant/occupant_est_locataire_ou_proprietaire_2"] == "bailleur":
            fk_proprietaire = occupant.id
        else:
            fk_locataire = occupant.id

        # 3. Insert Propriétaire into Personne (if they don't live in the parcel)
        if kobo.get("informations_de_l_occupant/le_proprietaire_habite_t_il_dans_la_parcelle") == 'non':
            proprietaire = Personne(
                nom=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nom_2"),
                postnom=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/post_nom_2"),
                prenom=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/prenom_2"),
                sexe=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/genre_2"),
                fk_type_personne=int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/tp")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/tp") else None,
                lieu_naissance=None,
                date_naissance=None,
                province_origine=None,
                district=None,
                territoire=None,
                secteur=None,
                village=None,
                fk_nationalite=int(kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nationalite_2")) if kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nationalite_2") else None,
                profession=None,
                type_piece_identite=None,
                numero_piece_identite=None,
                nom_du_pere=None,
                nom_de_la_mere=None,
                etat_civil=None,
                lieu_parente=None,
                telephone=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/n_telephone_2"),
                adresse_mail=kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/adresse_email_2"),
                nombre_enfant=None,
                niveau_etude=None,
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

        # 4. Insert into Parcelle
        parcelle = Parcelle(
            ref_parcelle=None,
            numero_parcellaire=kobo.get("adresse_de_la_parcelle/numero_parcellaire"),
            fk_unite=int(kobo.get("adresse_de_la_parcelle/Unit")) if kobo.get("adresse_de_la_parcelle/Unit") else None,
            longueur=float(kobo.get("adresse_de_la_parcelle/longueur")) if kobo.get("adresse_de_la_parcelle/longueur") else None,
            largeur=float(kobo.get("adresse_de_la_parcelle/largeur")) if kobo.get("adresse_de_la_parcelle/largeur") else None,
            superficie_calculee=float(kobo.get("adresse_de_la_parcelle/calculation")) if kobo.get("adresse_de_la_parcelle/calculation") else None,
            coordonnee_geographique=kobo.get("adresse_de_la_parcelle/coordonne_geographique"),
            fk_rang=int(kobo.get("adresse_de_la_parcelle/rang")) if kobo.get("adresse_de_la_parcelle/rang") else None,
            fk_proprietaire=fk_proprietaire,
            fk_adresse=fk_adresse,
            fk_agent=fk_agent,
            date_create=datetime.utcnow(),
            etat=1  # Assuming active state
        )
        db.add(parcelle)
        db.flush()
        fk_parcelle = parcelle.id

        # 5. Insert into Bien
        bien = Bien(
            ref_bien=kobo.get("informations_du_menage/numero_bien"),
            coordinates=kobo.get("informations_du_menage/coordonnee_geographique"),
            fk_parcelle=fk_parcelle,
            fk_nature_bien=int(kobo.get("informations_du_menage/nature")) if kobo.get("informations_du_menage/nature") else None,
            fk_unite=int(kobo.get("informations_du_menage/unite_de_la_superficie_1")) if kobo.get("informations_du_menage/unite_de_la_superficie_1") else None,
            fk_usage=int(kobo.get("informations_du_menage/usage")) if kobo.get("informations_du_menage/usage") else None,
            fk_usage_specifique=int(kobo.get("informations_du_menage/usage_specifique")) if kobo.get("informations_du_menage/usage_specifique") else None,
            fk_agent=fk_agent,
        )
        db.add(bien)
        db.flush()
        fk_bien = bien.id

        # 6. Insert into LocationBien (if the occupant is a locataire)
        if responsable_est_locataire:
            location_bien = LocationBien(
                fk_personne=fk_locataire,
                fk_bien=fk_bien,
                date_debut=None,
                date_fin=None,
                fk_agent=fk_agent,
                date_create=datetime.utcnow()
            )
            db.add(location_bien)
            db.flush()

        # 7. Insert additional Personnes (from information_sur_les_personnes)
        for personne in kobo.get("information_sur_les_personnes", []):
            new_personne = Personne(
                nom=personne.get("information_sur_les_personnes/nom_3"),
                postnom=personne.get("information_sur_les_personnes/post_nom_3"),
                prenom=personne.get("information_sur_les_personnes/prenom_3"),
                sexe=personne.get("information_sur_les_personnes/gr"),
                lieu_naissance=personne.get("information_sur_les_personnes/lieu_de_naissance_1"),
                date_naissance=personne.get("information_sur_les_personnes/date_de_naissance_1"),
                province_origine=personne.get("information_sur_les_personnes/province_d_origine"),
                district=None,
                territoire=personne.get("information_sur_les_personnes/territoire"),
                secteur=personne.get("information_sur_les_personnes/secteur"),
                village=personne.get("information_sur_les_personnes/village"),
                profession=personne.get("information_sur_les_personnes/profession_2"),
                type_piece_identite=personne.get("information_sur_les_personnes/type_de_piece_d_identite"),
                numero_piece_identite=personne.get("information_sur_les_personnes/numero_piece_d_identite"),
                nom_du_pere=personne.get("information_sur_les_personnes/nom_du_pere"),
                nom_de_la_mere=personne.get("information_sur_les_personnes/nom_de_la_mere"),
                etat_civil=personne.get("information_sur_les_personnes/etat_civil"),
                lieu_parente=personne.get("information_sur_les_personnes/lien_de_parente"),
                telephone=personne.get("information_sur_les_personnes/n_telphone"),
                adresse_mail=personne.get("information_sur_les_personnes/adresse_email_3"),
                nombre_enfant=int(personne.get("information_sur_les_personnes/nombre_d_enfant", 0)) if personne.get("information_sur_les_personnes/nombre_d_enfant") else None,
                niveau_etude=personne.get("information_sur_les_personnes/niveau_d_etude"),
                fk_nationalite=int(personne.get("information_sur_les_personnes/nationalite_3")) if personne.get("information_sur_les_personnes/nationalite_3") else None,
                fk_adresse=fk_adresse,
                fk_agent=fk_agent,
            )
            db.add(new_personne)

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
        return {"status": "error", "message": str(e)}


def create_user(user_data: UserCreate, db: Session):
    try:
        # Check if the user already exists
        existing_user = db.query(Utilisateur).filter(Utilisateur.login == user_data.login).first()
        if existing_user:
            raise HTTPException(status_code=400, detail="User with this login already exists")

        # Hash the password
        hashed_password = get_password_hash(user_data.password)

        # Create the new user
        new_user = Utilisateur(
            prenom=user_data.prenom,
            nom=user_data.nom,
            postnom=user_data.postnom,
            sexe=user_data.sexe,
            telephone=user_data.telephone,
            login=user_data.login,
            password=hashed_password,
            mail=user_data.mail,
            date_creat=datetime.datetime.now(datetime.timezone.utc),
        )
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        logger.info(f"User {user_data.login} created successfully")
        return new_user

    except Exception as e:
        db.rollback()
        logger.error(f"Failed to create user: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Failed to create user: {str(e)}")