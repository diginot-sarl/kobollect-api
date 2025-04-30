from app.db import get_mssql_connection
import logging
from app.auth import get_password_hash
from fastapi import HTTPException

logger = logging.getLogger(__name__)

def process_kobo_data(payload: dict):
    kobo: dict = payload["received_data"]
    record_id = kobo["_id"]

    conn = get_mssql_connection()
    cursor = conn.cursor()

    try:
        # Vérifier si l'ID existe déjà
        cursor.execute("SELECT 1 FROM log WHERE id_kobo = ?", (record_id,))
        if cursor.fetchone():
            return {"status": "duplicate", "message": f"Donnée avec _id {record_id} déjà existante."}

        # Démarrer transaction
        conn.autocommit = False
        
        # 0. Récupérer l'ID de l'agent
        fk_agent = 1
        fk_parcelle = None
        responsable_est_locataire = True if kobo.get("informations_de_l_occupant/occupant_est_locataire_ou_proprietaire_2") == "locataire" else False
        
        
        # 1. Insérer l'Adresse
        cursor.execute("""
            INSERT INTO adresse (fk_avenue, numero, fk_agent)
            OUTPUT INSERTED.id
            VALUES (?, ?, ?)           
        """, (
            kobo["adresse_de_la_parcelle/avenue"],
            kobo["adresse_de_la_parcelle/numero_parcellaire"],
            fk_agent
        ))
        fk_adresse = cursor.fetchone()[0]
        
        
        # 2. Insérer l'Occupant (Propriétaire/Locataire) dans la table Personne
        cursor.execute("""
            INSERT INTO personne (
                nom, postnom, prenom, sexe, fk_type_personne, lieu_naissance, date_naissance, province_origine, district, territoire, secteur, village, fk_nationalite, profession, type_piece_identiter, numero_piece_identite, nom_du_pere, nom_de_la_mere, etat_civil, lieu_parente, telephone, adresse_email, nombre_enfant, niveau_etude, denomination, sigle, numero_d_impot, rccm, id_nat, forme_juridique, domaine_d_activite, fk_adresse, fk_agent
            )
            OUTPUT INSERTED.id
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)         
        """, (
            kobo.get("informations_de_l_occupant/nom"),
            kobo.get("informations_de_l_occupant/post_nom"),
            kobo.get("informations_de_l_occupant/prenom"),
            kobo.get("informations_de_l_occupant/genre"),
            kobo.get("informations_de_l_occupant/type_de_personne"),
            kobo.get("informations_de_l_occupant/lieu_de_naissance"),
            kobo.get("informations_de_l_occupant/date_de_naissance"),
            None,
            None,
            None,
            None,
            None,
            kobo.get("informations_de_l_occupant/nationalite"),
            kobo.get("informations_de_l_occupant/profession"),
            None,
            None,
            None,
            None,
            None,
            None,
            kobo.get("informations_de_l_occupant/n_telephone"),
            kobo.get("informations_de_l_occupant/adresse_email"),
            kobo.get("informations_de_l_occupant/nombre_d_enfants"),
            kobo.get("informations_de_l_occupant/niveau_d_etudes"),
            kobo.get("informations_de_l_occupant/denomination"),
            kobo.get("informations_de_l_occupant/sigle"),
            kobo.get("informations_de_l_occupant/numero_d_impot"),
            kobo.get("informations_de_l_occupant/rccm"),
            kobo.get("informations_de_l_occupant/id_nat"),
            kobo.get("informations_de_l_occupant/forme_juridique"),
            kobo.get("informations_de_l_occupant/domaine_d_activite"),
            fk_adresse,
            fk_agent
        ))
        
        if kobo["informations_de_l_occupant/occupant_est_locataire_ou_proprietaire_2"] == "bailleur":
            # Si l'occupant est un bailleur
            fk_proprietaire = cursor.fetchone()[0]
        else:
            # Si l'occupant est un locataire
            fk_locataire = cursor.fetchone()[0]
        
        
        # 3. Insérer Proprietaire dans la table Personne (s'il n'habite pas dans la parcelle)
        if kobo.get("informations_de_l_occupant/le_proprietaire_habite_t_il_dans_la_parcelle") == 'non':
            cursor.execute("""
                INSERT INTO personne (
                    nom, postnom, prenom, sexe, fk_type_personne, lieu_naissance, date_naissance, province_origine, district, territoire, secteur, village, fk_nationalite, profession, type_piece_identiter, numero_piece_identite, nom_du_pere, nom_de_la_mere, etat_civil, lieu_parente, telephone, adresse_email, nombre_enfant, niveau_etude, denomination, sigle, numero_d_impot, rccm, id_nat, forme_juridique, domaine_d_activite, fk_adresse, fk_agent
                )
                OUTPUT INSERTED.id
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)         
            """, (
                kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nom_2"),
                kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/post_nom_2"),
                kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/prenom_2"),
                kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/genre_2"),
                kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/tp"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/nationalite_2"),
                None,
                None,
                None,
                None,
                None,
                None,
                kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/n_telephone_2"),
                kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/adresse_email_2"),
                None,
                None,
                None,
                kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/denomination_2"),
                kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/sigle_2"),
                kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/numero_d_impot_2"),
                kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/rccm_2"),
                kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/id_nat_2"),
                kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/forme_juridique_2"),
                kobo.get("informations_du_proprietaire_de_la_parcelle_si_le_proprietaire_habite_t_il_dans_la_parcelle_non/domaine_d_activite_2"),
                fk_adresse,
                fk_agent
            ))
            fk_proprietaire = cursor.fetchone()[0]
        
        
        # 4. Insérer la Parcelle
        cursor.execute("""
            INSERT INTO parcelle (
                ref_parcelle, numero_parcellaire, fk_unite, longueur, largeur, superficie_calculee, coordonnee_geographique, fk_rang, fk_proprietaire, fk_adresse, fk_agent
            )
            OUTPUT INSERTED.id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)           
        """, (
            None,
            kobo.get("adresse_de_la_parcelle/numero_parcellaire"),
            kobo.get("adresse_de_la_parcelle/Unit"),
            kobo.get("adresse_de_la_parcelle/longueur"),
            kobo.get("adresse_de_la_parcelle/largeur"),
            kobo.get("adresse_de_la_parcelle/calculation"),
            kobo.get("adresse_de_la_parcelle/coordonne_geographique"),
            kobo.get("adresse_de_la_parcelle/rang"),
            fk_proprietaire,
            fk_adresse,
            fk_agent
        ))
        fk_parcelle = cursor.fetchone()[0]
        
        
        # 5. Insérer le Bien
        cursor.execute("""
            INSERT INTO bien (
                ref_bien, coordinates, fk_parcelle , fk_nature_bien, fk_unite, fk_usage, fk_usage_specifique, fk_agent
            )
            OUTPUT INSERTED.id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)           
        """, (
            kobo.get("informations_du_menage/numero_bien"),
            kobo.get("informations_du_menage/coordonnee_geographique"),
            fk_parcelle,
            kobo.get("informations_du_menage/nature"),
            kobo.get("informations_du_menage/unite_de_la_superficie_1"),
            kobo.get("informations_du_menage/usage"),
            kobo.get("informations_du_menage/usage_specifique"),
            fk_agent
        ))
        fk_bien = cursor.fetchone()[0]
        
        
        # 6. Insérer la Locataire_Bien
        if responsable_est_locataire:
            cursor.execute("""
                INSERT INTO location_bien (
                    fk_personne, fk_bien, date_debut, date_fin, fk_agent
                )
                VALUES (?, ?, ?, ?, ?)           
            """, (
                fk_locataire,
                fk_bien,
                None,
                None,
                fk_agent
            ))
            
                
        # 7. Insérer les Personnes
        for personne in kobo.get("information_sur_les_personnes", []):
            cursor.execute("""
                INSERT INTO personne (
                    nom, postnom, prenom, sexe, lieu_naissance, date_naissance, province_origine, district, territoire, secteur, village, profession, type_piece_identiter, numero_piece_identite, nom_du_pere, nom_de_la_mere, etat_civil, lieu_parente, telephone, adresse_email, nombre_enfant, niveau_etude, fk_nationalite, fk_adresse, fk_agent
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                personne.get("information_sur_les_personnes/nom_3"),
                personne.get("information_sur_les_personnes/post_nom_3"),
                personne.get("information_sur_les_personnes/prenom_3"),
                personne.get("information_sur_les_personnes/gr"),
                personne.get("information_sur_les_personnes/lieu_de_naissance_1"),
                personne.get("information_sur_les_personnes/date_de_naissance_1"),
                personne.get("information_sur_les_personnes/province_d_origine"),
                None,
                personne.get("information_sur_les_personnes/territoire"),
                personne.get("information_sur_les_personnes/secteur"),
                personne.get("information_sur_les_personnes/village"),
                personne.get("information_sur_les_personnes/profession_2"),
                personne.get("information_sur_les_personnes/type_de_piece_d_identite"),
                personne.get("information_sur_les_personnes/numero_piece_d_identite"),
                personne.get("information_sur_les_personnes/nom_du_pere"),
                personne.get("information_sur_les_personnes/nom_de_la_mere"),
                personne.get("information_sur_les_personnes/etat_civil"),
                personne.get("information_sur_les_personnes/lien_de_parente"),
                personne.get("information_sur_les_personnes/n_telphone"),
                personne.get("information_sur_les_personnes/adresse_email_3"),
                int(personne.get("information_sur_les_personnes/nombre_d_enfant", 0)),
                personne.get("information_sur_les_personnes/niveau_d_etude"),
                personne.get("information_sur_les_personnes/nationalite_3"),
                fk_adresse,
                fk_agent
            ))
 

        conn.commit()
        logger.info(f"Données insérées pour l'entrée _id={record_id}")
        return {"status": "success", "message": f"Données insérées pour l'entrée _id={record_id}"}

    except Exception as e:
        conn.rollback()
        logger.error(f"Erreur lors de l'insertion des données _id={record_id} : {str(e)}")
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()


def create_user(user_data: dict):
    conn = get_mssql_connection()
    cursor = conn.cursor()
    try:
        # Hash the password
        hashed_password = get_password_hash(user_data["password"])

        # Insert the new user into the users table
        cursor.execute("""
            INSERT INTO users (username, last_name, middle_name, first_name, email, password, matricule, disabled)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0)
        """, (
            user_data["username"],
            user_data["last_name"],
            user_data["middle_name"],
            user_data["first_name"],
            user_data["email"],
            hashed_password,
            user_data["matricule"],
        ))

        conn.commit()
        logger.info(f"User {user_data['username']} created successfully")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to create user: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Failed to create user: {str(e)}")
    finally:
        cursor.close()
        conn.close()
        
        