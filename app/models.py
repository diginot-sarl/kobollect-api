# app/models.py
from sqlalchemy import Column, Integer, String, Float, Text, DateTime, Boolean, ForeignKey, BigInteger, Date
from sqlalchemy.sql.sqltypes import NVARCHAR, NCHAR
from app.database import Base

# Table: access
class Access(Base):
    __tablename__ = "access"
    id = Column(Integer, primary_key=True, index=True)
    etat = Column(Integer, nullable=True)
    fk_utilisateur = Column(Integer, ForeignKey("utilisateur.id"), nullable=True)
    fk_droit = Column(Integer, ForeignKey("droit.id"), nullable=True)

# Table: adresse
class Adresse(Base):
    __tablename__ = "adresse"
    id = Column(Integer, primary_key=True, index=True)
    fk_avenue = Column(Integer, ForeignKey("avenue.id"), nullable=True)
    numero = Column(NVARCHAR(50), nullable=True)
    etat = Column(Boolean, nullable=True)
    fk_agent = Column(BigInteger, nullable=True)

# Table: avenue
class Avenue(Base):
    __tablename__ = "avenue"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    intitule = Column(String(50), nullable=True)
    etat = Column(Integer, nullable=True)
    lon = Column(Float(precision=18), nullable=True)
    lat = Column(Float(precision=18), nullable=True)
    shape = Column(String, nullable=True)  # varchar(max)
    fk_quartier = Column(Integer, ForeignKey("quartier.id"), nullable=True)
    date_create = Column(DateTime, nullable=True)

# Table: bien
class Bien(Base):
    __tablename__ = "bien"
    id = Column(Integer, primary_key=True, index=True)
    ref_bien = Column(String(50), nullable=True)
    coordinates = Column(String, nullable=True)  # varchar(max)
    coord_projected = Column(String, nullable=True)  # varchar(max)
    coord_corrige = Column(String, nullable=True)  # varchar(max)
    etat = Column(Integer, nullable=True)
    fk_parcelle = Column(Integer, ForeignKey("parcelle.id"), nullable=True)
    fk_nature_bien = Column(Integer, ForeignKey("nature_bien.id"), nullable=True)
    fk_unite = Column(Integer, ForeignKey("unite.id"), nullable=True)
    fk_usage = Column(Integer, ForeignKey("usage.id"), nullable=True)
    fk_usage_specifique = Column(BigInteger, ForeignKey("usage_specifique.id"), nullable=True)
    date_create = Column(DateTime, nullable=True)
    date_update = Column(DateTime, nullable=True)
    fk_agent = Column(Integer, nullable=True)

# Table: commune
class Commune(Base):
    __tablename__ = "commune"
    id = Column(Integer, primary_key=True, index=True)
    intitule = Column(String(50), nullable=False)
    etat = Column(Integer, nullable=True)
    lon = Column(Float(precision=18), nullable=True)
    lat = Column(Float(precision=18), nullable=True)
    shape = Column(Text, nullable=True)
    date_creat = Column(DateTime, nullable=True)
    agent_Creat = Column(Integer, nullable=True)
    fk_ville = Column(Integer, ForeignKey("ville.id"), nullable=True)

# Table: droit
class Droit(Base):
    __tablename__ = "droit"
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(50), nullable=True)
    intitule = Column(String, nullable=True)  # varchar(max)
    etat = Column(Integer, nullable=True)
    fk_module = Column(Integer, ForeignKey("module.id"), nullable=True)

# Table: filiation_membre
class FiliationMembre(Base):
    __tablename__ = "filiation_membre"
    id = Column(BigInteger, primary_key=True, index=True)
    intitule = Column(NVARCHAR(100), nullable=True)
    etat = Column(Boolean, nullable=True)

# Table: fonction
class Fonction(Base):
    __tablename__ = "fonction"
    id = Column(Integer, primary_key=True, index=True)
    intitule = Column(String(50), nullable=True)
    etat = Column(Integer, nullable=True)

# Table: Forme_juridique
class FormeJuridique(Base):
    __tablename__ = "Forme_juridique"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    intitule = Column(String(50), nullable=True)
    etat = Column(Integer, nullable=True)

# Table: groupe
class Groupe(Base):
    __tablename__ = "groupe"
    id = Column(Integer, primary_key=True, index=True)
    intitule = Column(String(100), nullable=True)
    etat = Column(Integer, nullable=True)
    description = Column(String(500), nullable=True)
    userCreat = Column(Integer, nullable=True)

# Table: groupe_droit
class GroupeDroit(Base):
    __tablename__ = "groupe_droit"
    id = Column(Integer, primary_key=True, index=True)
    fk_droit = Column(Integer, ForeignKey("droit.id"), nullable=True)
    fk_groupe = Column(Integer, ForeignKey("groupe.id"), nullable=True)

# Table: location_bien
class LocationBien(Base):
    __tablename__ = "location_bien"
    id = Column(BigInteger, primary_key=True, index=True)
    fk_personne = Column(BigInteger, ForeignKey("personne.id"), nullable=True)
    fk_bien = Column(Integer, ForeignKey("bien.id"), nullable=True)
    date_create = Column(DateTime, nullable=True)
    date_debut = Column(Date, nullable=True)
    date_fin = Column(Date, nullable=True)
    fk_agent = Column(BigInteger, nullable=True)

# Table: logs
class Logs(Base):
    __tablename__ = "logs"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    logs = Column(String, nullable=True)  # varchar(max)
    id_kobo = Column(Integer, nullable=True)
    data_json = Column(String, nullable=True)  # varchar(max)

# Table: membre_menage
class MembreMenage(Base):
    __tablename__ = "membre_menage"
    id = Column(BigInteger, primary_key=True, index=True)
    fk_menage = Column(BigInteger, ForeignKey("menage.id"), nullable=True)
    fk_personne = Column(BigInteger, ForeignKey("personne.id"), nullable=True)
    fk_filiation = Column(BigInteger, ForeignKey("filiation_membre.id"), nullable=True)

# Table: menage
class Menage(Base):
    __tablename__ = "menage"
    id = Column(BigInteger, primary_key=True, index=True)
    fk_personne = Column(BigInteger, ForeignKey("personne.id"), nullable=True)
    fk_bien = Column(Integer, ForeignKey("bien.id"), nullable=True)

# Table: module
class Module(Base):
    __tablename__ = "module"
    id = Column(Integer, primary_key=True, index=True)
    intitule = Column(String(50), nullable=True)
    etat = Column(Integer, nullable=True)
    userCreat = Column(Integer, nullable=True)

# Table: nationalite
class Nationalite(Base):
    __tablename__ = "nationalite"
    id = Column(Integer, primary_key=True, index=True)
    intitule = Column(String(50), nullable=True)
    pays = Column(String(50), nullable=True)
    etat = Column(Integer, nullable=True)

# Table: nature_bien
class NatureBien(Base):
    __tablename__ = "nature_bien"
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(50), nullable=True)
    intitule = Column(String(50), nullable=True)
    etat = Column(Integer, nullable=True)
    date_creat = Column(DateTime, nullable=True)
    agent_Creat = Column(Integer, nullable=True)

# Table: parcelle
class Parcelle(Base):
    __tablename__ = "parcelle"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    ref_parcelle = Column(String(50), nullable=True)
    numero_parcellaire = Column(String(50), nullable=True)
    fk_unite = Column(Integer, ForeignKey("unite.id"), nullable=True)
    longueur = Column(Float(precision=18), nullable=True)
    largeur = Column(Float(precision=18), nullable=True)
    superficie_calculee = Column(Float(precision=18), nullable=True)
    coordonnee_geographique = Column(String, nullable=True)  # varchar(max)
    coord_projected = Column(String, nullable=True)  # varchar(max)
    coord_corrige = Column(String, nullable=True)  # varchar(max)
    etat = Column(Integer, nullable=True)
    fk_proprietaire = Column(BigInteger, ForeignKey("personne.id"), nullable=True)
    fk_rang = Column(Integer, ForeignKey("rang.id"), nullable=True)
    date_create = Column(DateTime, nullable=True)
    date_update = Column(DateTime, nullable=True)
    fk_agent = Column(Integer, nullable=True)
    fk_adresse = Column(Integer, ForeignKey("adresse.id"), nullable=True)

# Table: personne
class Personne(Base):
    __tablename__ = "personne"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    nom = Column(String(100), nullable=True)
    postnom = Column(String(100), nullable=True)
    prenom = Column(String(100), nullable=True)
    sexe = Column(String(50), nullable=True)
    denomination = Column(String(100), nullable=True)
    sigle = Column(NCHAR(50), nullable=True)
    numero_impot = Column(String(50), nullable=True)
    rccm = Column(String(50), nullable=True)
    id_nat = Column(String(50), nullable=True)
    domaine_activite = Column(String(50), nullable=True)
    fk_type_personne = Column(Integer, ForeignKey("type_personne.id"), nullable=True)
    lieu_naissance = Column(String(100), nullable=True)
    date_naissance = Column(Date, nullable=True)
    province_origine = Column(String(100), nullable=True)
    district = Column(String(100), nullable=True)
    territoire = Column(String(100), nullable=True)
    secteur = Column(String(100), nullable=True)
    village = Column(String(100), nullable=True)
    etat = Column(Integer, nullable=True)
    fk_nationalite = Column(Integer, ForeignKey("nationalite.id"), nullable=True)
    profession = Column(String(100), nullable=True)
    type_piece_identite = Column(String(50), nullable=True)
    numero_piece_identite = Column(String(50), nullable=True)
    nom_du_pere = Column(String(100), nullable=True)
    nom_de_la_mere = Column(String(100), nullable=True)
    etat_civil = Column(String(50), nullable=True)
    lieu_parente = Column(String(100), nullable=True)
    telephone = Column(String(50), nullable=True)
    adresse_mail = Column(String(100), nullable=True)
    nombre_enfant = Column(Integer, nullable=True)
    niveau_etude = Column(String(50), nullable=True)
    date_create = Column(DateTime, nullable=True)
    date_update = Column(DateTime, nullable=True)
    fk_agent = Column(Integer, nullable=True)
    fk_adresse = Column(Integer, ForeignKey("adresse.id"), nullable=True)

# Table: province
class Province(Base):
    __tablename__ = "province"
    id = Column(Integer, primary_key=True, index=True)
    intitule = Column(String(100), nullable=True)
    etat = Column(Integer, nullable=True)
    lon = Column(Float(precision=18), nullable=True)
    lat = Column(Float(precision=18), nullable=True)
    date_creat = Column(DateTime, nullable=True)
    agent_Creat = Column(Integer, nullable=True)

# Table: quartier
class Quartier(Base):
    __tablename__ = "quartier"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    intitule = Column(String(50), nullable=True)
    etat = Column(Integer, nullable=True)
    lon = Column(Float(precision=18), nullable=True)
    lat = Column(Float(precision=18), nullable=True)
    shape = Column(String, nullable=True)  # varchar(max)
    date_creat = Column(DateTime, nullable=True)
    fk_commune = Column(Integer, ForeignKey("commune.id"), nullable=True)

# Table: rang
class Rang(Base):
    __tablename__ = "rang"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    code = Column(String(50), nullable=True)
    intitule = Column(String(50), nullable=True)
    etat = Column(Integer, nullable=True)
    date_creat = Column(DateTime, nullable=True)
    agent_Creat = Column(Integer, nullable=True)

# Table: site
class Site(Base):
    __tablename__ = "site"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    intitule = Column(String(50), nullable=True)
    etat = Column(Integer, nullable=True)
    date_creat = Column(DateTime, nullable=True)
    agent_Creat = Column(Integer, nullable=True)
    fk_ville = Column(Integer, ForeignKey("ville.id"), nullable=True)
    code_site = Column(String(50), nullable=True)

# Table: type_personne
class TypePersonne(Base):
    __tablename__ = "type_personne"
    id = Column(Integer, primary_key=True, index=True)
    intitule = Column(NVARCHAR(100), nullable=True)

# Table: Type_piece_identite
class TypePieceIdentite(Base):
    __tablename__ = "Type_piece_identite"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    intitule = Column(String(50), nullable=True)
    etat = Column(Integer, nullable=True)

# Table: unite
class Unite(Base):
    __tablename__ = "unite"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    code = Column(String(50), nullable=True)
    sigle = Column(String(150), nullable=True)
    intitule = Column(String(50), nullable=True)
    etat = Column(Integer, nullable=True)

# Table: usage
class Usage(Base):
    __tablename__ = "usage"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    intitule = Column(NVARCHAR(100), nullable=True)
    etat = Column(Integer, nullable=True)

# Table: usage_specifique
class UsageSpecifique(Base):
    __tablename__ = "usage_specifique"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    intitule = Column(NVARCHAR(100), nullable=True)
    etat = Column(Boolean, nullable=True)

# Table: utilisateur
class Utilisateur(Base):
    __tablename__ = "utilisateur"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    prenom = Column(String(50), nullable=True)
    nom = Column(String(50), nullable=True)
    postnom = Column(String(50), nullable=True)
    sexe = Column(String(50), nullable=True)
    telephone = Column(String(50), nullable=True)
    login = Column(String(50), nullable=True)
    password = Column(String(255), nullable=True)
    mail = Column(String(50), nullable=True)
    etat = Column(Integer, nullable=True)
    date_creat = Column(DateTime, nullable=True)
    status = Column(Integer, nullable=True)
    fk_fonction = Column(Integer, ForeignKey("fonction.id"), nullable=True)
    fk_site = Column(Integer, ForeignKey("site.id"), nullable=True)
    fk_agent_creat = Column(Integer, nullable=True)

# Table: ville
class Ville(Base):
    __tablename__ = "ville"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    intitule = Column(String(50), nullable=True)
    etat = Column(Integer, nullable=True)
    lon = Column(Float(precision=18), nullable=True)
    lat = Column(Float(precision=18), nullable=True)
    shape = Column(Text, nullable=True)
    fk_province = Column(Integer, ForeignKey("province.id"), nullable=True)