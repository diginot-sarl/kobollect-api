from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
from datetime import datetime

class UserCreate(BaseModel):
    login: str
    prenom: str
    nom: str
    postnom: Optional[str] = None
    mail: Optional[str] = None
    password: Optional[str] = None
    telephone: Optional[str] = None
    sexe: Optional[str] = None
    photo_url: Optional[str] = None
    code_chasuble: Optional[str] = None
    fk_groupe: Optional[int] = None
    
class User(BaseModel):
    id: int
    prenom: Optional[str] = None
    nom: Optional[str] = None
    postnom: Optional[str] = None
    sexe: Optional[str] = None
    telephone: Optional[str] = None
    login: Optional[str] = None
    mail: Optional[str] = None
    etat: Optional[int] = None
    date_creat: Optional[datetime] = None
    status: Optional[int] = None
    fk_fonction: Optional[int] = None
    fk_site: Optional[int] = None
    fk_agent_creat: Optional[int] = None
    photo_url: Optional[str] = None
    code_chasuble: Optional[str] = None

    class Config:
        orm_mode = True

class UserOut(BaseModel):
    id: int
    prenom: Optional[str] = None
    nom: Optional[str] = None
    postnom: Optional[str] = None
    sexe: Optional[str] = None
    telephone: Optional[str] = None
    login: Optional[str] = None
    mail: Optional[str] = None
    etat: Optional[int] = None
    date_create: Optional[datetime] = None
    status: Optional[int] = None
    fk_fonction: Optional[int] = None
    fk_site: Optional[int] = None
    fk_agent_creat: Optional[int] = None
    photo_url: Optional[str] = None
    code_chasuble: Optional[str] = None

    class Config:
        orm_mode = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class Token(BaseModel):
    access_token: str
    token_type: str

class UserSchema(BaseModel):
    id: int
    prenom: Optional[str]
    nom: Optional[str]
    postnom: Optional[str]
    sexe: Optional[str]
    telephone: Optional[str]
    login: Optional[str]
    mail: Optional[str]
    etat: Optional[int]
    date_creat: Optional[str]
    status: Optional[int]
    fk_fonction: Optional[int]
    fk_site: Optional[int]
    fk_agent_creat: Optional[int]

    class Config:
        orm_mode = True

class PaginatedUserResponse(BaseModel):
    data: List[UserOut]
    total: int
    page: int
    page_size: int

class TeamCreate(BaseModel):
    intitule: str
    fk_quartier: int
    
class AssignUserTeams(BaseModel):
    user_id: int
    team_ids: list[int]
    
    
class UserUpdate(BaseModel):
    login: Optional[str] = None
    nom: Optional[str] = None
    postnom: Optional[str] = None
    prenom: Optional[str] = None
    mail: Optional[str] = None
    code_chasuble: Optional[str] = None
    photo_url: Optional[str] = None
    sexe: Optional[str] = None
    telephone: Optional[str] = None
    fk_groupe: Optional[int] = None

class ModuleCreate(BaseModel):
    intitule: str
    userCreat: Optional[int] = None

class ModuleUpdate(BaseModel):
    intitule: Optional[str] = None

class ModuleOut(BaseModel):
    id: int
    intitule: str
    userCreat: Optional[int] = None

    class Config:
        orm_mode = True

class PaginatedModuleResponse(BaseModel):
    data: List[ModuleOut]
    total: int
    page: int
    page_size: int

class GroupeCreate(BaseModel):
    intitule: str
    description: Optional[str] = None
    userCreat: Optional[int] = None

class GroupeUpdate(BaseModel):
    intitule: Optional[str] = None
    description: Optional[str] = None

class GroupeOut(BaseModel):
    id: int
    intitule: str
    description: Optional[str] = None
    userCreat: Optional[int] = None
    droit_ids: List[int] = []

    class Config:
        orm_mode = True

class PaginatedGroupeResponse(BaseModel):
    data: List[GroupeOut]
    total: int
    page: int
    page_size: int

class DroitCreate(BaseModel):
    code: str
    intitule: str
    fk_module: int

class DroitUpdate(BaseModel):
    code: Optional[str] = None
    intitule: Optional[str] = None
    fk_module: Optional[int] = None

class DroitOut(BaseModel):
    id: int
    code: str
    intitule: str
    fk_module: int

    class Config:
        orm_mode = True

class DroitModuleOut(BaseModel):
    id: int
    intitule: str
class PaginatedDroitResponse(BaseModel):
    data: List[DroitOut]
    total: int
    page: int
    page_size: int
    module: DroitModuleOut

class AssignDroitsToEntity(BaseModel):
    droit_ids: List[int]

class AssignDroitsToUser(BaseModel):
    user_id: int
    droit_ids: List[int]

class UpdatePassword(BaseModel):
    new_password: str

class ProprietaireResponse(BaseModel):
    id: Optional[int]
    nom: Optional[str]
    postnom: Optional[str]
    prenom: Optional[str]
    date_naissance: Optional[str]
    sexe: Optional[str]
    etat_civil: Optional[str]
    profession: Optional[str]
    niveau_etude: Optional[str]
    lieu_naissance: Optional[str]
    nationalite: Optional[str]
    telephone: Optional[str]
    lien_parente: Optional[str]

class MembreResponse(BaseModel):
    id: Optional[int]
    nom: Optional[str]
    postnom: Optional[str]
    prenom: Optional[str]
    date_naissance: Optional[str]
    sexe: Optional[str]
    lien_parente: Optional[str]
    etat_civil: Optional[str]
    profession: Optional[str]
    niveau_etude: Optional[str]
    lieu_naissance: Optional[str]
    nationalite: Optional[str]
    telephone: Optional[str]

class AdresseResponse(BaseModel):
    commune: Optional[str]
    quartier: Optional[str]
    avenue: Optional[str]
    numero: Optional[str]
    rang: Optional[str]

class MenageResponse(BaseModel):
    id: int
    proprietaire: ProprietaireResponse
    membres: List[MembreResponse]
    adresse: AdresseResponse

class MenagesResponse(BaseModel):
    data: List[MenageResponse]
    total: int
    page: int
    page_size: int






