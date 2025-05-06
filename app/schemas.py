from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
from datetime import datetime

class UserCreate(BaseModel):
    login: str
    prenom: str
    nom: str
    postnom: Optional[str] = None
    mail: EmailStr
    password: str
    telephone: Optional[str] = None
    sexe: Optional[str] = None
    photo_url: Optional[str] = None
    code_chasuble: Optional[str] = None
    
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
