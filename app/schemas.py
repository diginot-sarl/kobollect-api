from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime

# Pydantic model for user creation
class UserCreate(BaseModel):
    username: str
    last_name: str
    middle_name: str | None = None
    first_name: str
    email: EmailStr
    password: str
    matricule: str
    
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

    class Config:
        orm_mode = True

class Token(BaseModel):
    access_token: str
    token_type: str