from pydantic import BaseModel

class UserBase(BaseModel):
    email:str
    username: str

class UserCreate(UserBase):
    password: str

class UserLogin(UserBase):
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str
