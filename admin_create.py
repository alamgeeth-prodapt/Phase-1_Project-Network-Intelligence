from passlib.context import CryptContext

pwd_context = CryptContext(
    schemes=["bcrypt"],
    deprecated="auto"
)

def hash_passowrd(plain: str):
    return pwd_context.hash(plain)

def verify_password(plain: str, hashed: str):
    return pwd_context.verify(plain, hashed)
