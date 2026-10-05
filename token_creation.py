from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
my_hash = pwd_context.hash("geeth")

print(my_hash)
# Output will look like: $2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQ/Yd.