import os
from dotenv import load_dotenv

# Carga las variables del archivo .env al entorno de Python
load_dotenv()

# Ahora puedes acceder a tus variables de forma segura
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
APP_ENV = os.getenv("APP_ENV", "development") # "development" es el valor por defecto si no lo encuentra