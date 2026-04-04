import logging
import os
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
from google import genai

# 1. Cargamos variables de entorno ANTES de importar los servicios
load_dotenv()
logging.basicConfig(level=logging.INFO)

# 2. Importamos nuestros módulos locales
from app.api import mercado_libre_controller

# 3. Inicializamos FastAPI
app = FastAPI(title="Microservicio Scraping+IA HardMatch")

# 4. Registramos los routers de nuestros controladores
app.include_router(mercado_libre_controller.router)

@app.get("/ping")
async def health_check():
    """Endpoint de prueba para verificar que el servicio está corriendo."""
    return {"status": "ok", "mensaje": "Microservicio operativo"}

@app.get("/test-ia")
async def test_ia():
    """
    Endpoint administrativo para listar los modelos de Gemini disponibles 
    en la API Key actual.
    - filter_flash: Si es True, solo devuelve los modelos rápidos de la familia 'flash'.
    """
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None
    
    try:
        modelos_disponibles = []
        
        # Consultamos la lista de modelos a Google
        for m in client.models.list():
            
            modelos_disponibles.append({
                "nombre": m.name,
                "descripcion": getattr(m, 'description', 'Sin descripción'),
                "version": getattr(m, 'version', 'N/A')
            })
            
        logging.info(f"Se encontraron {len(modelos_disponibles)} modelos.")
        return {"status": "ok", "total": len(modelos_disponibles), "modelos": modelos_disponibles}
        
    except Exception as e:
        logging.error(f"Error al consultar modelos de Gemini: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error consultando la API de Google: {str(e)}")
