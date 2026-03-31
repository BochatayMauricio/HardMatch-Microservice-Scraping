import logging
from typing import List
from fastapi import FastAPI, Query
from dotenv import load_dotenv

# 1. Cargamos variables de entorno ANTES de importar los servicios
load_dotenv()
logging.basicConfig(level=logging.INFO)

# 2. Importamos nuestros módulos locales
from app.schemas.product_schema import ProductSchema
from app.services.scrape_mercadoLibre import scrape_mercadolibre
from app.services.normalize_gemini import normalize_with_ia

# 3. Inicializamos FastAPI
app = FastAPI(title="Microservicio Scraping+IA HardMatch")

@app.get("/ping")
async def health_check():
    """Endpoint de prueba para verificar que el servicio está corriendo."""
    return {"status": "ok", "mensaje": "Microservicio operativo"}

@app.get("/scrape", response_model=List[ProductSchema])
async def do_scrape_and_normalize(q: str = Query("notebook gamer", description="Término de búsqueda")):
    """
    Endpoint maestro: Busca en ML, scrapea los primeros 3 resultados, 
    se los pasa a la IA para normalizar características y devuelve el modelo final.
    """
    # Delegamos al servicio de scraping
    items_crudos = await scrape_mercadolibre(query=q, limit=3)
    
    if not items_crudos:
        return []
        
    # Delegamos al servicio de IA
    productos_finales = await normalize_with_ia(items_crudos)
    
    return productos_finales