from fastapi import APIRouter, Query
from typing import List
import logging

# Importamos los schemas y servicios necesarios
from app.schemas.product_schema import ProductSchema
from app.services.scrape_mercadoLibre import scrape_mercadolibre
from app.services.normalize_gemini import normalize_with_ia

# Instanciamos el router. 
# Le agregamos un prefijo y etiquetas para que la documentación en /docs
router = APIRouter(
    prefix="/mercadolibre",
    tags=["Mercado Libre"]
)

@router.get("/scrape", response_model=List[ProductSchema])
async def do_scrape_and_normalize(q: str = Query("Productos", description="Término de búsqueda")):
    """
    Endpoint maestro: Busca en ML, scrapea los resultados, 
    se los pasa a la IA para normalizar características y devuelve el modelo final.
    """
    logging.info(f"Iniciando flujo de scraping para: {q}")
    
    # 1. Delegamos al servicio de scraping (ahora usaría la versión con paginación)
    items_crudos = await scrape_mercadolibre(query=q, max_pages=1) 
    
    if not items_crudos:
        return []
        
    # 2. Delegamos al servicio de IA
    productos_finales = await normalize_with_ia(items_crudos)
    
    return productos_finales