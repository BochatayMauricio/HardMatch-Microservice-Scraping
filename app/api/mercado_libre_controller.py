from fastapi import APIRouter, Query, HTTPException
from typing import List
import logging

from app.api.query_normalizer import normalize_query
# Importamos los schemas y servicios necesarios
from app.schemas.product_schema import ProductSchema
from app.services.scrape_mercadoLibre import scrape_mercadolibre
from app.services.normalize_data import normalize_data

router = APIRouter(
    prefix="/mercadolibre",
    tags=["Mercado Libre"]
)

@router.get("/scrape-by-query", response_model=List[ProductSchema])
async def do_scrape_and_normalize(
    q: str = Query("Productos", description="Termino de busqueda"),
    max_pages: int = Query(1, ge=1, le=10, description="Cantidad de paginas a scrapear"),
    include_details: bool = Query(True, description="Extraer caracteristicas desde la pagina del producto"),
):
    """
    Endpoint maestro: Busca en ML, scrapea los resultados, 
    se los pasa a la IA para normalizar características y devuelve el modelo final.
    """
    normalized_query = normalize_query(q)
    if normalized_query != q:
        logging.info("Query normalizada: '%s' -> '%s'", q, normalized_query)
    logging.info(f"Iniciando flujo de scraping para: {normalized_query}")
    
    # 1. Delegamos al servicio de scraping (ahora usaría la versión con paginación)
    items_crudos = await scrape_mercadolibre(query=normalized_query, max_pages=max_pages, include_details=include_details)
    
    if not items_crudos:
        return []
        
    # 2. Delegamos al servicio de IA
    productos_finales = normalize_data(items_crudos)
    
    return productos_finales