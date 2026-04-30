import logging
from typing import List

from fastapi import APIRouter, Query

from app.api.query_normalizer import normalize_query
from app.schemas.product_schema import ProductSchema
from app.services.normalize_data import normalize_data
from app.services.scrape_fravega import scrape_fravega

router = APIRouter(prefix="/fravega", tags=["Fravega"])


@router.get("/scrape-by-query", response_model=List[ProductSchema])
async def scrape_and_normalize_fravega(
    q: str = Query("notebook", description="Termino de busqueda"),
    max_pages: int = Query(1, ge=1, le=15, description="Cantidad de paginas a scrapear"),
):
    """Scrapea Fravega por API de catalogo y normaliza para respuesta unificada."""
    normalized_query = normalize_query(q)
    if normalized_query != q:
        logging.info("Query normalizada: '%s' -> '%s'", q, normalized_query)
    logging.info("Iniciando scraping de Fravega para query: %s", normalized_query)

    raw_items = await scrape_fravega(query=normalized_query, max_pages=max_pages)
    if not raw_items:
        return []

    return normalize_data(raw_items)
