import logging
from typing import List

from fastapi import APIRouter, Query

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
    logging.info("Iniciando scraping de Fravega para query: %s", q)

    raw_items = await scrape_fravega(query=q, max_pages=max_pages)
    if not raw_items:
        return []

    return normalize_data(raw_items)
