import logging
from typing import List

from fastapi import APIRouter, Query

from app.schemas.product_schema import ProductSchema
from app.services.normalize_data import normalize_data
from app.services.scrape_venex import scrape_venex

router = APIRouter(prefix="/venex", tags=["Venex"])


@router.get("/scrape-by-query", response_model=List[ProductSchema])
async def scrape_and_normalize_venex(
    q: str = Query("notebook", description="Termino de busqueda"),
    max_pages: int = Query(1, ge=1, le=10, description="Cantidad de paginas a scrapear"),
):
    """Scrapea Venex por busqueda paginada y normaliza el payload de salida."""
    logging.info("Iniciando scraping de Venex para query: %s", q)

    raw_items = await scrape_venex(query=q, max_pages=max_pages)
    if not raw_items:
        return []

    return normalize_data(raw_items)
