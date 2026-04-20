import logging
from typing import List

from fastapi import APIRouter, Query

from app.schemas.product_schema import ProductSchema
from app.services.normalize_data import normalize_data
from app.services.scrape_compraGamer import scrape_compra_gamer

router = APIRouter(prefix="/compragamer", tags=["Compra Gamer"])


@router.get("/scrape-by-query", response_model=List[ProductSchema])
async def scrape_and_normalize_compragamer(
    q: str = Query("notebook", description="Termino de busqueda"),
    max_pages: int = Query(1, ge=1, le=10, description="Cantidad de paginas a scrapear"),
):
    """Scrapea Compra Gamer, normaliza y devuelve productos listos para el backend central."""
    logging.info("Iniciando scraping de Compra Gamer para query: %s", q)

    raw_items = await scrape_compra_gamer(query=q, max_pages=max_pages)
    if not raw_items:
        return []

    return normalize_data(raw_items)
