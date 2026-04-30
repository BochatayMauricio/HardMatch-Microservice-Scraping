import logging
from typing import List

from fastapi import APIRouter, Query

from app.api.query_normalizer import normalize_query
from app.schemas.product_schema import ProductSchema
from app.services.normalize_data import normalize_data
from app.services.scrape_all_stores import scrape_all_stores_parallel

router = APIRouter(prefix="/all-stores", tags=["All Stores"])

@router.get("/scrape-by-query", response_model=List[ProductSchema])
async def scrape_all_stores_by_query(
    q: str = Query("notebook", description="Termino de busqueda"),
    max_pages: int = Query(1, ge=1, le=10, description="Cantidad de paginas por tienda"),
    include_details_ml: bool = Query(
        False,
        description="Si es true, Mercado Libre tambien extrae caracteristicas desde detalle",
    ),
):
    """Consulta las 4 tiendas en paralelo y devuelve un listado unificado normalizado."""
    normalized_query = normalize_query(q)
    if normalized_query != q:
        logging.info("Query normalizada: '%s' -> '%s'", q, normalized_query)
    logging.info("Iniciando scraping paralelo 4 tiendas para query: %s", normalized_query)

    items_crudos = await scrape_all_stores_parallel(
        query=normalized_query,
        max_pages=max_pages,
        include_details_ml=include_details_ml,
    )

    if not items_crudos:
        return []

    return normalize_data(items_crudos)
