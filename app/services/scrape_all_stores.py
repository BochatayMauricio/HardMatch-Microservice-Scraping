import asyncio
import logging
from typing import Dict, List

from app.services.scrape_compraGamer import scrape_compra_gamer
from app.services.scrape_fravega import scrape_fravega
from app.services.scrape_mercadolibre_selenium import scrape_mercadolibre
from app.services.scrape_venex import scrape_venex


STORE_NAMES: Dict[str, str] = {
    "mercadolibre": "Mercado Libre",
    "compragamer": "Compra Gamer",
    "fravega": "Fravega",
    "venex": "Venex",
}

async def scrape_all_stores_parallel(
    query: str,
    max_pages: int = 1,
    max_items=15,
    include_details_ml: bool = True,
) -> List[dict]:
    """Ejecuta scraping en las 4 tiendas en paralelo y devuelve items crudos combinados."""
    tasks_by_store = {
        "mercadolibre": asyncio.create_task(
            scrape_mercadolibre(query=query, max_pages=max_pages, max_items=max_items, include_details=include_details_ml, headless=True)
        ),
        "compragamer": asyncio.create_task(scrape_compra_gamer(query=query, max_pages=max_pages)),
        "fravega": asyncio.create_task(scrape_fravega(query=query, max_pages=max_pages)),
        "venex": asyncio.create_task(scrape_venex(query=query, max_pages=max_pages)),
    }

    results = await asyncio.gather(*tasks_by_store.values(), return_exceptions=True)

    combined_items: List[dict] = []
    seen_urls = set()

    for store_key, store_result in zip(tasks_by_store.keys(), results):
        store_name = STORE_NAMES.get(store_key, store_key)

        if isinstance(store_result, Exception):
            logging.error("Fallo scraping en %s: %s", store_name, str(store_result))
            continue

        if not store_result:
            logging.info("%s no devolvio resultados para el query actual", store_name)
            continue

        added_count = 0
        for item in store_result:
            if not isinstance(item, dict):
                continue

            item_url = str(item.get("url") or "").strip()
            dedupe_key = item_url or f"{store_key}:{item.get('titulo', '')}:{item.get('precio_actual', '')}"
            if dedupe_key in seen_urls:
                continue

            seen_urls.add(dedupe_key)
            item.setdefault("vendedor", store_name)
            item["tienda"] = store_name
            combined_items.append(item)
            added_count += 1

        logging.info("%s aporto %s productos", store_name, added_count)

    logging.info("Scraping paralelo finalizado. Total combinado: %s", len(combined_items))
    return combined_items
