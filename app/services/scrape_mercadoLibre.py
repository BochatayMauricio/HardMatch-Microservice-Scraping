import logging
import asyncio
import random
from typing import List, Dict
import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import quote
# Carga las variables del archivo .env al entorno de Python
load_dotenv()

DETAIL_CONCURRENCY = 5
HEADER_ROTATION_EVERY = 8
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]


def _parse_product_features(html: str) -> List[Dict[str, str]]:
    """
    Extrae caracteristicas desde la pagina de detalle del producto.
    """
    soup = BeautifulSoup(html, "html.parser")
    features: List[Dict[str, str]] = []
    seen = set()

    for row in soup.select(".ui-pdp-specs__table tr, .andes-table__body tr, .andes-table tr"):
        key_el = row.select_one("th") or row.select_one(".andes-table__header")
        val_el = row.select_one("td") or row.select_one(".andes-table__column")
        if not key_el or not val_el:
            continue
        key = key_el.get_text(strip=True)
        value = val_el.get_text(strip=True)
        if key and value and (key, value) not in seen:
            features.append({"keyword": key, "value": value})
            seen.add((key, value))

    for item in soup.select(".ui-pdp-specs__list li, .ui-pdp-specs__attributes li"):
        key_el = item.select_one("span.ui-pdp-specs__attribute-label")
        val_el = item.select_one("span.ui-pdp-specs__attribute-value")
        if not key_el or not val_el:
            continue
        key = key_el.get_text(strip=True)
        value = val_el.get_text(strip=True)
        if key and value and (key, value) not in seen:
            features.append({"keyword": key, "value": value})
            seen.add((key, value))

    return features


async def _fetch_product_features(
    session: aiohttp.ClientSession,
    url: str,
    headers: Dict[str, str],
    semaphore: asyncio.Semaphore,
) -> List[Dict[str, str]]:
    if not url:
        return []

    async with semaphore:
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    logging.warning(f"Error {resp.status} al acceder a detalle: {url}")
                    return []
                html = await resp.text()
        except Exception as e:
            logging.error(f"Fallo en detalle de producto: {str(e)}")
            return []

    return _parse_product_features(html)


def _build_headers(user_agent: str) -> Dict[str, str]:
    return {
        "User-Agent": user_agent,
        "Accept-Language": "es-AR,es;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Referer": "https://www.mercadolibre.com.ar/",
    }


async def scrape_mercadolibre(
    query: str,
    max_pages: int = 1,
    include_details: bool = False,
) -> List[dict]:
    """
    Scrapea múltiples páginas de Mercado Libre por texto de búsqueda.
    max_pages: Cantidad de páginas a scrapear (cada página trae ~50 productos).
    """
    # 1. Formateo de URL Universal y seguro
    # Reemplazamos espacios por guiones y codificamos caracteres especiales
    query_formatted = quote(query.strip().replace(" ", "-"))
    base_url = f"https://listado.mercadolibre.com.ar/{query_formatted}"
    
    request_count = 0
    current_headers = _build_headers(random.choice(USER_AGENTS))
    
    items_crudos = []
    
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(DETAIL_CONCURRENCY)
        for page in range(max_pages):
            offset = 1 + (page * 50)
            url = base_url if page == 0 else f"{base_url}_Desde_{offset}_NoIndex_True"
            
            logging.info(f"Scrapeando página {page + 1}/{max_pages} - URL: {url}")
            
            if request_count % HEADER_ROTATION_EVERY == 0:
                current_headers = _build_headers(random.choice(USER_AGENTS))

            try:
                async with session.get(url, headers=current_headers) as resp:
                    if resp.status != 200:
                        logging.warning(f"Error {resp.status} al acceder a ML en la pág {page + 1}")
                        break # Salimos del loop si ML nos bloquea o hay un error de red
                    
                    html = await resp.text()
            except Exception as e:
                logging.error(f"Fallo en la petición HTTP: {str(e)}")
                break
                
            soup = BeautifulSoup(html, 'html.parser')
            # Selector de los contenedores de publicaciones
            results = soup.select('li.ui-search-layout__item')
            
            if not results:
                logging.info("No se encontraron más productos. Fin de la búsqueda.")
                break
                
            page_items: List[dict] = []
            for el in results:
                # 3. Extracción robusta de los datos
                title_el = el.select_one('h2.ui-search-item__title') or el.select_one('.poly-component__title')
                current_price_el = el.select_one('.poly-price__current .andes-money-amount__fraction') or el.select_one('.andes-money-amount__fraction')
                previous_price_el = (
                    el.select_one('.poly-price__previous .andes-money-amount__fraction')
                    or el.select_one('.andes-money-amount--previous .andes-money-amount__fraction')
                    or el.select_one('.poly-price__original .andes-money-amount__fraction')
                )
                seller_el = el.select_one('.poly-component__seller') or el.select_one('.ui-search-official-store-label')
                installments_el = el.select_one('.poly-price__installments') or el.select_one('.ui-search-installments')
                url_el = el.select_one('a')
                
                if not title_el or not current_price_el or not url_el:
                    continue
                    
                precio_actual = current_price_el.get_text(strip=True).replace('.', '')
                if previous_price_el:
                    precio_anterior = previous_price_el.get_text(strip=True).replace('.', '')
                else:
                    precio_anterior = None
                
                page_items.append({
                    "titulo": title_el.get_text(strip=True),
                    "precio_actual": precio_actual,
                    "precio_anterior": precio_anterior,
                    "vendedor": seller_el.get_text(strip=True).replace('por ', '') if seller_el else "Mercado Libre",
                    "metodo_pago": installments_el.get_text(strip=True) if installments_el else "No especificado",
                    "url": url_el.get('href', ''),
                })

            if include_details and page_items:
                tasks = [
                    _fetch_product_features(session, item["url"], current_headers, semaphore)
                    for item in page_items
                ]
                features_list = await asyncio.gather(*tasks)
                for item, features in zip(page_items, features_list):
                    if features:
                        item["caracteristicas"] = features

            items_crudos.extend(page_items)
            
            # 4. Rate Limiting (Pausa entre páginas para evitar bloqueos)
            if page < max_pages - 1:
                await asyncio.sleep(2.5) 
            request_count += 1

    logging.info(f"Scraping finalizado. Se extrajeron {len(items_crudos)} productos crudos.")

    return items_crudos