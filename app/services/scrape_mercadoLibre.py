import asyncio
import logging
import random
import re
from typing import Dict, List
from urllib.parse import quote, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from app.services.http_client import ResilientScraperClient

DETAIL_CONCURRENCY = 4
PAGE_SLEEP_MIN_SECONDS = 1.8
PAGE_SLEEP_MAX_SECONDS = 4.0
ML_REFERER = "https://www.mercadolibre.com.ar/"

BLOCK_HINTS = (
    "captcha",
    "verifica que no eres un robot",
    "acceso denegado",
    "are you human",
)

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


def _normalize_price(raw_value: str) -> str:
    return re.sub(r"[^\d]", "", raw_value or "")


def _clean_product_url(raw_url: str) -> str:
    if not raw_url:
        return ""
    parsed = urlsplit(raw_url)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def _extract_image_url(element) -> str:
    image = element.select_one("img") if element else None
    if not image:
        return ""

    for key in ("src", "data-src", "data-lazy-src", "data-zoom", "srcset"):
        value = image.get(key)
        if not value:
            continue

        if key == "srcset":
            first_candidate = value.split(",", 1)[0].strip().split(" ", 1)[0].strip()
            if first_candidate:
                return first_candidate

        if isinstance(value, str) and value.strip():
            return value.strip()

    return ""


def _looks_like_block_page(html: str) -> bool:
    lowered = html.lower()
    return any(hint in lowered for hint in BLOCK_HINTS)


async def _fetch_product_features(
    client: ResilientScraperClient,
    url: str,
    semaphore: asyncio.Semaphore,
) -> List[Dict[str, str]]:
    if not url:
        return []

    async with semaphore:
        html = await client.get_text(url, referer=ML_REFERER)
        if not html:
            return []
        if _looks_like_block_page(html):
            logging.warning("Se detecto pagina anti-bot en detalle de producto: %s", url)
            return []

    return _parse_product_features(html)


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

    items_crudos = []

    async with ResilientScraperClient(min_delay_seconds=1.2, max_delay_seconds=3.6) as client:
        semaphore = asyncio.Semaphore(DETAIL_CONCURRENCY)
        for page in range(max_pages):
            offset = 1 + (page * 50)
            url = base_url if page == 0 else f"{base_url}_Desde_{offset}_NoIndex_True"

            logging.info(f"Scrapeando página {page + 1}/{max_pages} - URL: {url}")

            html = await client.get_text(url, referer=ML_REFERER)
            if not html:
                logging.warning("No se pudo obtener la pagina %s de Mercado Libre", page + 1)
                break

            if _looks_like_block_page(html):
                logging.warning("Mercado Libre devolvio una pagina anti-bot. Se corta el scraping.")
                break
                
            soup = BeautifulSoup(html, 'html.parser')
            # Selector de los contenedores de publicaciones
            results = soup.select('li.ui-search-layout__item')

            if not results:
                logging.info("No se encontraron más productos. Fin de la búsqueda.")
                break

            page_items: List[dict] = []
            seen_page_urls = set()
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

                product_url = _clean_product_url(url_el.get('href', ''))
                if not product_url or product_url in seen_page_urls:
                    continue

                seen_page_urls.add(product_url)

                precio_actual = _normalize_price(current_price_el.get_text(strip=True))
                if not precio_actual:
                    continue

                if previous_price_el:
                    precio_anterior = _normalize_price(previous_price_el.get_text(strip=True)) or None
                else:
                    precio_anterior = None

                image_url = _extract_image_url(el)

                page_items.append({
                    "titulo": title_el.get_text(strip=True),
                    "precio_actual": precio_actual,
                    "precio_anterior": precio_anterior,
                    "vendedor": seller_el.get_text(strip=True).replace('por ', '') if seller_el else "Mercado Libre",
                    "metodo_pago": installments_el.get_text(strip=True) if installments_el else "No especificado",
                    "url": product_url,
                    "url_imagen": image_url or None,
                })

            if include_details and page_items:
                tasks = [
                    _fetch_product_features(client, item["url"], semaphore)
                    for item in page_items
                ]
                features_list = await asyncio.gather(*tasks)
                for item, features in zip(page_items, features_list):
                    if features:
                        item["caracteristicas"] = features

            items_crudos.extend(page_items)

            # 4. Rate Limiting (Pausa entre páginas para evitar bloqueos)
            if page < max_pages - 1:
                await asyncio.sleep(random.uniform(PAGE_SLEEP_MIN_SECONDS, PAGE_SLEEP_MAX_SECONDS))

    logging.info(f"Scraping finalizado. Se extrajeron {len(items_crudos)} productos crudos.")

    return items_crudos