import asyncio
import logging
import random
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus, urljoin

from app.services.http_client import ResilientScraperClient

BASE_URL = "https://www.fravega.com"
SEARCH_API_URL = (
    BASE_URL + "/api/catalog_system/pub/products/search?ft={query}&_from={offset_from}&_to={offset_to}&sc=1"
)
PAGE_SIZE = 48
PAGE_SLEEP_MIN_SECONDS = 1.2
PAGE_SLEEP_MAX_SECONDS = 2.8
PC_CATEGORY_HINTS = (
    "computacion",
    "informatica",
    "tecnologia",
    "notebook",
    "pc",
    "monitores",
    "perifericos",
)
NON_PC_CATEGORY_HINTS = (
    "electrodomesticos",
    "hogar",
    "cocina",
    "alimentos",
    "juguetes",
    "jugueteria",
    "bebes",
    "indumentaria",
    "moda",
    "animales",
)


def _sanitize_keyword(raw_keyword: str) -> str:
    keyword = raw_keyword.strip().lower()
    keyword = re.sub(r"[^a-z0-9]+", "_", keyword)
    keyword = re.sub(r"_+", "_", keyword).strip("_")
    return keyword or "feature"


def _normalize_text(value: str) -> str:
    compact = re.sub(r"\s+", " ", value or "").strip().lower()
    return compact


def _pick_best_seller_offer(product: Dict[str, Any]) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    items = product.get("items") or []
    if not items:
        return None, None

    for item in items:
        sellers = item.get("sellers") or []
        
        for seller in sellers:
            offer = seller.get("commertialOffer") or {}
            
            # ATRIBUTOS CLAVE: Confiamos en IsAvailable y el Precio. 
            # Eliminamos la restricción estricta de AvailableQuantity > 0.
            is_available = offer.get("IsAvailable") is True
            price = float(offer.get("Price") or 0)
            
            if is_available and price > 0:
                return seller.get("sellerName"), offer

    return None, None


def _build_payment_description(offer: Optional[Dict[str, Any]]) -> str:
    if not offer:
        return "No especificado"

    installments = offer.get("Installments") or []
    if not installments:
        return "No especificado"

    without_interest = [
        item
        for item in installments
        if float(item.get("InterestRate") or 0) == 0 and int(item.get("NumberOfInstallments") or 0) > 1
    ]
    if without_interest:
        max_installments = max(int(item.get("NumberOfInstallments") or 0) for item in without_interest)
        return f"Hasta {max_installments} cuotas sin interes"

    max_installments = max(int(item.get("NumberOfInstallments") or 0) for item in installments)
    return f"Hasta {max_installments} cuotas"


def _extract_image_url(product: Dict[str, Any]) -> Optional[str]:
    candidate_keys = ("imageUrl", "image_url", "image", "thumbnail", "thumb", "src")

    for key in candidate_keys:
        value = product.get(key)
        if isinstance(value, str) and value.strip():
            return urljoin(BASE_URL, value.strip())

    items = product.get("items") or []
    if not isinstance(items, list):
        return None

    for item in items:
        if not isinstance(item, dict):
            continue

        for key in candidate_keys:
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return urljoin(BASE_URL, value.strip())

        images = item.get("images") or []
        if not isinstance(images, list):
            continue

        for image in images:
            if not isinstance(image, dict):
                continue

            for key in ("imageUrl", "image_url", "url", "src", "fileUrl"):
                value = image.get(key)
                if isinstance(value, str) and value.strip():
                    return urljoin(BASE_URL, value.strip())

    return None

def _extract_raw_features(product: Dict[str, Any]) -> List[Dict[str, str]]:
    features: List[Dict[str, str]] = []
    seen = set()

    for key in product.get("allSpecifications") or []:
        raw_values = product.get(key)
        values: List[str] = []

        if isinstance(raw_values, list):
            values = [str(value).strip() for value in raw_values if str(value).strip()]
        elif raw_values is not None:
            values = [str(raw_values).strip()]

        if not values:
            continue

        normalized_key = _sanitize_keyword(str(key))
        normalized_value = ", ".join(values)
        signature = (normalized_key, normalized_value)
        if signature in seen:
            continue

        seen.add(signature)
        features.append({"keyword": normalized_key, "value": normalized_value})

    return features



def _is_irrelevant_for_pc(query: str, title: str, categories: List[str]) -> bool:
    normalized_title = _normalize_text(title)
    normalized_query = _normalize_text(query)

    if "mouse" in normalized_query:
        if any(keyword in normalized_title for keyword in ("mickey", "minnie", "disney", "peluche", "juguete", "muneco")):
            return True

    if any(term in normalized_query for term in ("procesador", "cpu")):
        if any(keyword in normalized_title for keyword in ("procesadora", "alimentos", "cocina", "food", "kitchen")):
            return True

    if any(keyword in normalized_title for keyword in ("mickey", "minnie", "disney", "peluche", "juguete", "muneco")):
        return True

    if categories:
        category_text = _normalize_text(" ".join(categories))
        if any(keyword in category_text for keyword in NON_PC_CATEGORY_HINTS):
            if not any(keyword in category_text for keyword in PC_CATEGORY_HINTS):
                return True

    return False
async def scrape_fravega(query: str, max_pages: int = 1) -> List[dict]:
    """Scrapea Fravega usando su API de catalogo para evitar bloqueos por HTML dinamico."""
    encoded_query = quote_plus(query.strip())
    all_items: List[dict] = []
    seen_urls = set()
    discarded_no_image = 0

    async with ResilientScraperClient(min_delay_seconds=1.0, max_delay_seconds=2.3) as client:
        for page in range(max_pages):
            offset_from = page * PAGE_SIZE
            offset_to = offset_from + PAGE_SIZE - 1

            url = SEARCH_API_URL.format(
                query=encoded_query,
                offset_from=offset_from,
                offset_to=offset_to,
            )
            logging.info("Fravega: scrapeando pagina %s/%s", page + 1, max_pages)

            payload = await client.get_json(
                url,
                referer=f"{BASE_URL}/l/?keyword={encoded_query}",
                expected_status={200, 206},
            )

            logging.info("PAYLOAD RECIBIDO TIPO: %s", type(payload))
            if isinstance(payload, list):
                logging.info("CANTIDAD DE ITEMS EN PAYLOAD: %s", len(payload))
            elif isinstance(payload, dict):
                 logging.error("EL PAYLOAD ES UN DICCIONARIO: %s", payload)

            if not payload or not isinstance(payload, list):
                logging.warning("Fravega: respuesta vacia o invalida en pagina %s", page + 1)
                break

            page_items: List[dict] = []
            for product in payload:
                if not isinstance(product, dict):
                    continue

                title = str(product.get("productName") or "").strip()
                product_url = str(product.get("link") or "").strip()
                categories = product.get("categories") or []

                logging.info("PROCESANDO: %s - URL: %s", title, product_url)

                # CORRECCIÓN: Eliminamos el .endswith("/p") que era demasiado estricto
                if not title or not product_url:
                    logging.warning("SALTEADO: %s - URL invalida o vacia", title)
                    continue

                if _is_irrelevant_for_pc(query, title, categories):
                    logging.info("DESCARTADO (No PC): %s", title)
                    continue

                seller_name, offer = _pick_best_seller_offer(product)
                if not offer:
                    continue

                current_price = offer.get("Price")
                if current_price is None:
                    continue

                list_price = offer.get("ListPrice")
                previous_price = None
                if list_price is not None and float(list_price) > float(current_price):
                    previous_price = float(list_price)

                raw_features = _extract_raw_features(product)
                brand = str(product.get("brand") or "").strip()
                image_url = _extract_image_url(product)
                if not image_url:
                    discarded_no_image += 1
                    continue
                if brand:
                    raw_features.append({"keyword": "marca_api", "value": brand})

                page_items.append(
                    {
                        "titulo": title,
                        "precio_actual": float(current_price),
                        "precio_anterior": previous_price,
                        "vendedor": seller_name or "Fravega",
                        "metodo_pago": _build_payment_description(offer),
                        "url": product_url,
                        "url_imagen": image_url,
                        "caracteristicas": raw_features,
                    }
                )

            if not page_items:
                logging.info("Fravega: sin resultados utiles en pagina %s", page + 1)
                break

            added = 0
            for item in page_items:
                url_key = item.get("url")
                if not url_key or url_key in seen_urls:
                    continue
                seen_urls.add(url_key)
                all_items.append(item)
                added += 1

            if added == 0:
                logging.info("Fravega: sin productos nuevos en pagina %s", page + 1)
                break

            if page < max_pages - 1:
                await asyncio.sleep(random.uniform(PAGE_SLEEP_MIN_SECONDS, PAGE_SLEEP_MAX_SECONDS))

    logging.info("Fravega: se extrajeron %s productos crudos", len(all_items))
    if discarded_no_image:
        logging.warning("Fravega: descartados sin imagen=%s", discarded_no_image)
    return all_items
