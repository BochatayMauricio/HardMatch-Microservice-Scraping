import asyncio
import logging
import random
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus

from app.services.http_client import ResilientScraperClient

BASE_URL = "https://www.fravega.com"
SEARCH_API_URL = (
    BASE_URL + "/api/catalog_system/pub/products/search?ft={query}&_from={offset_from}&_to={offset_to}&sc=1"
)
PAGE_SIZE = 48
PAGE_SLEEP_MIN_SECONDS = 1.2
PAGE_SLEEP_MAX_SECONDS = 2.8


def _sanitize_keyword(raw_keyword: str) -> str:
    keyword = raw_keyword.strip().lower()
    keyword = re.sub(r"[^a-z0-9]+", "_", keyword)
    keyword = re.sub(r"_+", "_", keyword).strip("_")
    return keyword or "feature"


def _pick_best_seller_offer(product: Dict[str, Any]) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    items = product.get("items") or []
    if not items:
        return None, None

    # Iteramos sobre todos los SKUs (ítems) del producto, porque a veces 
    # el item[0] (ej: color rojo) no tiene stock, pero el item[1] (color azul) sí.
    for item in items:
        sellers = item.get("sellers") or []
        
        for seller in sellers:
            offer = seller.get("commertialOffer") or {}
            
            # ATRIBUTOS CLAVE PARA DISTINGUIR PRODUCTOS REALMENTE DISPONIBLES
            is_available = offer.get("IsAvailable") is True
            has_stock = int(offer.get("AvailableQuantity") or 0) > 0
            price = float(offer.get("Price") or 0)
            
            # Solo si está explícitamente disponible, tiene al menos 1 en stock y cuesta más de 0
            if is_available and has_stock and price > 0:
                return seller.get("sellerName"), offer

    # Si recorrimos todos los items y vendedores y nadie tiene stock real, 
    # devolvemos None. NO usamos un fallback.
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

def _is_accessory(product: Dict[str, Any], title: str) -> bool:
    """Detecta si un producto es un accesorio usando las categorías de VTEX y su título."""
    title_lower = title.lower()

    # 1. Filtro por frases obvias en el título (mata los que vimos en tu log)
    if "porta notebook" in title_lower or "portanotebook" in title_lower:
        return True
    
    # Si arranca con estas palabras, 100% seguro no es una compu
    if title_lower.startswith(("mochila", "funda", "maletin", "maletín", "bolso", "portafolio", "soporte", "base", "cargador")):
        return True

    # 2. Filtro profundo por el árbol de categorías de VTEX (El más seguro)
    categories = product.get("categories") or []
    for cat in categories:
        cat_lower = str(cat).lower()
        if any(keyword in cat_lower for keyword in ["accesorios", "mochilas", "fundas", "indumentaria", "cables", "conectividad"]):
            return True

    return False

async def scrape_fravega(query: str, max_pages: int = 1) -> List[dict]:
    """Scrapea Fravega usando su API de catalogo para evitar bloqueos por HTML dinamico."""
    encoded_query = quote_plus(query.strip())
    all_items: List[dict] = []
    seen_urls = set()

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

                # --- NUEVOS LOGS AQUÍ ---
                logging.info("PROCESANDO: %s - URL: %s", title, product_url)
                # ------------------------

                if not title or not product_url or not product_url.endswith("/p"):
                    logging.warning("SALTEADO: %s - URL invalida o malformada (%s)", title, product_url)
                    continue

                # Validación 2: EL NUEVO FILTRO DE ACCESORIOS
                if _is_accessory(product, title):
                    logging.info("DESCARTADO (Es Accesorio): %s", title)
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
    return all_items
