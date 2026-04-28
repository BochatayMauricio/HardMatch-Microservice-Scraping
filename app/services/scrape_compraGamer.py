import asyncio
import logging
import re
import time
import unicodedata
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus, urljoin

from bs4 import BeautifulSoup

from app.services.http_client import ResilientScraperClient

BASE_URL = "https://compragamer.com"
CATALOG_URL = "https://static.compragamer.com/productos"
DETAILS_URL = "https://static.compragamer.com/productos_caracteristicas"
CACHE_TTL_SECONDS = 900
PAGE_SIZE = 24
DETAIL_IMAGE_CONCURRENCY = 4
MAX_DETAIL_IMAGE_FETCH = 18

NON_WORD_PATTERN = re.compile(r"[^a-z0-9]+")
MULTISPACE_PATTERN = re.compile(r"\s+")
IMAGE_URL_PATTERN = re.compile(r"https?://imagenes\.compragamer\.com/[^\s\"']+")

_catalog_cache: Optional[List[Dict[str, Any]]] = None
_details_by_product_id_cache: Dict[int, Dict[str, Any]] = {}
_cache_loaded_at = 0.0
_cache_lock = None

EXCLUDED_TITLE_KEYWORDS = {
    "armado",
    "ensamblado",
    "ensamble",
    "servicio",
}

EXCLUDED_CATEGORY_KEYWORDS = {
    "armado",
    "servicio",
}


def _get_cache_lock():
    global _cache_lock
    if _cache_lock is None:
        import asyncio

        _cache_lock = asyncio.Lock()
    return _cache_lock


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return normalized.encode("ascii", "ignore").decode("ascii")


def _normalize_search_text(value: str) -> str:
    lowered = _strip_accents(value).lower()
    lowered = NON_WORD_PATTERN.sub(" ", lowered)
    return MULTISPACE_PATTERN.sub(" ", lowered).strip()


def _get_category_text(product: Dict[str, Any]) -> str:
    keys = ("categoria", "subcategoria", "rubro", "familia", "linea", "tipo", "seccion")
    parts: List[str] = []
    for key in keys:
        raw_value = product.get(key)
        if isinstance(raw_value, str) and raw_value.strip():
            parts.append(raw_value.strip())
    return _normalize_search_text(" ".join(parts)) if parts else ""


def _is_excluded_product(name: str, product: Dict[str, Any]) -> bool:
    normalized_name = _normalize_search_text(name)
    if any(keyword in normalized_name for keyword in EXCLUDED_TITLE_KEYWORDS):
        return True

    category_text = _get_category_text(product)
    if category_text and any(keyword in category_text for keyword in EXCLUDED_CATEGORY_KEYWORDS):
        return True

    return False


def _slugify_product_name(name: str) -> str:
    compact = _strip_accents(name)
    compact = re.sub(r"[^A-Za-z0-9]+", "_", compact)
    compact = re.sub(r"_+", "_", compact).strip("_")
    return compact or "producto"


def _format_price_number(raw_value: Optional[Any]) -> Optional[str]:
    if raw_value in (None, ""):
        return None

    if isinstance(raw_value, (int, float)):
        amount = int(round(float(raw_value)))
        return str(amount) if amount > 0 else None

    digits = re.sub(r"[^\d]", "", str(raw_value))
    return digits or None


def _to_float(raw_value: Optional[Any]) -> Optional[float]:
    if raw_value in (None, ""):
        return None
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        numeric = _format_price_number(raw_value)
        return float(numeric) if numeric else None


def _resolve_previous_price(product: Dict[str, Any], current_price: float) -> Optional[str]:
    candidates = [
        product.get("precioEspecialAnterior"),
        product.get("precioListaAnterior"),
        product.get("precioLista"),
    ]
    for candidate in candidates:
        value = _to_float(candidate)
        if value and value > current_price:
            return str(int(round(value)))
    return None


def _build_payment_description(precios_cuotas: Any) -> str:
    if not isinstance(precios_cuotas, dict):
        return "No especificado"

    max_installments = 0
    for cuotas_values in precios_cuotas.values():
        if not isinstance(cuotas_values, list):
            continue
        for row in cuotas_values:
            if not isinstance(row, dict):
                continue
            cuotas = row.get("cuotas")
            if isinstance(cuotas, int):
                max_installments = max(max_installments, cuotas)

    if max_installments <= 1:
        return "No especificado"
    return f"Hasta {max_installments} cuotas (segun medio de pago)"


def _build_product_url(name: str, product_id: int, query: str, page: int) -> str:
    slug = _slugify_product_name(name)
    encoded_query = quote_plus(query.strip())
    return f"{BASE_URL}/producto/{slug}_{product_id}?criterio={encoded_query}&page={page}"


def _normalize_image_url(raw_value: Optional[str]) -> Optional[str]:
    if not raw_value:
        return None

    value = str(raw_value).strip()
    if not value:
        return None

    if value.startswith("//"):
        return f"https:{value}"
    if value.startswith("http://") or value.startswith("https://"):
        return value

    value = value.lstrip("/")
    static_base = "https://static.compragamer.com/"
    if value.startswith("productos/") or value.startswith("imagenes/") or value.startswith("uploads/"):
        return urljoin(static_base, value)

    return urljoin(CATALOG_URL + "/", value)


def _extract_product_image_from_payload(payload: Dict[str, Any]) -> Optional[str]:
    if not isinstance(payload, dict):
        return None

    candidate_keys = (
        "imagen",
        "image",
        "imageUrl",
        "image_url",
        "foto",
        "thumbnail",
        "thumb",
        "urlImagen",
        "url_imagen",
        "archivo",
        "filename",
        "fileName",
        "path",
    )

    for key in candidate_keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return _normalize_image_url(value)

    nested_keys = ("images", "imagenes", "fotos", "gallery", "galeria")
    for key in nested_keys:
        nested_values = payload.get(key) or []
        if isinstance(nested_values, dict):
            nested_values = list(nested_values.values())

        if not isinstance(nested_values, list):
            continue

        for nested in nested_values:
            if isinstance(nested, str) and nested.strip():
                return _normalize_image_url(nested)
            if not isinstance(nested, dict):
                continue

            for image_key in (
                "url",
                "src",
                "imageUrl",
                "image_url",
                "fileUrl",
                "archivo",
                "filename",
                "fileName",
                "path",
            ):
                value = nested.get(image_key)
                if isinstance(value, str) and value.strip():
                    return _normalize_image_url(value)

    return None


def _extract_product_image(product: Dict[str, Any], detail_payload: Optional[Dict[str, Any]] = None) -> Optional[str]:
    image_url = _extract_product_image_from_payload(product)
    if image_url:
        return image_url

    if detail_payload:
        return _extract_product_image_from_payload(detail_payload)

    return None


def _extract_image_from_html(html: str) -> Optional[str]:
    if not html:
        return None

    match = IMAGE_URL_PATTERN.search(html)
    if match:
        return _normalize_image_url(match.group(0))

    soup = BeautifulSoup(html, "html.parser")
    meta_keys = (
        ("property", "og:image"),
        ("property", "og:image:secure_url"),
        ("name", "twitter:image"),
        ("name", "twitter:image:src"),
    )
    for attr, key in meta_keys:
        meta = soup.find("meta", {attr: key})
        if meta:
            content = meta.get("content")
            if isinstance(content, str) and content.strip():
                normalized = _normalize_image_url(content)
                if normalized:
                    return normalized

    candidate_keys = (
        "data-srcset",
        "data-lazy-srcset",
        "srcset",
        "data-src",
        "data-lazy-src",
        "data-lazy",
        "data-original",
        "src",
    )
    for image in soup.select("img"):
        for key in candidate_keys:
            value = image.get(key)
            if not value:
                continue

            candidate = value
            if "srcset" in key:
                candidate = value.split(",", 1)[0].strip().split(" ", 1)[0].strip()

            if isinstance(candidate, str) and "compragamer.com" in candidate:
                normalized = _normalize_image_url(candidate)
                if normalized:
                    return normalized

    return None


async def _fetch_detail_image(
    client: ResilientScraperClient,
    product_url: str,
    semaphore: asyncio.Semaphore,
) -> Optional[str]:
    if not product_url:
        return None

    async with semaphore:
        html = await client.get_text(product_url, referer=BASE_URL)
        if not html:
            return None

    return _extract_image_from_html(html)


def _extract_raw_features(detail_payload: Optional[Dict[str, Any]]) -> List[Dict[str, str]]:
    if not detail_payload or not isinstance(detail_payload, dict):
        return []

    features: List[Dict[str, str]] = []
    seen = set()
    for item in detail_payload.get("caracteristicas") or []:
        if not isinstance(item, dict):
            continue

        keyword = str(item.get("etiqueta") or "").strip()
        value = str(item.get("valor") or "").strip()
        unit = str(item.get("unidades") or "").strip()
        feature_type = str(item.get("tipo") or "").strip().lower()

        if not keyword or not value:
            continue

        if feature_type == "booleano":
            if value in {"1", "true", "True", "SI", "si", "Sí", "sí"}:
                value = "Si"
            elif value in {"0", "false", "False", "NO", "no"}:
                value = "No"

        if unit and unit != "None":
            value = f"{value} {unit}".strip()

        signature = (_normalize_search_text(keyword), _normalize_search_text(value))
        if signature in seen:
            continue
        seen.add(signature)
        features.append({"keyword": keyword, "value": value})

    return features


def _filter_products(products: List[Dict[str, Any]], query: str) -> List[Dict[str, Any]]:
    normalized_query = _normalize_search_text(query)
    tokens = [token for token in normalized_query.split(" ") if token]
    if not tokens:
        return products

    all_tokens_match: List[Tuple[int, Dict[str, Any]]] = []
    any_token_match: List[Tuple[int, Dict[str, Any]]] = []

    for product in products:
        name = str(product.get("nombre") or "")
        normalized_name = _normalize_search_text(name)
        if not normalized_name:
            continue

        token_hits = sum(1 for token in tokens if token in normalized_name)
        if token_hits == len(tokens):
            all_tokens_match.append((token_hits, product))
        elif token_hits > 0:
            any_token_match.append((token_hits, product))

    selected = all_tokens_match if all_tokens_match else any_token_match
    selected.sort(key=lambda row: row[0], reverse=True)
    return [row[1] for row in selected]


async def _load_catalog_data(client: ResilientScraperClient) -> Tuple[List[Dict[str, Any]], Dict[int, Dict[str, Any]]]:
    global _catalog_cache
    global _details_by_product_id_cache
    global _cache_loaded_at

    now = time.time()
    if _catalog_cache is not None and now - _cache_loaded_at < CACHE_TTL_SECONDS:
        return _catalog_cache, _details_by_product_id_cache

    lock = _get_cache_lock()
    async with lock:
        now = time.time()
        if _catalog_cache is not None and now - _cache_loaded_at < CACHE_TTL_SECONDS:
            return _catalog_cache, _details_by_product_id_cache

        products_payload, details_payload = await __import__("asyncio").gather(
            client.get_json(CATALOG_URL, referer=BASE_URL, expected_status={200}),
            client.get_json(DETAILS_URL, referer=BASE_URL, expected_status={200}),
        )

        products = products_payload if isinstance(products_payload, list) else []
        details_by_id: Dict[int, Dict[str, Any]] = {}
        if isinstance(details_payload, list):
            for row in details_payload:
                if not isinstance(row, dict):
                    continue
                raw_id = row.get("id_producto")
                if isinstance(raw_id, int):
                    details_by_id[raw_id] = row

        _catalog_cache = products
        _details_by_product_id_cache = details_by_id
        _cache_loaded_at = time.time()
        logging.info(
            "Compra Gamer: catalogo actualizado. Productos=%s, detalles=%s",
            len(_catalog_cache),
            len(_details_by_product_id_cache),
        )

        return _catalog_cache, _details_by_product_id_cache


async def scrape_compra_gamer(query: str, max_pages: int = 1) -> List[dict]:
    """Scrapea Compra Gamer desde su catalogo JSON estatico (el listado HTML es JS-rendered)."""
    if not query or not query.strip():
        return []

    max_pages = max(1, max_pages)
    hard_limit = max_pages * PAGE_SIZE
    all_items: List[dict] = []

    async with ResilientScraperClient(min_delay_seconds=0.5, max_delay_seconds=1.4) as client:
        catalog, details_by_id = await _load_catalog_data(client)

        if not catalog:
            logging.warning("Compra Gamer: catalogo vacio o no disponible")
            return []

        filtered_products = _filter_products(catalog, query)
        if not filtered_products:
            logging.info("Compra Gamer: sin coincidencias para query '%s'", query)
            return []

        missing_image_items: List[dict] = []
        detail_semaphore = asyncio.Semaphore(DETAIL_IMAGE_CONCURRENCY)
        discarded_no_image = 0

        for idx, product in enumerate(filtered_products[:hard_limit]):
            if not isinstance(product, dict):
                continue

            product_id = product.get("id_producto")
            if not isinstance(product_id, int):
                continue

            title = str(product.get("nombre") or "").strip()
            if not title:
                continue

            if _is_excluded_product(title, product):
                continue

            if int(product.get("vendible") or 0) != 1:
                continue

            current_price_float = _to_float(product.get("precioEspecial"))
            if not current_price_float or current_price_float <= 0:
                continue

            current_price = str(int(round(current_price_float)))
            previous_price = _resolve_previous_price(product, current_price_float)
            page = (idx // PAGE_SIZE) + 1
            detail_payload = details_by_id.get(product_id)
            image_url = _extract_product_image(product, detail_payload)
            product_url = _build_product_url(title, product_id, query, page)

            item = {
                "titulo": title,
                "precio_actual": current_price,
                "precio_anterior": previous_price,
                "vendedor": "Compra Gamer",
                "metodo_pago": _build_payment_description(product.get("precios_cuotas")),
                "url": product_url,
                "url_imagen": image_url,
                "caracteristicas": _extract_raw_features(detail_payload),
            }

            if image_url:
                all_items.append(item)
            else:
                missing_image_items.append(item)

        if missing_image_items:
            to_fetch = missing_image_items[:MAX_DETAIL_IMAGE_FETCH]
            skipped_fetch = len(missing_image_items) - len(to_fetch)
            if skipped_fetch > 0:
                discarded_no_image += skipped_fetch
                logging.info(
                    "Compra Gamer: se omiten %s items sin imagen para evitar sobrecarga",
                    skipped_fetch,
                )

            image_tasks = [
                _fetch_detail_image(client, item.get("url", ""), detail_semaphore)
                for item in to_fetch
            ]
            images = await asyncio.gather(*image_tasks)
            for item, image in zip(to_fetch, images):
                if image:
                    item["url_imagen"] = image
                    all_items.append(item)
                else:
                    discarded_no_image += 1

        if discarded_no_image:
            logging.warning(
                "Compra Gamer: descartados sin imagen=%s",
                discarded_no_image,
            )

    logging.info(
        "Compra Gamer: query='%s' filtrados=%s, devueltos=%s",
        query,
        len(filtered_products),
        len(all_items),
    )
    return all_items
