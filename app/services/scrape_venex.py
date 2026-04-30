import asyncio
import logging
import random
import re
import unicodedata
from typing import List, Optional
from urllib.parse import quote_plus, unquote, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from app.services.http_client import ResilientScraperClient

BASE_URL = "https://www.venex.com.ar"
SEARCH_URL = BASE_URL + "/resultado-busqueda.htm?keywords={query}&page={page}"
PAGE_SLEEP_MIN_SECONDS = 1.3
PAGE_SLEEP_MAX_SECONDS = 2.8
DETAIL_CONCURRENCY = 5

PRICE_PATTERN = re.compile(r"\$\s*([\d\.,]+)")
MULTISPACE_PATTERN = re.compile(r"\s+")
STYLE_URL_PATTERN = re.compile(r"url\((['\"]?)(.*?)\1\)")
DETAIL_PAIR_PATTERN = re.compile(
    r"([A-Za-z0-9\s\-_/\.,ÁÉÍÓÚáéíóúÑñ]{2,45}):\s*([^:]{1,140})(?=\s+[A-Za-z0-9\s\-_/\.,ÁÉÍÓÚáéíóúÑñ]{2,45}:|$)"
)

EXCLUDED_PATH_HINTS = (
    "/resultado-busqueda.htm",
    "/shopping_cart",
    "/account_",
    "/products_favorite",
    "/pagina-inicial",
    "/configurador",
    "/centro-de-ayuda",
    "/quienes-somos",
    "/politicas",
    "/terminos",
    "/promociones",
)

FINANCING_KEYWORDS = {"cuotas", "interes", "cft", "tea"}


def _normalize_for_match(value: str) -> str:
    cleaned = value.replace("�", "a")
    cleaned = unicodedata.normalize("NFKD", cleaned).encode("ascii", "ignore").decode("ascii")
    cleaned = cleaned.lower()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _clean_price(raw_value: Optional[str]) -> Optional[str]:
    if not raw_value:
        return None
    digits = re.sub(r"[^\d]", "", raw_value)
    return digits or None


def _canonical_url(raw_href: str) -> str:
    if not raw_href:
        return ""
    absolute = urljoin(BASE_URL, raw_href)
    parsed = urlsplit(absolute)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def _normalize_image_url(raw_value: str) -> str:
    value = (raw_value or "").strip()
    if not value:
        return ""

    if value.startswith("//"):
        return f"https:{value}"

    return urljoin(BASE_URL, value)


def _is_product_url(url: str) -> bool:
    if not url:
        return False

    parsed = urlsplit(url)
    if "venex.com.ar" not in parsed.netloc:
        return False
    if not parsed.path.endswith(".html"):
        return False

    lowered_path = parsed.path.lower()
    return not any(hint in lowered_path for hint in EXCLUDED_PATH_HINTS)


def _title_from_url(url: str) -> str:
    path = unquote(urlsplit(url).path)
    slug = path.rstrip("/").split("/")[-1]
    slug = re.sub(r"\.html?$", "", slug, flags=re.IGNORECASE)
    title = slug.replace("-", " ")
    title = MULTISPACE_PATTERN.sub(" ", title).strip()
    return title or "Producto sin titulo"


def _normalize_title(raw_text: str, fallback_url: str) -> str:
    text = MULTISPACE_PATTERN.sub(" ", raw_text.replace("\n", " ")).strip()
    text = re.sub(r"\[[^\]]+\]", "", text)
    if "$" in text:
        text = text.split("$", 1)[0].strip()
    text = re.sub(r"\.html?$", "", text, flags=re.IGNORECASE).strip()

    if len(text) >= 8:
        return text
    return _title_from_url(fallback_url)


def _extract_url_from_style(style_value: str) -> str:
    if not style_value:
        return ""

    match = STYLE_URL_PATTERN.search(style_value)
    if not match:
        return ""

    return match.group(2).strip()


def _extract_image_url(anchor: BeautifulSoup) -> str:
    if not anchor:
        return ""

    candidates = [anchor]
    if anchor.parent:
        candidates.append(anchor.parent)
    if anchor.parent and anchor.parent.parent:
        candidates.append(anchor.parent.parent)

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

    for node in candidates:
        if not getattr(node, "select_one", None):
            continue

        image = node.select_one("img")
        if image:
            for key in candidate_keys:
                value = image.get(key)
                if not value:
                    continue

                if "srcset" in key:
                    first_candidate = value.split(",", 1)[0].strip().split(" ", 1)[0].strip()
                    if first_candidate:
                        normalized = _normalize_image_url(first_candidate)
                        if normalized:
                            return normalized

                if isinstance(value, str) and value.strip():
                    normalized = _normalize_image_url(value)
                    if normalized:
                        return normalized

        style_source = node.get("style")
        if style_source:
            style_url = _extract_url_from_style(style_source)
            if style_url:
                normalized = _normalize_image_url(style_url)
                if normalized:
                    return normalized

        for style_node in node.select("[style]"):
            style_url = _extract_url_from_style(style_node.get("style", ""))
            if style_url:
                normalized = _normalize_image_url(style_url)
                if normalized:
                    return normalized

    return ""


def _extract_prices_from_context(context: str) -> List[str]:
    prices: List[str] = []
    for raw_price in PRICE_PATTERN.findall(context):
        cleaned = _clean_price(raw_price)
        if cleaned:
            prices.append(cleaned)
    return prices


def _extract_items_from_html(html: str) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.select('a[href*=".html"]')

    items: List[dict] = []
    seen_urls = set()

    for anchor in anchors:
        product_url = _canonical_url(anchor.get("href", ""))
        if not _is_product_url(product_url) or product_url in seen_urls:
            continue

        parent_text = anchor.parent.get_text(" ", strip=True) if anchor.parent else ""
        grandparent_text = (
            anchor.parent.parent.get_text(" ", strip=True)
            if anchor.parent and anchor.parent.parent
            else ""
        )
        context = " ".join(
            chunk
            for chunk in [anchor.get_text(" ", strip=True), parent_text, grandparent_text]
            if chunk
        )

        prices = _extract_prices_from_context(context)
        if not prices:
            continue

        title = _normalize_title(anchor.get_text(" ", strip=True), product_url)
        current_price = prices[0]
        previous_price = prices[1] if len(prices) > 1 else None
        image_url = _extract_image_url(anchor)

        seen_urls.add(product_url)
        items.append(
            {
                "titulo": title,
                "precio_actual": current_price,
                "precio_anterior": previous_price,
                "vendedor": "Venex",
                "metodo_pago": "No especificado",
                "url": product_url,
                "url_imagen": image_url or None,
            }
        )

    return items


def _table_looks_like_financing(table: BeautifulSoup) -> bool:
    header_cells = [
        _normalize_for_match(cell.get_text(" ", strip=True))
        for cell in table.select("tr:first-child th, tr:first-child td")
    ]
    if not header_cells:
        return False
    return all(any(keyword in cell for cell in header_cells) for keyword in FINANCING_KEYWORDS)


def _extract_features_from_table(table: BeautifulSoup) -> List[dict]:
    if _table_looks_like_financing(table):
        return []

    features: List[dict] = []
    for row in table.find_all("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"])]
        if len(cells) < 2:
            continue

        keyword = MULTISPACE_PATTERN.sub(" ", cells[0]).strip(" :")
        value = MULTISPACE_PATTERN.sub(" ", cells[1]).strip(" :")
        if keyword and value:
            features.append({"keyword": keyword, "value": value})

    return features


def _extract_features_from_text_block(text: str) -> List[dict]:
    compact = MULTISPACE_PATTERN.sub(" ", text).strip()
    if not compact:
        return []

    features: List[dict] = []
    for keyword, value in DETAIL_PAIR_PATTERN.findall(compact):
        key_norm = MULTISPACE_PATTERN.sub(" ", keyword).strip(" :")
        val_norm = MULTISPACE_PATTERN.sub(" ", value).strip(" :")
        if key_norm and val_norm:
            features.append({"keyword": key_norm, "value": val_norm})

    return features


def _extract_product_features_from_detail_html(html: str) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: List[dict] = []
    seen = set()

    for heading in soup.find_all("h2"):
        heading_text = _normalize_for_match(heading.get_text(" ", strip=True))
        if "caracter" not in heading_text:
            continue

        parent = heading.parent if heading.parent else heading

        for table in parent.find_all("table"):
            for feat in _extract_features_from_table(table):
                signature = (_normalize_for_match(feat["keyword"]), _normalize_for_match(feat["value"]))
                if signature in seen:
                    continue
                seen.add(signature)
                candidates.append(feat)

        feature_blocks = parent.select(".features-product, .linea-caract")
        if not feature_blocks:
            sibling = heading.find_next_sibling()
            while sibling and sibling.name not in {"h1", "h2", "h3"}:
                if getattr(sibling, "select", None):
                    feature_blocks.extend(sibling.select(".features-product, .linea-caract"))
                sibling = sibling.find_next_sibling()

        for block in feature_blocks:
            block_text = block.get_text(" ", strip=True)
            for feat in _extract_features_from_text_block(block_text):
                signature = (_normalize_for_match(feat["keyword"]), _normalize_for_match(feat["value"]))
                if signature in seen:
                    continue
                seen.add(signature)
                candidates.append(feat)

    return candidates


async def _fetch_detail_features(
    client: ResilientScraperClient,
    product_url: str,
    semaphore: asyncio.Semaphore,
) -> List[dict]:
    if not product_url:
        return []

    async with semaphore:
        html = await client.get_text(product_url, referer=BASE_URL)
        if not html:
            return []

    return _extract_product_features_from_detail_html(html)


async def scrape_venex(query: str, max_pages: int = 1) -> List[dict]:
    """Scrapea Venex desde su endpoint de resultados y pagina en base al query."""
    encoded_query = quote_plus(query.strip())
    all_items: List[dict] = []
    seen_urls = set()
    discarded_no_image = 0

    async with ResilientScraperClient(min_delay_seconds=1.2, max_delay_seconds=2.6) as client:
        detail_semaphore = asyncio.Semaphore(DETAIL_CONCURRENCY)
        for page in range(1, max_pages + 1):
            search_url = SEARCH_URL.format(query=encoded_query, page=page)
            logging.info("Venex: scrapeando pagina %s/%s", page, max_pages)

            html = await client.get_text(search_url, referer=BASE_URL)
            if not html:
                logging.warning("Venex: no se pudo obtener pagina %s", page)
                break

            page_items = _extract_items_from_html(html)
            if not page_items:
                logging.info("Venex: pagina %s sin resultados parseables", page)
                break

            filtered_items: List[dict] = []
            for item in page_items:
                if not item.get("url_imagen"):
                    discarded_no_image += 1
                    continue
                filtered_items.append(item)

            if not filtered_items:
                logging.info("Venex: pagina %s sin resultados con imagen", page)
                break

            page_items = filtered_items

            added = 0
            for item in page_items:
                product_url = item.get("url")
                if not product_url or product_url in seen_urls:
                    continue

                seen_urls.add(product_url)
                all_items.append(item)
                added += 1

            if page_items:
                detail_tasks = [
                    _fetch_detail_features(client, item.get("url", ""), detail_semaphore)
                    for item in page_items
                ]
                details_results = await asyncio.gather(*detail_tasks)
                for item, features in zip(page_items, details_results):
                    if features:
                        item["caracteristicas"] = features

            if added == 0:
                logging.info("Venex: no hubo productos nuevos en pagina %s", page)
                break

            if page < max_pages:
                await asyncio.sleep(random.uniform(PAGE_SLEEP_MIN_SECONDS, PAGE_SLEEP_MAX_SECONDS))

    logging.info("Venex: se extrajeron %s productos crudos", len(all_items))
    if discarded_no_image:
        logging.warning("Venex: descartados sin imagen=%s", discarded_no_image)
    return all_items
