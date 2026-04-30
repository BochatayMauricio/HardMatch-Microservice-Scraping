import logging
import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlsplit

from app.schemas.product_schema import BrandSchema, FeatureSchema, ProductSchema

BRAND_KEYWORDS: Dict[str, str] = {
    "asus": "Asus",
    "hp": "HP",
    "lenovo": "Lenovo",
    "dell": "Dell",
    "acer": "Acer",
    "msi": "MSI",
    "gigabyte": "Gigabyte",
    "samsung": "Samsung",
    "lg": "LG",
    "apple": "Apple",
    "intel": "Intel",
    "amd": "AMD",
    "nvidia": "Nvidia",
    "xpg": "XPG",
    "corsair": "Corsair",
    "kingston": "Kingston",
    "redragon": "Redragon",
    "bangho": "Bangho",
    "asrock": "ASRock",
    "zotac": "Zotac",
    "palit": "Palit",
    "biostar": "Biostar",
    "cooler master": "Cooler Master",
    "logitech": "Logitech",
    "razer": "Razer",
}

CATEGORY_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (
        re.compile(
            r"\b(soporte|stand|base\s+refrigerante|cooler\s*pad|cooling\s*pad|dock(ing)?|porta\s*notebook|portanotebook)\b",
            re.IGNORECASE,
        ),
        "Accesorios",
    ),
    (re.compile(r"\b(notebook|laptop|ultrabook)\b", re.IGNORECASE), "Notebooks"),
    (re.compile(r"\bmonitor(es)?\b", re.IGNORECASE), "Monitores"),
    (re.compile(r"\b(placa\s*de\s*video|gpu|rtx|gtx|radeon)\b", re.IGNORECASE), "Placas de Video"),
    (re.compile(r"\b(procesador|cpu|ryzen|intel\s*core|pentium|celeron)\b", re.IGNORECASE), "Procesadores"),
    (re.compile(r"\b(memoria\s*ram|ram|ddr4|ddr5)\b", re.IGNORECASE), "Memorias RAM"),
    (re.compile(r"\b(ssd|hdd|disco|nvme)\b", re.IGNORECASE), "Almacenamiento"),
    (re.compile(r"\b(mouse|mou?se|rat[oó]n)\b", re.IGNORECASE), "Mouses"),
    (re.compile(r"\b(teclado|keyboard)\b", re.IGNORECASE), "Teclados"),
    (re.compile(r"\b(auricular(es)?|headset|headphone|cascos)\b", re.IGNORECASE), "Auriculares"),
    (re.compile(r"\b(parlante(s)?|speaker|altavoz|soundbar)\b", re.IGNORECASE), "Audio"),
    (re.compile(r"\b(mother|motherboard|placa\s*madre)\b", re.IGNORECASE), "Motherboards"),
    (re.compile(r"\b(gabinete|case|torre)\b", re.IGNORECASE), "Gabinetes"),
    (re.compile(r"\b(fuente|fuente\s*de\s*alimentaci[oó]n|psu)\b", re.IGNORECASE), "Fuentes"),
    (re.compile(r"\b(webcam|c[aá]mara)\b", re.IGNORECASE), "Webcams"),
    (re.compile(r"\b(cooler|water\s*cooler|disipador)\b", re.IGNORECASE), "Refrigeracion"),
]

PROCESSOR_PATTERN = re.compile(
    r"(intel\s*core\s*ultra\s*[3579]|intel\s*core\s*i[3579]|ryzen\s*(ai\s*)?[3579]\s*\d{3,4}x?|pentium|celeron)",
    re.IGNORECASE,
)
RAM_MODULE_PATTERN = re.compile(r"\b(memoria\s*ram|modulo\s*ram|ram\s*sodimm|sodimm|udimm|dimm)\b", re.IGNORECASE)
RAM_PATTERN = re.compile(r"(\d{1,3})\s*gb\s*(ram|ddr[345])", re.IGNORECASE)
RAM_FALLBACK_PATTERN = re.compile(r"\b(\d{1,3})\s*gb\b", re.IGNORECASE)
STORAGE_PATTERN = re.compile(r"(\d{2,4})\s*(gb|tb)\s*(ssd|hdd|nvme)?", re.IGNORECASE)
GPU_PATTERN = re.compile(
    r"(rtx\s*\d{3,4}|gtx\s*\d{3,4}|radeon\s*(rx\s*)?\d{3,4}|intel\s*iris|uhd\s*graphics)",
    re.IGNORECASE,
)
RESOLUTION_PATTERN = re.compile(r"(\d{3,4}x\d{3,4}|fhd|full\s*hd|qhd|uhd|4k)", re.IGNORECASE)
REFRESH_RATE_PATTERN = re.compile(r"(\d{2,3})\s*hz", re.IGNORECASE)
VRAM_PATTERN = re.compile(r"(\d{1,2})\s*gb\s*(vram|gddr[56])", re.IGNORECASE)
SCREEN_SIZE_PATTERN = re.compile(r"(\d{2}([\.,]\d)?)\s*(\"|pulgadas|in)\b", re.IGNORECASE)
PANEL_PATTERN = re.compile(r"\b(ips|tn|va|oled)\b", re.IGNORECASE)
STORAGE_TYPE_PATTERN = re.compile(r"\b(ssd|hdd|nvme)\b", re.IGNORECASE)
CONNECTION_PATTERN = re.compile(r"\b(inal[áa]mbrico|wireless|bluetooth|usb|2\.4\s*ghz|cableado)\b", re.IGNORECASE)
DPI_PATTERN = re.compile(r"(\d{3,5})\s*dpi", re.IGNORECASE)
SWITCH_PATTERN = re.compile(r"\b(red|brown|blue|silver|yellow|black)\b\s*(switch|switches|mec[aá]nico)?", re.IGNORECASE)
LAYOUT_PATTERN = re.compile(r"\b(iso|ansi|qwerty|qwertz|azerty)\b", re.IGNORECASE)
MIC_PATTERN = re.compile(r"\b(micr[oó]fono|micro)\b", re.IGNORECASE)
KEYBOARD_LANG_PATTERN = re.compile(r"\b(espa[ñn]ol|spanish|ingl[eé]s|english|portugu[eé]s|pt|br)\b", re.IGNORECASE)
DIMENSIONS_PATTERN = re.compile(
    r"(\d{1,3}(?:[\.,]\d{1,2})?)\s*[x×]\s*(\d{1,3}(?:[\.,]\d{1,2})?)\s*[x×]\s*(\d{1,3}(?:[\.,]\d{1,2})?)\s*(cm|mm)\b",
    re.IGNORECASE,
)
OS_PATTERN = re.compile(r"\b(windows\s*11|windows\s*10|win11|win10|freedos|linux|macos)\b", re.IGNORECASE)

RAW_KEYWORD_ALIASES: Dict[str, str] = {
    "tamano_de_pantalla": "pulgadas",
    "capacidad_de_disco": "almacenamiento",
    "almacenamiento": "almacenamiento",
    "modelo": "modelo",
    "origen": "origen",
    "color": "color",
    "resolucion": "resolucion",
    "memoria_ram": "memoria_ram",
}

IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".svg", ".avif")
PLACEHOLDER_HOSTS = {"placehold.co", "via.placeholder.com"}


def _first_present(item: Dict[str, Any], keys: List[str]) -> Optional[Any]:
    for key in keys:
        if key in item and item.get(key) not in (None, ""):
            return item.get(key)
    return None


def _title_from_url(url: str) -> str:
    if not url:
        return ""

    path = unquote(urlsplit(url).path)
    slug = path.rstrip("/").split("/")[-1]
    slug = re.sub(r"\.html?$", "", slug, flags=re.IGNORECASE)
    slug = re.sub(r"_\d{4,8}$", "", slug)
    text = slug.replace("-", " ").replace("_", " ")
    return re.sub(r"\s+", " ", text).strip()


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return normalized.encode("ascii", "ignore").decode("ascii")


def _normalize_keyword(raw_keyword: str) -> str:
    keyword = _strip_accents(raw_keyword).lower().strip()
    keyword = re.sub(r"[^a-z0-9]+", "_", keyword)
    keyword = re.sub(r"_+", "_", keyword).strip("_")
    return RAW_KEYWORD_ALIASES.get(keyword, keyword or "feature")


def _extract_brand_from_raw_features(raw_features: List[dict]) -> Tuple[Optional[str], List[dict]]:
    if not raw_features:
        return None, []

    brand_name: Optional[str] = None
    filtered: List[dict] = []
    for raw in raw_features:
        if not isinstance(raw, dict):
            continue

        keyword_raw = str(raw.get("keyword") or raw.get("name") or "").strip()
        keyword_norm = _normalize_keyword(keyword_raw) if keyword_raw else ""

        if keyword_norm in {"marca_api", "marca", "brand", "brand_name"}:
            value = str(raw.get("value") or raw.get("valor") or "").strip()
            if value and value.lower() != "no especificado" and not brand_name:
                brand_name = value
            continue

        filtered.append(raw)

    return brand_name, filtered


def _to_float(value: Optional[Any]) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    raw = str(value).strip()
    if not raw:
        return None

    cleaned = re.sub(r"[^\d,\.\-]", "", raw)
    if not cleaned:
        return None

    sign = -1 if cleaned.startswith("-") else 1
    cleaned = cleaned.lstrip("-")

    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            numeric = cleaned.replace(".", "").replace(",", ".")
        else:
            numeric = cleaned.replace(",", "")
    elif "," in cleaned:
        if cleaned.count(",") == 1 and len(cleaned.split(",", 1)[1]) <= 2:
            numeric = cleaned.replace(",", ".")
        else:
            numeric = cleaned.replace(",", "")
    elif "." in cleaned:
        if cleaned.count(".") == 1 and len(cleaned.split(".", 1)[1]) <= 2:
            numeric = cleaned
        else:
            numeric = cleaned.replace(".", "")
    else:
        numeric = cleaned

    try:
        return sign * float(numeric)
    except ValueError:
        return None


def _looks_like_placeholder_url(value: str) -> bool:
    if not value:
        return False

    parsed = urlsplit(value)
    host = parsed.netloc.lower()
    path = parsed.path.lower()

    if host in PLACEHOLDER_HOSTS or host.endswith(".placehold.co"):
        return True
    if "placeholder" in host or "placehold" in host:
        return True
    if "placeholder" in path or "placehold" in path:
        return True

    return False


def _looks_like_image_url(value: str) -> bool:
    if not value:
        return False

    parsed = urlsplit(value)
    path = parsed.path.lower()
    if any(path.endswith(ext) for ext in IMAGE_EXTENSIONS):
        return True

    if _looks_like_placeholder_url(value):
        return True

    if re.search(r"/\d{2,4}x\d{2,4}(/|$)", path) and "text=" in parsed.query.lower():
        return True

    return False


def _pick_first_non_image_url(candidates: List[str]) -> str:
    for candidate in candidates:
        value = candidate.strip()
        if value and not _looks_like_image_url(value):
            return value
    return ""


def _pick_first_image_url(candidates: List[str]) -> str:
    for candidate in candidates:
        value = candidate.strip()
        if value and _looks_like_image_url(value) and not _looks_like_placeholder_url(value):
            return value
    return ""


def _resolve_store_name(item: Dict[str, Any]) -> str:
    for key in ("tienda", "vendedor", "seller", "sellerName"):
        raw_value = item.get(key)
        if isinstance(raw_value, str) and raw_value.strip():
            return raw_value.strip()
    return "desconocida"


def _extract_brand(title: str) -> Optional[BrandSchema]:
    lowered = title.lower()
    for key, name in BRAND_KEYWORDS.items():
        if re.search(rf"\b{re.escape(key)}\b", lowered):
            return BrandSchema(name=name)
    return None


def _extract_category(title: str) -> Optional[str]:
    if RAM_MODULE_PATTERN.search(title):
        return "Memorias RAM"
    for pattern, category in CATEGORY_PATTERNS:
        if pattern.search(title):
            return category
    return None


def _add_feature(features: List[FeatureSchema], keyword: str, value: Optional[str]) -> None:
    if value:
        normalized_keyword = keyword.strip()
        normalized_value = value.strip()
        if not normalized_keyword or not normalized_value:
            return

        if any(
            f.keyword.lower() == normalized_keyword.lower()
            and (f.value or "").lower() == normalized_value.lower()
            for f in features
        ):
            return
        features.append(FeatureSchema(keyword=normalized_keyword, value=normalized_value))

def _extract_features(
    metodo_pago: Optional[str],
    raw_features: Optional[List[dict]] = None,
) -> List[FeatureSchema]:
    features: List[FeatureSchema] = []

    # 1. Cargar las caracteristicas puras obtenidas por los scrapers
    if raw_features:
        for raw in raw_features:
            if not isinstance(raw, dict):
                continue

            keyword_raw = str(raw.get("keyword") or raw.get("name") or "").strip()
            value = str(raw.get("value") or raw.get("valor") or "").strip()
            if not keyword_raw or not value:
                continue

            keyword = _normalize_keyword(keyword_raw)
            _add_feature(features, keyword, value)

    # 2. Agregar el metodo de pago como una caracteristica extra si existe
    if metodo_pago and metodo_pago.strip() and metodo_pago != "No especificado":
        _add_feature(features, "metodo_pago", metodo_pago.strip())

    return features



def normalize_data(items_crudos: List[dict]) -> List[ProductSchema]:
    """
    Normaliza productos sin IA usando reglas y expresiones regulares.
    """
    if not items_crudos:
        return []

    normalized: List[ProductSchema] = []
    total_products = 0
    total_features = 0
    zero_feature_products = 0
    one_feature_products = 0
    store_feature_stats: Dict[str, Dict[str, int]] = {}
    sample_logged = 0
    for item in items_crudos:
        if not isinstance(item, dict):
            continue

        title = str(
            _first_present(item, ["titulo", "title", "name", "productName"]) or ""
        ).strip()
        url_candidates = [
            str(item.get("url") or "").strip(),
            str(item.get("link") or "").strip(),
            str(item.get("urlAccess") or "").strip(),
            str(item.get("permalink") or "").strip(),
            str(item.get("product_url") or "").strip(),
            str(item.get("productUrl") or "").strip(),
            str(item.get("product_link") or "").strip(),
        ]
        url_candidates = [candidate for candidate in url_candidates if candidate]

        url = _pick_first_non_image_url(url_candidates)
        image_url = str(
            _first_present(item, ["url_imagen", "image_url", "imageUrl", "image", "thumbnail"])
            or ""
        ).strip()
        if image_url and _looks_like_placeholder_url(image_url):
            image_url = ""
        if not image_url and url_candidates:
            image_url = _pick_first_image_url(url_candidates)

        if not url:
            store_name = _resolve_store_name(item)
            logging.warning(
                "URL de acceso invalida en tienda '%s' para '%s'. Candidatos=%s",
                store_name,
                title or "Sin nombre",
                url_candidates,
            )
            continue
        if _looks_like_image_url(url):
            store_name = _resolve_store_name(item)
            logging.warning(
                "URL de acceso parece imagen en tienda '%s' para '%s': %s",
                store_name,
                title or "Sin nombre",
                url,
            )
            continue
        if not title:
            title = _title_from_url(url)

        price = _to_float(_first_present(item, ["precio_actual", "price", "precio"]))
        regular_price = _to_float(
            _first_present(item, ["precio_anterior", "regularPrice", "listPrice"])
        )

        seller_raw = _first_present(item, ["vendedor", "seller", "sellerName"])
        seller = str(seller_raw).strip() if seller_raw else None

        metodo_pago_raw = _first_present(item, ["metodo_pago", "paymentMethod", "payment"])
        metodo_pago = str(metodo_pago_raw).strip() if metodo_pago_raw else None

        category_raw = _first_present(item, ["categoria", "category"])
        category = str(category_raw).strip() if category_raw else None
        if not category and title:
            category = _extract_category(title)

        raw_features = _first_present(item, ["caracteristicas", "features"]) or []
        if not isinstance(raw_features, list):
            raw_features = []

        brand_from_features, raw_features = _extract_brand_from_raw_features(raw_features)

        raw_brand = _first_present(item, ["marca", "brand", "brand_name"])
        brand_name: Optional[str] = None
        if isinstance(raw_brand, dict):
            possible_name = raw_brand.get("name")
            if possible_name:
                brand_name = str(possible_name).strip()
        elif raw_brand:
            brand_name = str(raw_brand).strip()

        if not brand_name and brand_from_features:
            brand_name = brand_from_features

        if seller and brand_name and seller.lower() == brand_name.lower():
            brand_name = None

        if not brand_name and title:
            inferred = _extract_brand(title)
            brand_name = inferred.name if inferred else None

        if not brand_name:
            brand_name = "Sin marca"

        brand = BrandSchema(name=brand_name)

        features = _extract_features(metodo_pago, raw_features)
        if sample_logged < 2:
            store_name = _resolve_store_name(item)
            logging.info(
                "Features sample tienda='%s' titulo='%s': %s",
                store_name,
                title or "Sin nombre",
                [
                    {"keyword": feature.keyword, "value": feature.value}
                    for feature in features
                ],
            )
            sample_logged += 1
        total_products += 1
        total_features += len(features)

        if len(features) == 0:
            zero_feature_products += 1
        elif len(features) == 1:
            one_feature_products += 1

        store_name = _resolve_store_name(item)
        store_stats = store_feature_stats.setdefault(
            store_name,
            {"total": 0, "low": 0, "zero": 0, "one": 0},
        )
        store_stats["total"] += 1
        if len(features) <= 1:
            store_stats["low"] += 1
            if len(features) == 0:
                store_stats["zero"] += 1
            else:
                store_stats["one"] += 1
            logging.info(
                "Features bajas en tienda '%s' para '%s': raw=%s, normalizadas=%s",
                store_name,
                title or "Sin nombre",
                len(raw_features),
                len(features),
            )

        normalized.append(
            ProductSchema(
                name=title or "Sin nombre",
                urlAccess=url,
                imageUrl=image_url or None,
                price=price or 0.0,
                regularPrice=regular_price,
                seller=seller,
                brand=brand,
                category=category,
                features=features,
            )
        )

    average_features = (total_features / total_products) if total_products else 0.0
    logging.info(
        "Normalizacion local completada. Total=%s, features_promedio=%.2f, sin_features=%s, con_1_feature=%s",
        total_products,
        average_features,
        zero_feature_products,
        one_feature_products,
    )

    for store_name, stats in store_feature_stats.items():
        if stats["low"]:
            logging.warning(
                "Normalizacion: tienda '%s' con features bajas=%s (0=%s, 1=%s) de %s",
                store_name,
                stats["low"],
                stats["zero"],
                stats["one"],
                stats["total"],
            )
    return normalized
