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


def _extract_brand(title: str) -> Optional[BrandSchema]:
    lowered = title.lower()
    for key, name in BRAND_KEYWORDS.items():
        if re.search(rf"\b{re.escape(key)}\b", lowered):
            return BrandSchema(name=name)
    return None


def _extract_category(title: str) -> Optional[str]:
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


# def _extract_features(
#     title: str,
#     metodo_pago: Optional[str],
#     raw_features: Optional[List[dict]] = None,
# ) -> List[FeatureSchema]:
#     features: List[FeatureSchema] = []
#     match = PROCESSOR_PATTERN.search(title)
#     _add_feature(features, "procesador", match.group(1) if match else None)

#     match = RAM_PATTERN.search(title)
#     if match:
#         _add_feature(features, "memoria_ram", f"{match.group(1)}GB")
#     else:
#         match = RAM_FALLBACK_PATTERN.search(title)
#         _add_feature(features, "memoria_ram", f"{match.group(1)}GB" if match else None)

#     match = STORAGE_PATTERN.search(title)
#     if match:
#         size = match.group(1)
#         unit = match.group(2).upper()
#         kind = (match.group(3) or "").upper().strip()
#         storage_value = f"{size}{unit} {kind}".strip()
#         _add_feature(features, "almacenamiento", storage_value)

#     match = GPU_PATTERN.search(title)
#     _add_feature(features, "placa_video", match.group(1) if match else None)

#     match = VRAM_PATTERN.search(title)
#     if match:
#         _add_feature(features, "vram", f"{match.group(1)}GB")

#     match = RESOLUTION_PATTERN.search(title)
#     if match:
#         _add_feature(features, "resolucion", match.group(1).upper())

#     match = REFRESH_RATE_PATTERN.search(title)
#     if match:
#         _add_feature(features, "frecuencia", f"{match.group(1)}Hz")

#     match = SCREEN_SIZE_PATTERN.search(title)
#     if match:
#         pulgadas = match.group(1).replace(",", ".")
#         _add_feature(features, "pulgadas", pulgadas)

#     match = PANEL_PATTERN.search(title)
#     _add_feature(features, "panel", match.group(1).upper() if match else None)

#     match = STORAGE_TYPE_PATTERN.search(title)
#     _add_feature(features, "tipo_almacenamiento", match.group(1).upper() if match else None)

#     match = CONNECTION_PATTERN.search(title)
#     _add_feature(features, "conexion", match.group(1).upper() if match else None)

#     match = DPI_PATTERN.search(title)
#     _add_feature(features, "dpi", match.group(1) if match else None)

#     match = SWITCH_PATTERN.search(title)
#     if match:
#         _add_feature(features, "switch", match.group(1).upper())

#     match = LAYOUT_PATTERN.search(title)
#     _add_feature(features, "layout", match.group(1).upper() if match else None)

#     match = MIC_PATTERN.search(title)
#     if match:
#         _add_feature(features, "microfono", "Si")

#     match = KEYBOARD_LANG_PATTERN.search(title)
#     if match:
#         lang = match.group(1).lower()
#         if lang in {"espanol", "español", "spanish"}:
#             value = "ES"
#         elif lang in {"ingles", "inglés", "english"}:
#             value = "EN"
#         elif lang in {"portugues", "portugués", "pt", "br"}:
#             value = "PT"
#         else:
#             value = lang.upper()
#         _add_feature(features, "idioma_teclado", value)

#     match = DIMENSIONS_PATTERN.search(title)
#     if match:
#         x = match.group(1).replace(",", ".")
#         y = match.group(2).replace(",", ".")
#         z = match.group(3).replace(",", ".")
#         unit = match.group(4).lower()
#         _add_feature(features, "dimensiones", f"{x}x{y}x{z} {unit}")

#     match = OS_PATTERN.search(title)
#     if match:
#         os_name = match.group(1).lower().replace(" ", "")
#         if os_name == "win11":
#             os_name = "windows11"
#         elif os_name == "win10":
#             os_name = "windows10"
#         _add_feature(features, "sistema_operativo", os_name)

#     if raw_features:
#         for raw in raw_features:
#             if not isinstance(raw, dict):
#                 continue

#             keyword_raw = str(raw.get("keyword") or raw.get("name") or "").strip()
#             value = str(raw.get("value") or raw.get("valor") or "").strip()
#             if not keyword_raw or not value:
#                 continue

#             keyword = _normalize_keyword(keyword_raw)
#             _add_feature(features, keyword, value)

#     if metodo_pago and metodo_pago.strip() and metodo_pago != "No especificado":
#         _add_feature(features, "metodo_pago", metodo_pago.strip())

#     return features

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
    for item in items_crudos:
        if not isinstance(item, dict):
            continue

        title = str(
            _first_present(item, ["titulo", "title", "name", "productName"]) or ""
        ).strip()
        url = str(_first_present(item, ["url", "urlAccess", "link"]) or "").strip()
        image_url = str(
            _first_present(item, ["url_imagen", "image_url", "imageUrl", "image", "thumbnail"])
            or ""
        ).strip()
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

        raw_brand = _first_present(item, ["marca", "brand", "brand_name"])
        brand_name: Optional[str] = None
        if isinstance(raw_brand, dict):
            possible_name = raw_brand.get("name")
            if possible_name:
                brand_name = str(possible_name).strip()
        elif raw_brand:
            brand_name = str(raw_brand).strip()

        brand = BrandSchema(name=brand_name) if brand_name else (_extract_brand(title) if title else None)

        raw_features = _first_present(item, ["caracteristicas", "features"]) or []
        if not isinstance(raw_features, list):
            raw_features = []

        features = _extract_features(metodo_pago, raw_features)

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

    logging.info(f"Normalizacion local completada. Total: {len(normalized)} productos.")
    return normalized
