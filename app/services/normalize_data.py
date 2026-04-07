import logging
import re
from typing import Dict, List, Optional, Tuple

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
]

PROCESSOR_PATTERN = re.compile(
    r"(intel\s*core\s*i[3579]|ryzen\s*[3579]\s*\d{3,4}x?|pentium|celeron)",
    re.IGNORECASE,
)
RAM_PATTERN = re.compile(r"(\d{1,3})\s*gb\s*ram", re.IGNORECASE)
RAM_FALLBACK_PATTERN = re.compile(r"\b(\d{1,3})\s*gb\b", re.IGNORECASE)
STORAGE_PATTERN = re.compile(r"(\d{2,4})\s*(gb|tb)\s*(ssd|hdd|nvme)?", re.IGNORECASE)
GPU_PATTERN = re.compile(
    r"(rtx\s*\d{3,4}|gtx\s*\d{3,4}|radeon\s*(rx\s*)?\d{3,4}|intel\s*iris|uhd\s*graphics)",
    re.IGNORECASE,
)
RESOLUTION_PATTERN = re.compile(r"(\d{3,4}x\d{3,4}|fhd|full\s*hd|qhd|uhd|4k)", re.IGNORECASE)
REFRESH_RATE_PATTERN = re.compile(r"(\d{2,3})\s*hz", re.IGNORECASE)
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


def _to_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    cleaned = value.replace(".", "").replace(",", ".").strip()
    try:
        return float(cleaned)
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
        if any(f.keyword == keyword and f.value == value for f in features):
            return
        features.append(FeatureSchema(keyword=keyword, value=value))


def _extract_features(
    title: str,
    metodo_pago: Optional[str],
    raw_features: Optional[List[dict]] = None,
) -> List[FeatureSchema]:
    features: List[FeatureSchema] = []
    match = PROCESSOR_PATTERN.search(title)
    _add_feature(features, "procesador", match.group(1) if match else None)

    match = RAM_PATTERN.search(title)
    if match:
        _add_feature(features, "memoria_ram", f"{match.group(1)}GB")
    else:
        match = RAM_FALLBACK_PATTERN.search(title)
        _add_feature(features, "memoria_ram", f"{match.group(1)}GB" if match else None)

    match = STORAGE_PATTERN.search(title)
    if match:
        size = match.group(1)
        unit = match.group(2).upper()
        kind = (match.group(3) or "").upper().strip()
        storage_value = f"{size}{unit} {kind}".strip()
        _add_feature(features, "almacenamiento", storage_value)

    match = GPU_PATTERN.search(title)
    _add_feature(features, "placa_video", match.group(1) if match else None)

    match = RESOLUTION_PATTERN.search(title)
    if match:
        _add_feature(features, "resolucion", match.group(1).upper())

    match = REFRESH_RATE_PATTERN.search(title)
    if match:
        _add_feature(features, "frecuencia", f"{match.group(1)}Hz")

    match = SCREEN_SIZE_PATTERN.search(title)
    if match:
        pulgadas = match.group(1).replace(",", ".")
        _add_feature(features, "pulgadas", pulgadas)

    match = PANEL_PATTERN.search(title)
    _add_feature(features, "panel", match.group(1).upper() if match else None)

    match = STORAGE_TYPE_PATTERN.search(title)
    _add_feature(features, "tipo_almacenamiento", match.group(1).upper() if match else None)

    match = CONNECTION_PATTERN.search(title)
    _add_feature(features, "conexion", match.group(1).upper() if match else None)

    match = DPI_PATTERN.search(title)
    _add_feature(features, "dpi", match.group(1) if match else None)

    match = SWITCH_PATTERN.search(title)
    if match:
        _add_feature(features, "switch", match.group(1).upper())

    match = LAYOUT_PATTERN.search(title)
    _add_feature(features, "layout", match.group(1).upper() if match else None)

    match = MIC_PATTERN.search(title)
    if match:
        _add_feature(features, "microfono", "Si")

    match = KEYBOARD_LANG_PATTERN.search(title)
    if match:
        lang = match.group(1).lower()
        if lang in {"espanol", "español", "spanish"}:
            value = "ES"
        elif lang in {"ingles", "inglés", "english"}:
            value = "EN"
        elif lang in {"portugues", "portugués", "pt", "br"}:
            value = "PT"
        else:
            value = lang.upper()
        _add_feature(features, "idioma_teclado", value)

    match = DIMENSIONS_PATTERN.search(title)
    if match:
        x = match.group(1).replace(",", ".")
        y = match.group(2).replace(",", ".")
        z = match.group(3).replace(",", ".")
        unit = match.group(4).lower()
        _add_feature(features, "dimensiones", f"{x}x{y}x{z} {unit}")

    if raw_features:
        for raw in raw_features:
            keyword = (raw.get("keyword") or "").strip()
            value = (raw.get("value") or "").strip()
            if keyword and value:
                _add_feature(features, keyword, value)

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
        title = (item.get("titulo") or "").strip()
        price = _to_float(item.get("precio_actual"))
        regular_price = _to_float(item.get("precio_anterior"))
        seller = item.get("vendedor")
        metodo_pago = item.get("metodo_pago")
        url = item.get("url") or ""

        brand = _extract_brand(title) if title else None
        category = _extract_category(title) if title else None
        raw_features = item.get("caracteristicas") or []
        features = _extract_features(title, metodo_pago, raw_features)

        normalized.append(
            ProductSchema(
                name=title or "Sin nombre",
                urlAccess=url,
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
