import re
from typing import List, Tuple

QUERY_REPLACEMENTS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"\bnotebooks\b", re.IGNORECASE), "notebook"),
    (re.compile(r"\bmonitores\b", re.IGNORECASE), "monitor"),
    (re.compile(r"\bprocesadores\b", re.IGNORECASE), "procesador"),
    (re.compile(r"\bplacas\s+de\s+video(s)?\b", re.IGNORECASE), "placa de video"),
    (re.compile(r"\bmemorias\s+ram\b", re.IGNORECASE), "memoria ram"),
    (re.compile(r"\bteclados\b", re.IGNORECASE), "teclado"),
    (re.compile(r"\bmouses\b", re.IGNORECASE), "mouse"),
    (re.compile(r"\btablets\b", re.IGNORECASE), "tablet"),
]


def normalize_query(raw_query: str) -> str:
    """
    Normaliza queries para mejorar compatibilidad entre buscadores.
    """
    if not raw_query:
        return raw_query

    cleaned = re.sub(r"\s+", " ", raw_query.strip())
    if not cleaned:
        return cleaned

    normalized = cleaned.lower()
    for pattern, replacement in QUERY_REPLACEMENTS:
        normalized = pattern.sub(replacement, normalized)

    return normalized
