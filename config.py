import os
from typing import List

from dotenv import load_dotenv

# Carga las variables del archivo .env al entorno de Python
load_dotenv()


def _get_float_env(name: str, default: float) -> float:
	value = os.getenv(name)
	if value is None:
		return default
	try:
		return float(value)
	except ValueError:
		return default


def _get_int_env(name: str, default: int) -> int:
	value = os.getenv(name)
	if value is None:
		return default
	try:
		return int(value)
	except ValueError:
		return default


def _get_list_env(name: str) -> List[str]:
	value = os.getenv(name, "")
	return [item.strip() for item in value.split(",") if item.strip()]


# Variables generales de aplicación
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
APP_ENV = os.getenv("APP_ENV", "development")
APP_HOST = os.getenv("APP_HOST", "localhost")
APP_PORT = _get_int_env("APP_PORT", 8000)

# Variables de scraping (mantiene compatibilidad con nombres anteriores)
SCRAPER_TIMEOUT_SECONDS = _get_float_env(
	"SCRAPER_TIMEOUT_SECONDS",
	_get_float_env("SCRAPER_TIMEOUT", 20.0),
)
SCRAPER_MAX_RETRIES = _get_int_env(
	"SCRAPER_MAX_RETRIES",
	_get_int_env("MAX_RETRIES", 3),
)
SCRAPER_MIN_DELAY_SECONDS = _get_float_env("SCRAPER_MIN_DELAY_SECONDS", 1.1)
SCRAPER_MAX_DELAY_SECONDS = _get_float_env("SCRAPER_MAX_DELAY_SECONDS", 3.2)
SCRAPER_BACKOFF_BASE_SECONDS = _get_float_env("SCRAPER_BACKOFF_BASE_SECONDS", 1.5)
SCRAPER_BACKOFF_MAX_SECONDS = _get_float_env("SCRAPER_BACKOFF_MAX_SECONDS", 35.0)
SCRAPER_PROXIES = _get_list_env("SCRAPER_PROXIES")