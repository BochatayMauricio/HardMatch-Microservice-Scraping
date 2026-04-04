import logging
import asyncio
from typing import List
import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import quote
# Carga las variables del archivo .env al entorno de Python
load_dotenv()

async def scrape_mercadolibre(query: str, max_pages: int = 1) -> List[dict]:
    """
    Scrapea múltiples páginas de Mercado Libre para enriquecer la BD.
    max_pages: Cantidad de páginas a scrapear (cada página trae ~50 productos).
    """
    # 1. Formateo de URL Universal y seguro
    # Reemplazamos espacios por guiones y codificamos caracteres especiales
    query_formatted = quote(query.strip().replace(" ", "-"))
    base_url = f"https://listado.mercadolibre.com.ar/{query_formatted}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "es-AR,es;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive"
    }
    
    items_crudos = []
    
    async with aiohttp.ClientSession() as session:
        for page in range(max_pages):
            # 2. Lógica de paginación de Mercado Libre (50 productos por pagina)
            offset = 1 + (page * 50)
            url = base_url if page == 0 else f"{base_url}_Desde_{offset}_NoIndex_True"
            
            logging.info(f"Scrapeando página {page + 1}/{max_pages} - URL: {url}")
            
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        logging.warning(f"Error {resp.status} al acceder a ML en la pág {page + 1}")
                        break # Salimos del loop si ML nos bloquea o hay un error de red
                    
                    html = await resp.text()
            except Exception as e:
                logging.error(f"Fallo en la petición HTTP: {str(e)}")
                break
                
            soup = BeautifulSoup(html, 'html.parser')
            # Selector de los contenedores de publicaciones
            results = soup.select('li.ui-search-layout__item')
            
            if not results:
                logging.info("No se encontraron más productos. Fin de la búsqueda.")
                break
                
            for el in results:
                # 3. Extracción robusta de los datos
                title_el = el.select_one('h2.ui-search-item__title') or el.select_one('.poly-component__title')
                current_price_el = el.select_one('.poly-price__current .andes-money-amount__fraction') or el.select_one('.andes-money-amount__fraction')
                previous_price_el = el.select_one('.poly-price__previous .andes-money-amount__fraction')
                seller_el = el.select_one('.poly-component__seller') or el.select_one('.ui-search-official-store-label')
                installments_el = el.select_one('.poly-price__installments') or el.select_one('.ui-search-installments')
                url_el = el.select_one('a')
                
                if not title_el or not current_price_el or not url_el:
                    continue
                    
                precio_actual = current_price_el.get_text(strip=True).replace('.', '')
                precio_anterior = previous_price_el.get_text(strip=True).replace('.', '') if previous_price_el else None
                
                items_crudos.append({
                    "titulo": title_el.get_text(strip=True),
                    "precio_actual": precio_actual,
                    "precio_anterior": precio_anterior,
                    "vendedor": seller_el.get_text(strip=True).replace('por ', '') if seller_el else "Mercado Libre",
                    "metodo_pago": installments_el.get_text(strip=True) if installments_el else "No especificado",
                    "url": url_el.get('href', '')
                })
            
            # 4. Rate Limiting (Pausa entre páginas para evitar bloqueos)
            if page < max_pages - 1:
                await asyncio.sleep(2.5) 
            
    logging.info(f"Scraping finalizado. Se extrajeron {len(items_crudos)} productos crudos.")
    return items_crudos