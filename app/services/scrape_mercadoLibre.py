import logging
from typing import List
import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import HTTPException
# Carga las variables del archivo .env al entorno de Python
load_dotenv()

async def scrape_mercadolibre(query: str, limit: int = 3) -> List[dict]:
    query_formatted = query.replace(" ", "-")
    url = f"https://listado.mercadolibre.com.ar/{query_formatted}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "es-AR,es;q=0.9"
    }
    
    logging.info(f"Scrapeando URL: {url}")
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                raise HTTPException(status_code=resp.status, detail="Error al acceder a MercadoLibre")
            html = await resp.text()
            
    soup = BeautifulSoup(html, 'html.parser')
    
    # ML suele agrupar los resultados en etiquetas 'li' con esta clase
    results = soup.select('li.ui-search-layout__item')
    logging.info(f"El scraper encontró {len(results)} contenedores de productos en el HTML.")
    
    
    if len(results) == 0:
        logging.warning("No se encontraron productos. Mostrando los primeros 500 caracteres del HTML:")
        logging.warning(html[:500])
    
    items_crudos = []
    
    for el in results[:limit]:
        # Buscamos con los selectores clásicos y también con los nuevos de ML
        title_el = el.select_one('h2') or el.select_one('.poly-component__title')
        
        # Buscamos específicamente el precio tachado (anterior)
        previous_price_el = el.select_one('.andes-money-amount--previous .andes-money-amount__fraction')

        # Buscamos el precio actual
        current_price_el = el.select_one('.poly-price__current .andes-money-amount__fraction') or el.select_one('.andes-money-amount__fraction')

        seller_el = el.select_one('.poly-component__seller') or el.select_one('.ui-search-official-store-label')
        installments_el = el.select_one('.poly-price__installments') or el.select_one('.ui-search-installments')
        
        url_el = el.select_one('a')
        
        # Modo depuración: si falta algo, que la consola nos "buchonee" qué fue
        if not title_el or not url_el or not current_price_el:
                    continue
            
        precio_actual = current_price_el.get_text(strip=True).replace('.', '')
        precio_anterior = previous_price_el.get_text(strip=True).replace('.', '') if previous_price_el else None

        items_crudos.append({
                    "titulo": title_el.get_text(strip=True),
                    "precio_actual": precio_actual,
                    "precio_anterior": precio_anterior,
                    "vendedor": seller_el.get_text(strip=True).replace('por ', '') if seller_el else "Mercado Libre",
                    "metodo_pago": installments_el.get_text(strip=True) if installments_el else "Contado",
                    "url": url_el.get('href')
                })
            
    logging.info(f"Productos extraídos exitosamente: {len(items_crudos)}")
    return items_crudos