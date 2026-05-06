import logging
import random
import time
import re
from typing import List, Dict, Optional
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Configuración
PAGE_SLEEP_MIN_SECONDS = 3.0
PAGE_SLEEP_MAX_SECONDS = 7.0

BLOCK_HINTS = (
    "captcha",
    "verifica que no eres un robot",
    "acceso denegado",
    "are you human",
)

logging.getLogger().setLevel(logging.INFO)

def _parse_product_features(html: str) -> List[Dict[str, str]]:
    """Extrae características técnicas reales de la página del producto basándose en el HTML provisto."""
    soup = BeautifulSoup(html, "html.parser")
    features: List[Dict[str, str]] = []
    seen = set()

    # Buscamos todas las filas de todas las tablas de características
    for row in soup.select("tr.andes-table__row, .ui-pdp-specs__table tr, .andes-table__body tr, .ui-pdp-specs__list li"):
        
        # KEY: Atrapamos el th o el div contenedor del título
        key_el = row.select_one("th, .andes-table__header, .ui-pdp-specs__attribute-label, .ui-pdp-list__name")
        
        # VALUE: Atrapamos el td o el span que contiene el valor
        val_el = row.select_one("td, .andes-table__column, .ui-pdp-specs__attribute-value, .ui-pdp-list__value")
        
        if not key_el or not val_el:
            continue
            
        # Al usar separator=" ", nos aseguramos de que si ML esconde el texto adentro de spans o divs, 
        # get_text lo extraiga igual sin pegar las palabras.
        key = key_el.get_text(separator=" ", strip=True).replace(":", "").strip()
        value = val_el.get_text(separator=" ", strip=True).strip()
        
        # Filtrado extra: A veces ML mete un texto de "Ocultar características" que no nos sirve
        if key.lower() == "ver más características" or key == "":
            continue
            
        if key and value and (key, value) not in seen:
            features.append({"keyword": key, "value": value})
            seen.add((key, value))

    return features

def _looks_like_block_page(html: str) -> bool:
    if not html:
        return True
    lowered = html.lower()
    return any(hint in lowered for hint in BLOCK_HINTS)

async def scrape_mercadolibre(
    query: str,
    max_pages: int = 1,
    max_items: int = 15,
    include_details: bool = True,
    headless: bool = True,
) -> List[dict]:
    items_crudos = []
    
    if not query:
        logging.error("Query vacía")
        return []
    
    query_formatted = quote(query.strip().replace(" ", "-"))
    logging.info(f"=== INICIANDO SCRAPING ML | QUERY: '{query}' ===")
    
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    
    driver = None
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        for page_num in range(max_pages):
            if page_num == 0:
                url = f"https://listado.mercadolibre.com.ar/{query_formatted}"
            else:
                offset = page_num * 50 + 1
                url = f"https://listado.mercadolibre.com.ar/{query_formatted}_Desde_{offset}_NoIndex_True"
            
            logging.info(f"👉 Navegando a URL: {url}")
            
            try:
                driver.get(url)
                time.sleep(3)
                
                if _looks_like_block_page(driver.page_source):
                    logging.warning("⚠️ ¡BLOQUEO DETECTADO!")
                    if not headless:
                        input("🔓 Resuelve el captcha y presiona Enter...")
                        driver.refresh()
                        time.sleep(3)
                    else:
                        break
                
                # Scroll para lazy loading
                for scroll_step in range(1, 4):
                    driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {scroll_step/3});")
                    time.sleep(1.5)
                
                html = driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                
                # Seleccionamos las tarjetas
                results = soup.select('div.ui-search-result__wrapper, li.ui-search-layout__item, div.poly-card--grid, div.poly-card--list')

                page_items = []
                seen_urls = set()
                
                for el in results:
                    if 'skeleton' in el.get('class', []):
                        continue

                    # 1. LINK y DESCARTE DE DUPLICADOS (Lo hacemos primero para no procesar al pedo)
                    link_el = el.select_one('a[href]')
                    if not link_el:
                        continue
                    url_producto = link_el['href'].split('#')[0].split('?')[0]
                    
                    if "click1.mercadolibre" in url_producto or url_producto in seen_urls:
                        continue
                    seen_urls.add(url_producto)

                    # 2. TÍTULO
                    title_el = el.select_one('h2, .poly-component__title, .ui-search-item__title')
                    if not title_el or not title_el.get_text(strip=True):
                        continue
                    titulo = title_el.get_text(strip=True)

                    # 3. PRECIO ACTUAL (Sacamos el que NO es tachado)
                    # Buscamos el contenedor principal de precio para no mezclar con el tachado
                    current_price_container = el.select_one('.poly-price__current, .ui-search-price__part--medium')
                    if current_price_container:
                        price_el = current_price_container.select_one('.andes-money-amount__fraction')
                    else:
                        # Fallback
                        price_el = el.select_one('.andes-money-amount__fraction')
                        
                    if not price_el:
                        continue
                    precio_actual = re.sub(r"[^\d]", "", price_el.get_text(strip=True))

                    # 4. PRECIO ANTERIOR (El tachado, dentro de la etiqueta <s> o con clase --previous)
                    previous_price_container = el.select_one('.poly-price__previous, s.andes-money-amount--previous')
                    precio_anterior = None
                    if previous_price_container:
                        regular_price_el = previous_price_container.select_one('.andes-money-amount__fraction')
                        if regular_price_el:
                            precio_anterior = re.sub(r"[^\d]", "", regular_price_el.get_text(strip=True))

                    # 5. IMAGEN
                    img_el = el.select_one('img')
                    url_imagen = None
                    if img_el:
                        url_imagen = img_el.get('data-src') or img_el.get('src') or img_el.get('srcset', '').split(' ')[0]
                    if url_imagen and "data:image/gif" in url_imagen:
                        url_imagen = None

                    # 6. VENDEDOR
                    seller_el = el.select_one('.poly-component__seller, .ui-search-official-store-label')
                    vendedor = seller_el.get_text(strip=True).replace('por ', '') if seller_el else "Mercado Libre"

                    # 7. MÉTODO DE PAGO (Buscamos cuotas)
                    payment_el = el.select_one('.poly-price__installments, .ui-search-item__group__element--installments')
                    metodo_pago = payment_el.get_text(strip=True) if payment_el else "Consultar"

                    page_items.append({
                        "titulo": titulo,
                        "precio_actual": precio_actual,
                        "precio_anterior": precio_anterior,
                        "url": url_producto,
                        "url_imagen": url_imagen,
                        "vendedor": vendedor,
                        "metodo_pago": metodo_pago
                    })

                    if len(items_crudos) + len(page_items) >= max_items:
                        break

                remaining_slots = max_items - len(items_crudos)
                page_items = page_items[:remaining_slots]

                logging.info(f"✅ Página {page_num + 1} procesada: Se extrajeron {len(page_items)} productos únicos.")
                items_crudos.extend(page_items)
                
                # --- EXTRACCIÓN DE DETALLES MEJORADA ---
                if include_details and page_items:
                    logging.info(f"🔍 Extrayendo características detalladas de los {len(page_items)} productos (esto tomará un tiempo)...")
                    
                    for idx, item in enumerate(page_items):
                        try:
                            # 2. AGREGAMOS LA PAUSA ACÁ (antes de cada nuevo producto)
                            # Usamos un random entre 3 y 6 segundos para imitar comportamiento humano
                            pausa = random.uniform(3, 6)
                            logging.debug(f"  ⏳ Esperando {pausa:.2f}s antes de extraer el producto {idx+1}...")
                            time.sleep(pausa)

                            driver.execute_script("window.open('');")
                            driver.switch_to.window(driver.window_handles[-1])
                            driver.get(item["url"])
                            
                            # Esperamos específicamente por la tabla '.andes-table' que es la que pasaste en el HTML
                            try:
                                WebDriverWait(driver, 8).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, ".andes-table"))
                                )
                                time.sleep(1) # Extra buffer para JS
                            except Exception:
                                logging.debug(f"Timeout esperando tabla .andes-table para: {item['titulo'][:20]}")
                            
                            # Si en la página individual de detalle hay una imagen de mejor calidad o precio más exacto,
                            # podríamos pisar los valores acá. Por ahora solo sacamos features.
                            
                            features = _parse_product_features(driver.page_source)
                            if features:
                                item["caracteristicas"] = features
                                logging.info(f"  ✓ [{idx+1}/{len(page_items)}] {len(features)} características extraídas para: {item['titulo'][:20]}...")
                            else:
                                item["caracteristicas"] = []
                                logging.debug(f"  ❌ [{idx+1}/{len(page_items)}] No se encontraron características para: {item['titulo'][:20]}...")
                            
                            # También podemos capturar el método de pago desde la página de detalle
                            # si es que en la tarjeta de búsqueda no aparecía (decía 'Consultar')
                            if item["metodo_pago"] == "Consultar":
                                detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
                                detail_payment = detail_soup.select_one('#pricing_price_subtitle')
                                if detail_payment:
                                    item["metodo_pago"] = detail_payment.get_text(strip=True)

                            driver.close()
                            driver.switch_to.window(driver.window_handles[0])
                        except Exception as e:
                            logging.error(f"❌ Error extrayendo detalles de {item['titulo'][:15]}: {e}")
                            if len(driver.window_handles) > 1:
                                driver.close()
                                driver.switch_to.window(driver.window_handles[0])

                if len(items_crudos) >= max_items:
                    logging.info(f"🛑 Límite de {max_items} productos alcanzado. Cortando paginación.")
                    break
                    
                if page_num < max_pages - 1:
                    time.sleep(random.uniform(PAGE_SLEEP_MIN_SECONDS, PAGE_SLEEP_MAX_SECONDS))
                
            except Exception as e:
                logging.error(f"❌ Error crítico procesando página {page_num + 1}: {e}")
                continue
        
        logging.info(f"🏁 SCRAPING FINALIZADO. Total final: {len(items_crudos)} productos únicos.")
        return items_crudos
        
    except Exception as e:
        logging.error(f"❌ Error fatal en inicialización de Selenium: {e}")
        return []
        
    finally:
        if driver:
            driver.quit()