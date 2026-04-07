import os
import json
import asyncio
import logging
from typing import List, Optional
from fastapi import HTTPException
from google import genai

# Importamos el esquema desde nuestra carpeta schemas
from app.schemas.product_schema import ProductSchema

# Inicializamos el cliente acá. Tomará la variable de entorno que ya cargamos en main.py
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

# Bajamos el lote a 5 para no saturar los tokens por minuto
BATCH_SIZE = 5


def _extract_json_payload(raw_text: str) -> Optional[object]:
    """
    Extrae un JSON válido desde el texto devuelto por el modelo.
    """
    cleaned = raw_text.strip()
    if "```json" in cleaned:
        cleaned = cleaned.split("```json", 1)[1].split("```", 1)[0].strip()
    elif "```" in cleaned:
        cleaned = cleaned.split("```", 1)[1].split("```", 1)[0].strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    start = cleaned.find("[")
    end = cleaned.rfind("]")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(cleaned[start : end + 1])
        except json.JSONDecodeError:
            return None

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return [json.loads(cleaned[start : end + 1])]
        except json.JSONDecodeError:
            return None

    return None

async def normalize_with_ia(items_crudos: List[dict]) -> List[ProductSchema]:
    if not client:
        raise HTTPException(status_code=500, detail="Falta configurar la GEMINI_API_KEY en el .env")
        
    if not items_crudos:
        return []
    
    batches = [items_crudos[i:i + BATCH_SIZE] for i in range(0, len(items_crudos), BATCH_SIZE)]
    
    logging.info(f"Iniciando normalización de {len(items_crudos)} productos en {len(batches)} lotes.")
    
    # 2. Procesamos los lotes. 
    # Usamos un bucle simple para no saturar el Rate Limit de la cuenta gratuita.
    all_normalized_products = []

    for i, batch in enumerate(batches):
        logging.info(f"Procesando lote {i+1}/{len(batches)}...")
        normalized_batch = await process_batch(batch)
        all_normalized_products.extend(normalized_batch)
        
        # Pausa preventiva entre lotes exitosos para evitar llegar al límite
        if i < len(batches) - 1:
            await asyncio.sleep(3)

    logging.info(f"Normalización completada. Total: {len(all_normalized_products)} productos.")
    return all_normalized_products
    

async def process_batch(batch_items: List[dict], max_retries: int = 3) -> List[ProductSchema]:
    """Procesa un pequeño grupo de productos con Gemini."""
    prompt = f"""
    Actúa como un experto en extracción de datos de hardware y e-commerce.
    Te daré una lista de productos en formato JSON extraídos de una tienda.
    Tu objetivo es normalizarlos y devolver ÚNICAMENTE un arreglo JSON válido que coincida con este esquema exacto:
    
    [
      {{
        "name": "Nombre limpio y profesional del producto",
        "urlAccess": "La misma url cruda que te envié",
        "price": 120000.00,
        "regularPrice": 150000.00,
        "category": "Categoría general deducida (ej. Notebooks, Monitores, Placas de Video)",
        "seller": "Nombre de la tienda",
        "brand": {{
            "name": "Nombre de la marca (ej. Asus, HP, Lenovo)"
        }},
        "features": [
          {{"keyword": "procesador", "value": "Intel Core i5"}},
          {{"keyword": "memoria_ram", "value": "16GB"}},
          {{"keyword": "almacenamiento", "value": "512GB SSD"}},
          {{"keyword": "placa_video", "value": "RTX 3060"}},
          {{"keyword": "metodo_pago", "value": "6 cuotas sin interés - Visa / 12 cuotas sin interés - Mastercard / etc."}}
        ]
      }}
    ]
    
    Reglas estrictas:
    1. 'price' es el precio final ('precio_actual'). 'regularPrice' es el precio original sin descuento ('precio_anterior'). Si el producto no tiene descuento, omite 'regularPrice' (null). Ambos deben ser de tipo numérico (float).
    2. Infiere la 'category' y la marca ('brand.name') a partir del título del producto.
    3. Extrae características técnicas en el arreglo 'features'.
    4. NO incluyas formato Markdown (sin ```json), solo devuelve texto de JSON puro.
    5. 'seller' es el vendedor directo.
    6. Feature 'metodo_pago' debe describir las opciones de pago disponibles (ej. "Contado", "6 cuotas sin interés - Visa", etc.)
    7. Si no puedes inferir alguna información, deja el campo como null o vacío, pero siempre devuelve un JSON válido que respete el esquema.
    
    Datos crudos a procesar:
    {json.dumps(batch_items, ensure_ascii=False)}
    """
    for attempt in range(max_retries):
        try:
            response = await asyncio.to_thread(
                client.models.generate_content,
                model="gemini-2.5-flash-lite", 
                contents=prompt
            )
            
            raw_text = response.text.strip()

            data = _extract_json_payload(raw_text)
            if data is None:
                logging.warning("Respuesta IA sin JSON válido. Reintentando lote.")
                continue

            if not isinstance(data, list):
                logging.warning("La respuesta IA no devolvió una lista. Reintentando lote.")
                continue

            normalized_items: List[ProductSchema] = []
            for item in data:
                try:
                    normalized_items.append(ProductSchema(**item))
                except Exception as exc:
                    logging.warning(f"Item inválido en lote, se omite. Error: {str(exc)}")

            return normalized_items
            
        except Exception as e:
            error_msg = str(e)

            if "429" in error_msg or "RESOURCE_EXHAUSTED" in error_msg:
                espera = 45 # Segundos seguros basados en el log
                logging.warning(f"Límite 429 alcanzado. Intento {attempt + 1}/{max_retries}. Pausando {espera}s...")
                await asyncio.sleep(espera)
            else:
                logging.error(f"Error crítico parseando lote: {error_msg}")
                break
                
    return [] # Retorna vacío si agotó los reintentos