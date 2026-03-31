import os
import json
import asyncio
import logging
from typing import List
from fastapi import HTTPException
from google import genai

# Importamos el esquema desde nuestra carpeta schemas
from app.schemas.product_schema import ProductSchema

# Inicializamos el cliente acá. Tomará la variable de entorno que ya cargamos en main.py
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

async def normalize_with_ia(items_crudos: List[dict]) -> List[ProductSchema]:
    if not client:
        raise HTTPException(status_code=500, detail="Falta configurar la GEMINI_API_KEY en el .env")
        
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
    {json.dumps(items_crudos, ensure_ascii=False)}
    """
    
    try:
        logging.info("Enviando datos a Gemini para normalización...")
        response = await asyncio.to_thread(
            client.models.generate_content,
            model="gemini-2.5-flash",
            contents=prompt
        )
        
        raw_text = response.text.strip()
        if raw_text.startswith("```json"):
            raw_text = raw_text[7:-3].strip()
        elif raw_text.startswith("```"):
            raw_text = raw_text[3:-3].strip()
            
        data = json.loads(raw_text)
        
        # Mapeamos los diccionarios a nuestras clases Pydantic
        return [ProductSchema(**item) for item in data]
        
    except json.JSONDecodeError:
        logging.error(f"Error parseando JSON de Gemini: {response.text}")
        raise HTTPException(status_code=500, detail="Gemini no devolvió un JSON válido")
    except Exception as e:
        logging.error(f"Error procesando con Gemini: {e}")
        raise HTTPException(status_code=500, detail="Error en la conexión con la IA")