from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional

class FeatureSchema(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    keyword: str
    value: Optional[str] = "No especificado"

class BrandSchema(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    name: str
    description: Optional[str] = None

class ProductSchema(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    name: str
    url_access: str = Field(..., alias="urlAccess")
    image_url: Optional[str] = Field(default=None, alias="imageUrl")
    price: float
    regular_price: Optional[float] = Field(default=None, alias="regularPrice")
    seller: Optional[str] = None
    brand: Optional[BrandSchema] = None
    category: Optional[str] = None
    features: List[FeatureSchema] = Field(default_factory=list)