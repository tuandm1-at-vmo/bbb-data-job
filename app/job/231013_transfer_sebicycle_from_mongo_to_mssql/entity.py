from typing import Optional, Union

from app.core.common import Entity
from bson import ObjectId


class SEBicycle(Entity):
    _bbb_transfered: Optional[bool] = None
    _id: Optional[ObjectId] = None
    brand_name: Optional[str] = None
    category_name: Optional[str] = None
    description: Optional[str] = None
    gender: Optional[str] = None
    height: Optional[str] = None
    image_default: Optional[str] = None
    length: Optional[str] = None
    model_name: Optional[str] = None
    msrp: Optional[float] = None
    sku: Optional[Union[str, int]] = None
    source: Optional[str] = None
    type_name: Optional[str] = None
    weight: Optional[str] = None
    width: Optional[str] = None
    year: Optional[int] = None
    status: Optional[str] = None
    bbb_sku: Optional[str] = None


class SEComponent(Entity):
    _bbb_transfered: Optional[bool] = None
    _id: Optional[ObjectId] = None
    comp_name: Optional[str] = None
    value: Optional[str] = None
    sku: Optional[Union[str, int]] = None
    bbb_sku: Optional[str] = None


class SEImage(Entity):
    _bbb_transfered: Optional[bool] = None
    _id: Optional[ObjectId] = None
    sku: Optional[Union[str, int]] = None
    micro: Optional[str] = None
    small: Optional[str] = None
    large: Optional[str] = None
    zoom: Optional[str] = None
    caption: Optional[str] = None
    sort_order: Optional[int] = None
    bbb_sku: Optional[str] = None


class SEBicycleGeometry(Entity):
    _bbb_transfered: Optional[bool] = None
    _id: Optional[ObjectId] = None