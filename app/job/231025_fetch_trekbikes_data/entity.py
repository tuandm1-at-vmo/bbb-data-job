from typing import Any, Dict, List, Optional
from app.core.common import Entity
from bson import ObjectId


class TrekBike(Entity):
    _id: Optional[ObjectId] = None
    id: int = 0
    details: Dict[str, Any] = {}
    technicalData: Dict[str, Any] = {}
    data: Dict[str, Any] = {}


class TrekXML(Entity):
    _id: Optional[ObjectId] = None
    order: int = 0
    content: str = ''
    total: int = 0
    includes: List[int] = []