from datetime import datetime
import json
import re
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from bs4 import BeautifulSoup, Tag
from app.config import get_logger
from app.core.common import Object

from app.core.task import FutureExecutor
from app.db.mongo import MongoConnection, MongoPool

from .entity import TrekBike
from .trekbikes import TrekService


logger = get_logger(__package__)


def get_bike_list(year: int) -> List[Dict[str, Union[str, int]]]:
    return [{
        'id': int(bike.get('id') or 0),
        'url': str(bike.get('productUrl')),
    } for bike in TrekService.list_bikes_by_year(year=year)]


def get_bike_details(bike_id: int, bike_url: str) -> Tuple[Optional[str], Dict[str, object]]: # NOSONAR
    details: Optional[Object] = None
    try:
        details = Object(TrekService.get_bike_details(model_id=bike_id))
    except Exception as ex:
        ex = RuntimeError(bike_id).with_traceback(ex.__traceback__)
        logger.error(ex, exc_info=True)
    if details is None: return ('details missing', {})
    default_product_code = None
    try:
        if not bike_url.startswith('/bikes/'):
            logger.debug('Bicycle skipped [id=%s,product_url=%s]', bike_id, bike_url)
            return ('invalid bike', details)
        model_name = details.get('name')
        if True in [str(model_name).lower().find(text) >= 0 for text in ['frameset', 'frs', 'f/s']]:
            logger.debug('Bicycle skipped [id=%s,model_name=%s]', bike_id, model_name)
            return ('frameset', details)
        model_name = str(model_name).replace('Edit.', 'Edition')
        brand_name = 'Trek'
        variants = details.get('variants', List[Dict[str, Any]])
        if variants is not None:
            default_product_code = int(variants[0]['code'])
            brand_name = str(variants[0]['brandNameFull'])
        details['defaultProductCode'] = default_product_code
        details['brand'] = brand_name
    except Exception as ex:
        ex = RuntimeError(bike_id).with_traceback(ex.__traceback__)
        logger.error(ex, exc_info=True)
    if default_product_code is None: return ('sku missing', details)
    try:
        details['price'] = details['prices']['consumerPrice']['price']['low']['value'] # type: ignore
    except Exception as ex:
        ex = RuntimeError(bike_id).with_traceback(ex.__traceback__)
        logger.error(ex, exc_info=True)
        return ('msrp missing', details)
    bike_spec: Optional[Object] = None
    try:
        bike_spec = Object(TrekService.get_bike_spec(spec_id=default_product_code))
    except Exception as ex:
        ex = RuntimeError(bike_id).with_traceback(ex.__traceback__)
        logger.error(ex, exc_info=True)
        return ('spec missing', details)
    specs: Dict[str, str] = {}
    spec_items = bike_spec.get('specItems', List[Dict[str, str]])
    if spec_items is None:
        return ('spec missing', details)
    for spec in spec_items:
        spec_name = spec.get('partId')
        spec_value = spec.get('description')
        if spec_name is None or spec_value is None: continue
        if '.' in str(spec_name):
            spec_name = str(spec_name).replace('.', '')
        if specs.get(spec_name) is None:
            specs[spec_name] = spec_value
        else:
            specs[spec_name] = f'{specs[spec_name]}; {spec_value}'
    details['specItems'] = specs
    default_images: Set[str] = set()
    images = details.get('images', Dict[str, List[Dict[str, Any]]])
    colors: Set[str] = set()
    if images is not None:
        for color in images:
            assets = images[color]
            colors.add(color.replace('-', ' ').strip().title())
            if len(assets) == 0: continue
            for asset in assets:
                asset_id = str(asset.get('assetId'))
                asset_variant_list = Object(asset).get('variantList', Set[str])
                if asset_variant_list is not None and str(default_product_code) in asset_variant_list:
                    default_images.add(TrekService.get_bike_image_url(asset_id))
        details['specItems']['Color'] = '; '.join(colors)
        details['defaultImages'] = list(default_images)
        if len(default_images) > 0:
            default_image = None
            for image in default_images:
                if image.lower().endswith('primary'):
                    default_image = image
                    break
            if default_image is None:
                for image in default_images:
                    if image.lower().endswith('portrait'):
                        default_image = image
                        break
            if default_image is None:
                default_image = list(default_images)[0]
            details['defaultImage'] = default_image
    if bike_url is not None:
        categories = [c.replace('-', ' ').strip().title() for c in str(bike_url).split('/')[2:4]]
        details['categories'] = categories
        if len(categories) > 0:
            details['category'] = categories[-1]
        product_content = TrekService.get_bike_product_page(product_url=bike_url)
        soup = BeautifulSoup(markup=product_content, features='html.parser')
        container = soup.find('bike-overview-container', {
            ':product-data': True,
        })
        if isinstance(container, Tag):
            product_data = json.loads(str(container.get(':product-data')))
            description = product_data['copyPositioningStatement']
            details['description'] = description
        reviews = soup.find('product-reviews-header', {
            ':options': True,
        })
        if isinstance(reviews, Tag):
            matches = re.findall(r"productUpc:\s*'(.+)',", str(reviews.get(':options')))
            if len(matches) > 0:
                product_upc = matches[0]
                details['productUpc'] = product_upc
        gender_element = soup.select_one('#gender')
        if isinstance(gender_element, Tag):
            genders = str(gender_element.get('data-gender')).split(',')
            details['genders'] = genders
            gender: Optional[str] = None
            genders_have = lambda gender: gender in genders
            if genders_have('Boys') or genders_have('Girls'): gender = 'Kids'
            elif genders_have('Unisex') or (genders_have('Men') and genders_have('Women')): gender = 'Unisex'
            elif genders_have('Men'): gender = 'Men\'s'
            elif genders_have('Women'): gender = 'Women\'s'
            else: gender = 'Not Designated'
            details['gender'] = gender
    return (None, details)


def extract_important_data(bike: TrekBike):
    data = {
        'url': bike.get('url'),
        'year': bike.get('year'),
    }
    details = bike.get('details', Dict[str, Any])
    if details is not None:
        data['sku'] = details.get('defaultProductCode')
        data['brand'] = details.get('brand')
        data['model'] = details.get('name')
        data['msrp'] = details.get('price')
        data['gender'] = details.get('gender')
        data['gtin'] = details.get('productUpc')
        data['description'] = details.get('description')
        data['categories'] = details.get('categories')
        data['category'] = details.get('category')
        data['images'] = details.get('defaultImages')
        data['defaultImage'] = details.get('defaultImage')
        data['specItems'] = details.get('specItems')
    bike['data'] = data
    return bike


def collect_bike(bike_id: int, bike_url: str, bike_year: int):
    (error, details) = get_bike_details(bike_id=bike_id, bike_url=bike_url)
    return extract_important_data(bike=TrekBike({
        'id': bike_id,
        'url': bike_url,
        'year': bike_year,
        'details': details,
        'error': error,
        'createdAt': datetime.now(),
    }))


def start(mongo: MongoPool, target_collection: str, year: int, parallel_executions: int = 100):
    bike_ids: List[int] = []
    execution = FutureExecutor.parallel(capacity=parallel_executions)
    for bike in get_bike_list(year=year):
        bike_id = bike.get('id')
        bike_url = bike.get('url')
        if bike_id is not None:
            bike_ids.append(int(bike_id))
            execution = execution.run(collect_bike, bike_id, bike_url, year)
    bikes: List[TrekBike] = []
    for doc in (execution.get() or ()):
        if doc is not None:
            bikes.append(doc)
    if len(bikes) > 0:
        conn = mongo.get_connection(connection_type=MongoConnection)
        filter_query = {
            'id': {
                '$in': bike_ids,
            },
            'deleted': {
                '$ne': True,
            },
        }
        new_value = {
            '$set': {
                'deleted': True,
                'deletedAt': datetime.now(),
            },
        }
        with conn:
            conn.update(collection=target_collection, filter=filter_query, value=new_value)
            conn.insert(collection=target_collection, docs=bikes)
    logger.debug('Total inserted documents: %s', len(bikes))
