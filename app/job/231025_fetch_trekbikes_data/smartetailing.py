from typing import Any, Dict, List, Union

from .entity import TrekBike


class SmartEtailingService:
    ''' Utilities for converting contents to SmartEtailing.com's XML files. '''

    @staticmethod
    def _escape_special_characters(content):
        if content is None: return ''
        return str(content) \
            .replace('<', '&#60;') \
            .replace('>', '&#62;') \
            .replace('&', '&#38;')

    @staticmethod
    def create_xml(bikes: List[TrekBike]):
        ''' This method is to create an XML content that contains the information of a given list of bikes. '''
        products = [SmartEtailingService._create_product_xml_element(bike) for bike in bikes]
        return f'''<?xml version="1.0" encoding="UTF-8"?><Products xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">{''.join(products)}</Products>'''

    @staticmethod
    def _create_product_xml_element(bike: TrekBike):
        data = bike.get('data', Dict[str, Any])
        if data is None: return ''
        sku = data.get('sku')
        brand = SmartEtailingService._escape_special_characters(data.get('brand'))
        model = SmartEtailingService._escape_special_characters(data.get('model'))
        year = data.get('year')
        description = SmartEtailingService._escape_special_characters(data.get('description'))
        msrp = data.get('msrp')
        images = data.get('images')
        default_image = data.get('defaultImage')
        gtin = data.get('gtin')
        gender = data.get('gender')
        categories = data.get('categories')
        spec_items = data.get('specItems')
        return f'''<Product><modelSku>{sku}</modelSku><modelName>{model}</modelName><modelYear>{year}</modelYear><modelDescription><![CDATA[{description}]]></modelDescription><gender>{gender}</gender><Brand><name>{brand}</name></Brand><Specs>{SmartEtailingService._create_specs_xml_element(spec_items=spec_items)}</Specs><Categories>{SmartEtailingService._create_categories_xml_element(categories)}</Categories><ModelImages>{SmartEtailingService._create_images_xml_element(images=images)}</ModelImages><VariationCombinations><VariationCombination><mpn/><gtin1>{gtin}</gtin1><msrp>{msrp}</msrp><length>{0}</length><width>{0}</width><height>{0}</height><weight>{0}</weight></VariationCombination></VariationCombinations><bbb><DefaultImage>{default_image}</DefaultImage></bbb></Product>'''

    @staticmethod
    def _create_spec_xml_element(spec_name: str, spec_value: str):
        spec_name = SmartEtailingService._escape_special_characters(spec_name)
        spec_value = SmartEtailingService._escape_special_characters(spec_value)
        return f'''<Spec><name>{spec_name}</name><value>{spec_value}</value></Spec>'''

    @staticmethod
    def _create_specs_xml_element(spec_items: Union[Dict[str, str], None]):
        if spec_items is None: return ''
        return ''.join([SmartEtailingService._create_spec_xml_element(spec_name=spec_name, spec_value=spec_items[spec_name]) for spec_name in spec_items.keys()])

    @staticmethod
    def _create_image_xml_element(image: str, order = 0):
        image = SmartEtailingService._escape_special_characters(image)
        return f'''<ModelImage><micro url="{image}">{image}</micro><small url="{image}">{image}</small><large url="{image}">{image}</large><zoom url="{image}">{image}</zoom><sortOrder>{order}</sortOrder></ModelImage>'''

    @staticmethod
    def _create_images_xml_element(images: Union[List[str], None]):
        if images is None: return ''
        return ''.join([SmartEtailingService._create_image_xml_element(image=images[index], order=index) for index in range(len(images))])

    @staticmethod
    def _create_category_xml_element(category: str):
        category_id = 0
        category_name = SmartEtailingService._escape_special_characters(category)
        return f'''<Category><ID>{category_id}</ID><name>{category_name}</name></Category>'''

    @staticmethod
    def _create_categories_xml_element(categories: Union[List[str], None]):
        if categories is None: return ''
        return ''.join([SmartEtailingService._create_category_xml_element(category=category) for category in categories])
