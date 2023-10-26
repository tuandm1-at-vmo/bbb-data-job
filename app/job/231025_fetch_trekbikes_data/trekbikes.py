import json
import requests
from typing import Any, Dict, List


_TREKBIKES_BASE_URL = 'https://www.trekbikes.com/us/en_US'
_TREKBIKES_MEDIA_BASE_URL = 'https://media.trekbikes.com/image/upload/f_auto,fl_progressive:semi,q_auto,c_pad'
_MOZILLA_HEADERS = {
    'User-Agent': 'Mozilla/5.0', # imitate sending request from a firefox browser
}


class TrekService:
    ''' Utilities for interacting with TrekBikes.com. '''

    @staticmethod
    def _error_response(res: requests.Response):
        ''' Get a RuntimeError version of a requests.Response. '''
        err = {
            'method': res.request.method,
            'url': res.request.url,
            'status': res.status_code,
            'text': res.text,
        }
        return RuntimeError(json.dumps(err))

    @staticmethod
    def list_bikes_by_year(year: int) -> List[Dict[str, Any]]:
        ''' List all bikes for a specific model year. '''
        url = f'{_TREKBIKES_BASE_URL}/product/archived?modelYear={year}&type=Bikes'
        headers = _MOZILLA_HEADERS
        res = requests.get(url=url, headers=headers, allow_redirects=True)
        if res.status_code == 200:
            data = json.loads(res.text)
            return list(data['data']['results'])
        raise TrekService._error_response(res)

    @staticmethod
    def get_bike_details(model_id: int) -> Dict[str, object]:
        ''' Fetch full information of a specific bike model. '''
        url = f'{_TREKBIKES_BASE_URL}/v1/api/product/{model_id}/full'
        headers = _MOZILLA_HEADERS
        res = requests.get(url=url, headers=headers, allow_redirects=True)
        if res.status_code == 200:
            data = json.loads(res.text)
            return {
                **data['data'],
                'id': model_id,
            }
        raise TrekService._error_response(res)

    @staticmethod
    def get_bike_image_url(asset_id: str, width = 1920, height = 1440):
        ''' Get url for a specific bike asset. '''
        return f'{_TREKBIKES_MEDIA_BASE_URL},w_{width},h_{height}/{asset_id}'

    @staticmethod
    def get_bike_product_page(product_url: str):
        ''' Get raw page content of a specific bike model. '''
        url = f'{_TREKBIKES_BASE_URL}{product_url}'
        headers = _MOZILLA_HEADERS
        res = requests.get(url=url, headers=headers, allow_redirects=True)
        if res.status_code == 200:
            return res.text
        raise TrekService._error_response(res)

    @staticmethod
    def get_bike_spec(spec_id: int) -> Dict[str, object]:
        ''' Fetch information for a specific bike spec. '''
        url = f'{_TREKBIKES_BASE_URL}/product/spec/{spec_id}'
        headers = _MOZILLA_HEADERS
        res = requests.get(url=url, headers=headers, allow_redirects=True)
        if res.status_code == 200:
            data = json.loads(res.text)
            return {
                **data['data'],
                'id': spec_id,
            }
        raise TrekService._error_response(res)

    @staticmethod
    def get_bike_technical_data(model_id: int) -> Dict[str, object]:
        ''' Get technical data for a specific bike model. '''
        url = f'{_TREKBIKES_BASE_URL}/product/spec/technicalData/{model_id}'
        headers = _MOZILLA_HEADERS
        res = requests.get(url=url, headers=headers, allow_redirects=True)
        if res.status_code == 200:
            data = json.loads(res.text)
            return {
                **data['data'],
                'id': model_id,
            }
        raise TrekService._error_response(res)
