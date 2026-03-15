import requests
import os
from pathlib import Path


# List of urls, for test connection and choose working one.
urls = ['https://swapi.dev/api',
        'https://swapi.py4e.com/api',
        'https://www.swapi.tech/api']


# Create class APIRequester.
class APIRequester():
    def __init__(self, base_url):
        self.base_url = base_url

    def get(self, sw_type_get=''):
        self.bind_url = f"{self.base_url.rstrip('/')}/{sw_type_get.lstrip('/')}"
        try:
            response = requests.get(self.bind_url)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Возникла ошибка при выполнении запроса {e}")
            return None


# Create subclass SWRequester.
class SWRequester(APIRequester):
    def __init__(self, base_url):
        super().__init__(base_url)

    def get_sw_categories(self):
        answer = self.get()
        self.sw_types = answer.json()
        return self.sw_types.keys()

    def get_sw_info(self, sw_type):
        response = self.get(f'{sw_type}/')
        return response.text


# Function to save SW data.
def save_sw_data():
    for url in urls:
        try:
            res_1 = requests.get(url)
            res_1.raise_for_status()
        except Exception:
            continue
        else:
            base_url = url
    request_1 = SWRequester('https://swapi.dev/api')

    # Get list of categories.
    categories = request_1.get_sw_categories()
    
    # Creating directory /data.
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Get and save memebers of categorie.
    for category in categories:
        sw_type = request_1.get_sw_info(category)
        file_path = f'data/{category}.txt'
        with open(file_path, 'w', encoding='utf-8') as sw_writer:
            sw_writer.write(sw_type)

    return 'Сохранение файлов завершено'


if __name__ == '__main__':
    
    print(save_sw_data())
