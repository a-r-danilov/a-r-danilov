import os
import json
from typing import List, Optional, Dict, Any
from datetime import datetime
from decimal import Decimal
from models import Car, CarFullInfo, CarStatus, Model, ModelSaleStats, Sale

LINE_LENGTH = 501


class CarService:
    def __init__(self, root_directory_path: str) -> None:
        self.root_directory_path = root_directory_path
        os.makedirs(root_directory_path, exist_ok=True)

        # Инициализация файлов.
        self.cars_path = os.path.join(root_directory_path, 'cars.txt')
        self.cars_index_path = os.path.join(
            root_directory_path, 'cars_index.txt')
        self.models_path = os.path.join(root_directory_path, 'models.txt')
        self.models_index_path = os.path.join(
            root_directory_path, 'models_index.txt')
        self.sales_path = os.path.join(root_directory_path, 'sales.txt')
        self.sales_index_path = os.path.join(
            root_directory_path, 'sales_index.txt')
        # Создание файлов.
        for path in [
            self.cars_path, self.cars_index_path,
            self.models_path, self.models_index_path,
            self.sales_path, self.sales_index_path
        ]:
            if not os.path.exists(path):
                open(path, 'w').close()

    # Чтение файлов с индексами.
    def _read_index(self, index_path: str) -> List[Dict[str, Any]]:
        """
        Чтение индексного файла в память.
        
        Формат индексного файла: каждая строка содержит ключ
        и номер строки в файле данных, разделенные ';'
        
        Args:
            index_path: Путь к индексному файлу
            
        Returns:
            Список словарей с ключами 'key' и 'line_num'
        """
        if not os.path.exists(index_path) or os.path.getsize(index_path) == 0:
            return []
        
        with open(index_path, 'r') as f:
            lines = f.readlines()
        
        index = []
        for line in lines:
            key, line_num = line.strip().split(';')
            index.append({'key': key, 'line_num': int(line_num)})
        return index
    
    # Запись индеса в файл.
    def _write_index(
        self, index_path: str, index: List[Dict[str, Any]]
    ) -> None:
        """
        Запись файла с индексами на диск.
        
        Args:
            index_path: Путь к индексному файлу
            index: Список словарей с ключами 'key' и 'line_num'
        """
        with open(index_path, 'w') as f:
            for item in index:
                f.write(f'{item['key']};{item['line_num']}\n')
    
    # Построчное чтение файлов.
    def _read_line(self, file_path: str, line_num: int) -> Optional[dict]:
        """
        Чтение конкретной строки из файла данных.
        
        Args:
            file_path: Путь к файлу данных
            line_num: Номер строки для чтения
            
        Returns:
            Распарсенный JSON-объект или None, если строка пустая
        """
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            return None
        
        with open(file_path, 'r') as f:
            f.seek(line_num * LINE_LENGTH)
            line = f.read(LINE_LENGTH).strip()
            if not line:
                return None
            return json.loads(line)
    
    # Запись в файл.
    def _write_line(self, file_path: str, line_num: int, data: dict) -> None:
        """
        Запись данных в конкретную строку файла.
        
        Args:
            file_path: Путь к файлу данных
            line_num: Номер строки для записи
            data: Данные для записи (словарь)
        """
        json_str = json.dumps(data, default=self._json_serializer)
        padded_line = json_str.ljust(LINE_LENGTH - 1) + '\n'
        
        with open(file_path, 'r+') as f:
            f.seek(line_num * LINE_LENGTH)
            f.write(padded_line)
    
    # Переход на следующую строку.
    def _append_line(self, file_path: str, data: dict) -> int:
        """
        Добавление новой строки в конец файла.
        
        Args:
            file_path: Путь к файлу данных
            data: Данные для записи (словарь)
            
        Returns:
            Номер строки, в которую были записаны данные
        """
        json_str = json.dumps(data, default=self._json_serializer)
        padded_line = json_str.ljust(LINE_LENGTH - 1) + '\n'
        
        with open(file_path, 'a') as f:
            pos = f.tell()
            f.write(padded_line)
            line_num = pos // LINE_LENGTH
        return line_num
    
    # Сериализация объектов.
    # Получение строк и файлов через формат json.
    def _json_serializer(self, obj: Any) -> Any:
        """
        Сериализатор для объектов, не поддерживаемых стандартным json.dumps.
        
        Поддерживает:
        - Decimal: преобразуется в строку
        - datetime: преобразуется в строку в формате ISO
        - CarStatus: преобразуется в строковое значение
        
        Args:
            obj: Объект для сериализации
            
        Returns:
            Сериализованное представление объекта
            
        Raises:
            TypeError: Если тип объекта не поддерживается
        """
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, CarStatus):
            return obj.value
        raise TypeError(f'Object of type {type(obj)} is not JSON serializable')
    
    # Поиск индекса.
    def _binary_search_index(
        self, index: List[Dict[str, Any]], key: str
    ) -> int:
        """
        Бинарный поиск в отсортированном индексе.
        
        Args:
            index: Список словарей с ключами 'key' и 'line_num'
            key: Искомый ключ
            
        Returns:
            Позиция в списке индекса или -1, если ключ не найден
        """
        low = 0
        high = len(index) - 1
        
        while low <= high:
            mid = (low + high) // 2
            if index[mid]['key'] == key:
                return mid
            elif index[mid]['key'] < key:
                low = mid + 1
            else:
                high = mid - 1
        return -1
    
    def _insert_sorted_index(
        self, index: List[Dict[str, Any]], key: str, line_num: int
    ) -> None:
        """
        Вставка новой записи в индекс с сохранением сортировки.
        
        Args:
            index: Список словарей с ключами 'key' и 'line_num'
            key: Ключ новой записи
            line_num: Номер строки в файле данных
        """
        new_item = {'key': key, 'line_num': line_num}
        index.append(new_item)
        index.sort(key=lambda x: x['key'])
    
    # Задание 1. Сохранение автомобилей и моделей.
    def add_model(self, model: Model) -> Model:
        """
        Добавление новой модели автомобиля.
        
        Args:
            model: Объект модели для добавления
            
        Returns:
            Добавленная модель
            
        Raises:
            ValueError: Если модель с таким ID уже существует
        """
        index = self._read_index(self.models_index_path)
        
        if self._binary_search_index(index, model.index()) != -1:
            raise ValueError(f'Model with id {model.id} already exists')
        
        line_num = self._append_line(self.models_path, model.dict())
        
        self._insert_sorted_index(index, model.index(), line_num)
        self._write_index(self.models_index_path, index)
        
        return model

    # Задание 1. Сохранение автомобилей и моделей.
    def add_car(self, car: Car) -> Car:
        """
        Добавление нового автомобиля.
        
        Args:
            car: Объект автомобиля для добавления
            
        Returns:
            Добавленный автомобиль
            
        Raises:
            ValueError: Если автомобиль с таким VIN уже существует
        """
        index = self._read_index(self.cars_index_path)
        
        if self._binary_search_index(index, car.index()) != -1:
            raise ValueError(f'Car with VIN {car.vin} already exists')
        
        line_num = self._append_line(self.cars_path, car.dict())
        
        self._insert_sorted_index(index, car.index(), line_num)
        self._write_index(self.cars_index_path, index)
        
        return car

    # Задание 2. Сохранение продаж.
    def sell_car(self, sale: Sale) -> Car:
        """
        Регистрация продажи автомобиля.
        
        Args:
            sale: Объект продажи
            
        Returns:
            Обновленный объект автомобиля
            
        Raises:
            ValueError: Если автомобиль не найден или уже продан
            ValueError: Если продажа с таким номером уже существует
        """
        car_index = self._read_index(self.cars_index_path)
        car_pos = self._binary_search_index(car_index, sale.car_vin)
        if car_pos == -1:
            raise ValueError(f'Car with VIN {sale.car_vin} not found')
        
        line_num = car_index[car_pos]['line_num']
        car_data = self._read_line(self.cars_path, line_num)
        if not car_data:
            raise ValueError(f'Car with VIN {sale.car_vin} '
                             f'not found in data file')
        
        car = Car(**car_data)
        
        if car.status == CarStatus.sold:
            raise ValueError(f'Car with VIN {sale.car_vin} is already sold')
        
        car.status = CarStatus.sold
        self._write_line(self.cars_path, line_num, car.dict())
        
        sales_index = self._read_index(self.sales_index_path)
        
        if self._binary_search_index(sales_index, sale.sales_number) != -1:
            raise ValueError(f'Sale with number {sale.sales_number}'
                             f' already exists')
        
        line_num = self._append_line(self.sales_path, sale.dict())
        self._insert_sorted_index(sales_index, sale.sales_number, line_num)
        self._write_index(self.sales_index_path, sales_index)
        
        return car

    # Задание 3. Доступные к продаже.
    def get_cars(self, status: CarStatus) -> List[Car]:
        """
        Получение списка автомобилей по статусу.
        
        Автомобили возвращаются в порядке их добавления в систему.
        
        Args:
            status: Статус автомобиля для фильтрации
            
        Returns:
            Список автомобилей с указанным статусом
        """
        cars: List[Car] = []
    
        if (not os.path.exists(self.cars_path)
                or os.path.getsize(self.cars_path) == 0):
            return cars
    
        # Читаем весь файл построчно, сохраняя порядок добавления
        with open(self.cars_path, 'r') as f:
            pos = 0
            while True:
                line = f.read(LINE_LENGTH)
                if not line:
                    break
            
                data = json.loads(line.strip())
                if not data:  # Пропускаем удаленные записи
                    pos += LINE_LENGTH
                    continue
            
                car = Car(**data)
                if car.status == status:
                    cars.append(car)
            
                pos += LINE_LENGTH
    
        return cars

    # Задание 4. Детальная информация.
    def get_car_info(self, vin: str) -> Optional[CarFullInfo]:
        """
        Получение полной информации об автомобиле по VIN.
        
        Args:
            vin: VIN-код автомобиля
            
        Returns:
            Полная информация об автомобиле или None, если автомобиль не найден
        """
        car_index = self._read_index(self.cars_index_path)
        car_pos = self._binary_search_index(car_index, vin)
        if car_pos == -1:
            return None
        
        line_num = car_index[car_pos]['line_num']
        car_data = self._read_line(self.cars_path, line_num)
        if not car_data:
            return None
        
        car = Car(**car_data)
        
        model_index = self._read_index(self.models_index_path)
        model_pos = self._binary_search_index(model_index, str(car.model))
        if model_pos == -1:
            return None
        
        model_line_num = model_index[model_pos]['line_num']
        model_data = self._read_line(self.models_path, model_line_num)
        if not model_data:
            return None
        
        model = Model(**model_data)
        
        sales_date = None
        sales_cost = None
        
        if car.status == CarStatus.sold:
            if (os.path.exists(self.sales_path)
                    and os.path.getsize(self.sales_path) > 0):
                with open(self.sales_path, 'r') as f:
                    pos = 0
                    while True:
                        line = f.read(LINE_LENGTH)
                        if not line:
                            break
                        
                        data = json.loads(line.strip())
                        if not data:
                            pos += LINE_LENGTH
                            continue
                        
                        sale = Sale(**data)
                        if sale.car_vin == vin:
                            sales_date = sale.sales_date
                            sales_cost = sale.cost
                            break
                        
                        pos += LINE_LENGTH
        
        return CarFullInfo(
            vin=car.vin,
            car_model_name=model.name,
            car_model_brand=model.brand,
            price=car.price,
            date_start=car.date_start,
            status=car.status,
            sales_date=sales_date,
            sales_cost=sales_cost
        )

    # Задание 5. Обновление ключевого поля.
    def update_vin(self, vin: str, new_vin: str) -> Car:
        """
        Обновление VIN-кода автомобиля.
        
        Args:
            vin: Текущий VIN-код
            new_vin: Новый VIN-код
            
        Returns:
            Обновленный объект автомобиля
            
        Raises:
            ValueError: Если автомобиль не найден
            ValueError: Если автомобиль с новым VIN уже существует
        """
        car_index = self._read_index(self.cars_index_path)
        
        old_pos = self._binary_search_index(car_index, vin)
        if old_pos == -1:
            raise ValueError(f'Car with VIN {vin} not found')
        
        if self._binary_search_index(car_index, new_vin) != -1:
            raise ValueError(f'Car with VIN {new_vin} already exists')
        
        line_num = car_index[old_pos]['line_num']
        car_data = self._read_line(self.cars_path, line_num)
        if not car_data:
            raise ValueError(f'Car with VIN {vin} not found in data file')
        
        car = Car(**car_data)
        car.vin = new_vin
        self._write_line(self.cars_path, line_num, car.dict())
        
        car_index.pop(old_pos)
        self._insert_sorted_index(car_index, new_vin, line_num)
        self._write_index(self.cars_index_path, car_index)
        
        return car

    # Задание 6. Удаление продажи.
    def revert_sale(self, sales_number: str) -> Car:
        """
        Отмена продажи автомобиля.
        
        Args:
            sales_number: Номер продажи
            
        Returns:
            Обновленный объект автомобиля
            
        Raises:
            ValueError: Если продажа не найдена
            ValueError: Если автомобиль не найден
        """
        sales_index = self._read_index(self.sales_index_path)
        sale_pos = self._binary_search_index(sales_index, sales_number)
        if sale_pos == -1:
            raise ValueError(f'Sale with number {sales_number} not found')
        
        line_num = sales_index[sale_pos]['line_num']
        sale_data = self._read_line(self.sales_path, line_num)
        if not sale_data:
            raise ValueError(f'Sale with number {sales_number}'
                             f' not found in data file')
        
        sale = Sale(**sale_data)
        
        car_index = self._read_index(self.cars_index_path)
        car_pos = self._binary_search_index(car_index, sale.car_vin)
        if car_pos == -1:
            raise ValueError(f'Car with VIN {sale.car_vin} not found')
        
        car_line_num = car_index[car_pos]['line_num']
        car_data = self._read_line(self.cars_path, car_line_num)
        if not car_data:
            raise ValueError(f'Car with VIN {sale.car_vin}'
                             f' not found in data file')
        
        car = Car(**car_data)
        car.status = CarStatus.available
        self._write_line(self.cars_path, car_line_num, car.dict())
        
        self._write_line(self.sales_path, line_num, {})
        sales_index.pop(sale_pos)
        self._write_index(self.sales_index_path, sales_index)
        
        return car

    # Задание 7. Самые продаваемые модели.
    def top_models_by_sales(self) -> List[ModelSaleStats]:
        """
        Получение топ-3 самых продаваемых моделей.
        
        Модели сортируются по количеству продаж (по убыванию),
        а при равном количестве - по названию модели (по возрастанию).
        
        Returns:
            Список из 3 объектов ModelSaleStats
        """
        model_sales: Dict[int, Dict[str, Any]] = {}

        # Чтение всех моделей, для получения информации о них.
        models_info = {}
        model_index = self._read_index(self.models_index_path)
        for item in model_index:
            model_data = self._read_line(self.models_path, item['line_num'])
            if model_data:
                model = Model(**model_data)
                models_info[model.id] = model
    
        # Подсчет количества продаж по модели.
        if (os.path.exists(self.sales_path)
                and os.path.getsize(self.sales_path) > 0):
            with open(self.sales_path, 'r') as f:
                pos = 0
                while True:
                    line = f.read(LINE_LENGTH)
                    if not line:
                        break
                
                    data = json.loads(line.strip())
                    if not data:
                        pos += LINE_LENGTH
                        continue
                
                    sale = Sale(**data)
                
                    car_index = self._read_index(self.cars_index_path)
                    car_pos = self._binary_search_index(
                        car_index, sale.car_vin
                    )
                    if car_pos == -1:
                        pos += LINE_LENGTH
                        continue
                
                    car_line_num = car_index[car_pos]['line_num']
                    car_data = self._read_line(self.cars_path, car_line_num)
                    if not car_data:
                        pos += LINE_LENGTH
                        continue
                
                    car = Car(**car_data)
                    model_id = car.model
                
                    if model_id not in model_sales:
                        model_sales[model_id] = {
                            'count': 0,
                            'model_id': model_id
                        }
                
                    model_sales[model_id]['count'] += 1
                
                    pos += LINE_LENGTH
    
        top_models = []
        for model_id, sales_data in model_sales.items():
            if model_id in models_info:
                model = models_info[model_id]
                top_models.append(ModelSaleStats(
                    car_model_name=model.name,
                    brand=model.brand,
                    sales_number=sales_data['count']
                ))
    
        top_models.sort(key=lambda x: (-x.sales_number, x.car_model_name))
    
        return top_models[:3]
