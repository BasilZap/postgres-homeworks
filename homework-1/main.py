"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv  # Импорт модуля для работы с CSV
import psycopg2  # Импорт пакета для работы с CSV

# Объявление констант путей к файлам
FILE_NAME_EMPLOYEES = './north_data/employees_data.csv'
FILE_NAME_CUSTOMERS = './north_data/customers_data.csv'
FILE_NAME_ORDERS = './north_data/orders_data.csv'


# Класс для подключения и заполнения DB
class DBConnection:

    # Конструктор класса DBConnection данными - название хоста и имя базы (пользователь/пароль скрыты)
    def __init__(self, db_host, db_name):
        self.db_host = db_host
        self.db_name = db_name
        self.__db_user = 'postgres'
        self.__db_pass = '123456'

    def fill_customers_db(self, customers_data: list) -> None:
        """
        Метод подключения к БД и записи данных в таблицу customers
        на вход подается список кортежей для записи
        """
        with psycopg2.connect(host=self.db_host, database=self.db_name, user=self.__db_user,
                              password=self.__db_pass) as conn:
            # Открываем курсор для работы с БД, с таблицей customers
            with conn.cursor() as cur:
                for custom_record in customers_data:
                    cur.execute("INSERT INTO customers VALUES (%s, %s, %s)", custom_record)
                conn.commit()  # Запись данных в таблицу customers

        conn.close()  # Закрываем соединение

    def fill_employees_db(self, employees_data: list) -> None:
        """
        Метод подключения к БД и записи данных в таблицу employees
        на вход подается список кортежей для записи
        """
        with psycopg2.connect(host=self.db_host, database=self.db_name, user=self.__db_user,
                              password=self.__db_pass) as conn:
            # Открываем курсор для работы с БД, с таблицей customers
            with conn.cursor() as emp_cur:
                for emp_record in employees_data:
                    emp_cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", emp_record)
                conn.commit()  # Запись данных в таблицу employees

        conn.close()  # Закрываем соединение с БД

    def fill_orders_db(self, order_data: list) -> None:
        """
        Метод подключения к БД и записи данных в таблицу employees
        на вход подается список кортежей для записи
        """
        with psycopg2.connect(host=self.db_host, database=self.db_name, user=self.__db_user,
                              password=self.__db_pass) as conn:
            # Открываем курсор для работы с БД, с таблицей customers
            with conn.cursor() as order_cur:
                for order_record in order_data:
                    order_cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", order_record)
                conn.commit()  # Запись данных в таблицу orders

        conn.close()  # Закрываем соединение с БД


def get_customers_data_from_csv(file_name: str) -> list:
    """
    Функция чтения данных из CSV-файла, форматирует, полученные
    в виде словаря данные, и возвращает список кортежей данных
    для таблицы customers
    """
    customers_list = []  # Список, в который будут складываться кортежи

    # Открытие CSV файла для чтения
    with open(file_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        # Читаем файл построчно, формируем кортежи и складываем их в список customers_list
        for dict_str in reader:
            tuple_of_data = (dict_str["customer_id"], dict_str["company_name"], dict_str["contact_name"])
            customers_list.append(tuple_of_data)
    csvfile.close()  # Закрываем CSV-файл
    return customers_list


def get_employee_data_from_csv(file_name: str) -> list:
    """
    Функция чтения данных из CSV-файла, форматирует, полученные
    в виде словаря данные, и возвращает список кортежей данных
    для таблицы employees
    """
    employee_list = []  # Список, в который будут складываться кортежи

    # Открытие CSV файла для чтения
    with open(file_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        # Читаем файл построчно, формируем кортежи и складываем их в список employee_list
        for dict_str in reader:
            tuple_of_data = (int(dict_str["employee_id"]), dict_str["first_name"], dict_str["last_name"],
                             dict_str["title"], dict_str["birth_date"], dict_str["notes"])
            employee_list.append(tuple_of_data)
    csvfile.close()  # Закрываем CSV-файл
    return employee_list


def get_orders_data_from_csv(file_name: str) -> list:
    """
    Функция чтения данных из CSV-файла, форматирует, полученные
    в виде словаря данные, и возвращает список кортежей данных
    для таблицы orders
    """
    order_list = []  # Список, в который будут складываться кортежи

    # Открытие CSV файла для чтения
    with open(file_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        # Читаем файл построчно, формируем кортежи и складываем их в список order_list
        for dict_str in reader:
            tuple_of_data = (int(dict_str["order_id"]), dict_str["customer_id"], int(dict_str["employee_id"]),
                             dict_str["order_date"], dict_str["ship_city"])
            order_list.append(tuple_of_data)
    csvfile.close()  # Закрываем CSV-файл
    return order_list


if __name__ == "__main__":

    # Получение данных из CSV-файлов для всех таблиц
    customer_data = get_customers_data_from_csv(FILE_NAME_CUSTOMERS)
    employee_data = get_employee_data_from_csv(FILE_NAME_EMPLOYEES)
    orders_data = get_orders_data_from_csv(FILE_NAME_ORDERS)

    # Создание экземпляра класса DBConnection с параметрами подключения
    north_db = DBConnection('localhost', 'north')

    # Заполнение таблиц БД
    north_db.fill_customers_db(customer_data)
    north_db.fill_employees_db(employee_data)
    north_db.fill_orders_db(orders_data)
