import os
import asyncio
import datetime
from math import ceil
from multiprocessing import Process, Manager

from src.AsyncAPI import AIOInfoGrabber
from src.ConfigWorks import get_proxies, get_tokens


def list_to_chunks(lst: list, n: int):
    """Разделяет список на n частей"""
    size = ceil(len(lst) / n)
    return list(map(lambda x: lst[x * size:x * size + size], list(range(n))))


def get_current_time() -> str:
    """Возвращает строку с текущим временем, нужно для логирования"""
    cur_time = datetime.datetime.now()
    return cur_time.strftime('%H:%M:%S')


class InfoProcess:
    """Класс для ПРОЦЕССА сбора информации"""
    def __init__(self, process_id: int,
                 max_process_id: int,
                 user_ids: list,
                 tokens: dict,
                 data_folder: str,
                 proxy: str | None,
                 proxy_auth: list[str, str] | None,
                 barrier,
                 need_repeat: int):
        """
        :param process_id: Номер процесса
        :param max_process_id: Сколько всего процессов
        :param user_ids: Все id пользователей
        :param tokens: Все токены API
        :param data_folder: Папка с данными этого списка пользователей
        :param proxy: Адрес прокси, если нет, то None
        :param proxy_auth: Данные для аутентификации прокси, если нет, то None
        :param barrier: Блокиратор для синхронизации процессов
        :param need_repeat: Переменная нужности повтора в общей памяти процессов
        """
        self.process_id = process_id
        self.max_id = max_process_id
        self.user_id_list = user_ids
        self.tokens_dict = tokens
        self.data_folder = data_folder
        self.proxy = proxy
        self.proxy_auth = proxy_auth
        self.barrier = barrier
        self.need_repeat = need_repeat

        while self.need_repeat.value == 1:
            print(f'[{get_current_time()}][INFO P_{self.process_id}] Ожидание запуска других процессов')

            # Ожидание пока все процессы достигнут этой точки кода
            self.barrier.wait()
            self.informing(f'[{get_current_time()}][INFO] Начинаем процесс сбора информации')

            if self.need_repeat.value == 1:
                self.need_repeat.value = 0

            # Запускаем нужные методы
            # (для больших наборов советую только users, т.к. он не ограничен по кол-ву вызовов)
            self.grab_info_method('users')
            # self.grab_info_method('groups')   # Ограничение  ~800 id после чего блокируется метод
            # self.grab_info_method('walls')    # Ограничение ~2000 id после чего блокируется метод

            # Ожидание пока все процессы завершат сбор информации
            self.barrier.wait()

            if self.need_repeat.value == 1:
                self.informing(f'[{get_current_time()}][INFO] Требуется повторение процесса сбора информации\n\n')

    def grab_info_method(self, method) -> None:
        """
        Конкурентный сбор информации для каждого процесса через его прокси, в соответствии с выбранным методом
        :param method: метод сбора, может быть ТОЛЬКО 'users', 'groups', 'walls'
        """
        # Ждем пока все процессы достигнут этой точки
        self.barrier.wait()

        # Выделяем токены API, которые могут взаимодействовать с выбранным методом.
        # То есть те, которые еще не достигли своего лимита в выбранном методе
        available_tokens = [key for key in self.tokens_dict.keys() if not self.tokens_dict[key][method]]
        if len(available_tokens) == 0:
            self.informing(f'[{get_current_time()}][ERROR] Все токены ограничены в методе {method}!'
                           f'\n\tПроцесс сбора продолжится, но этот метод не будет собран до конца')

        # Если доступных токенов осталось меньше, чем процессов, то оставшиеся процессы бездействуют
        elif self.process_id < len(available_tokens):
            # Делим список пользователей между процессами или по количеству доступных токенов, смотря чего меньше
            users_chunks = list_to_chunks(self.user_id_list, min(self.max_id, len(available_tokens)))
            current_process_token = available_tokens[self.process_id]    # Записываем токен для использования методом
            current_process_users = users_chunks[self.process_id]        # Записываем список пользователей для метода

            # Считаем примерное время ожидания завершения метода (при первоначальном запуске)
            # Кол-во пользователей, которое может обработать метод за минуту
            count_users_method = {'users': 6525, 'groups': 6575, 'walls': 2380}
            time_to_wait = int((len(current_process_users) / count_users_method[method]) * 2) + 1
            self.informing(f'[{get_current_time()}][INFO] Собираем информацию по '
                           f'методу {method}, время сбора с нуля: ~{time_to_wait} мин.')

            # Запускаем конкурентный сбор данных по пользователям с использованием переменных процесса
            limits, need_repeat_from_method = asyncio.run(AIOInfoGrabber(
                current_process_users, self.data_folder, current_process_token, self.proxy, self.proxy_auth,
                True if self.process_id == 0 else False).start(method))

            # Если после выполнения метода нужно повторно собрать информацию
            if need_repeat_from_method and self.need_repeat.value == 0:
                self.need_repeat.value = 1

            # Если лимиты изменились, то прописываем их
            for limit_name in limits.keys():
                if limits[limit_name]:  # Если лимит сменился на True, то применяем его
                    self.tokens_dict[current_process_token][limit_name] = limits[limit_name]

    def informing(self, message):
        if self.process_id == 0:
            print(message)


def take_data(all_ids: list, data_folder: str) -> None:
    """
    Создание и запуск Процессов для сбора информации пользователей
    :param all_ids: Список со всеми id, у которых нужно собрать информацию.
    :param data_folder: Папка, в которую помещается БД с данными анализа
    """
    manager = Manager()         # Менеджер управления данными для процессов

    proxys = get_proxies()       # Забираем все прокси
    token_keys = get_tokens()   # Забираем все токены

    if len(token_keys) == 0:
        raise ValueError('Необходимо указать как минимум один токен API!')

    # И преобразуем их в общий словарь с ограничениями по методам
    tokens = {}
    for key in token_keys:
        tokens[key] = manager.dict({'users': False, 'groups': False, 'walls': False})

    # Узнаем сколько потоков мы можем задействовать (если не можем узнать, то 8)
    cores_num = os.cpu_count()
    cores_num = 8 if cores_num is None else cores_num

    process_number = min(len(proxys), len(token_keys), cores_num)  # Кол-во процессов
    barrier = manager.Barrier(process_number)           # Блокиратор для синхронизации процессов
    need_repeat_val = manager.Value('i', 1)             # Переменная для повторения

    # Создание процессов сбора информации
    process = [Process(target=InfoProcess, args=(
        proc_id, process_number, all_ids, tokens, data_folder,
        proxys[proc_id][0], proxys[proc_id][1], barrier, need_repeat_val
    )) for proc_id in range(process_number)]

    # Запуск и ожидание завершения
    for proc in process:
        proc.start()
    for proc in process:
        proc.join()
