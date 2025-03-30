import asyncio
import configparser
from math import ceil
from AsyncAPI import AIOInfoGrabber
from multiprocessing import Process, Manager


def list_to_chunks(lst: list, n: int):
    """Разделяет список на n частей"""
    size = ceil(len(lst) / n)
    return list(map(lambda x: lst[x * size:x * size + size], list(range(n))))


def get_proxys():
    """
    Чтение прокси из конфигурационного файла
    :return: [[proxy_addr | None, (log, pass) | None], ]
    """
    config = configparser.ConfigParser()
    config.read("settings.ini", encoding='utf-8')               # Читаем конфиг
    proxys_strs = config['PROXY']['proxy'].strip().split('\n')  # Читаем строку прокси из конфига
    separator = config['PROXY']['separator']                    # И разделитель для логина и пароля

    if proxys_strs == ['']:     # Если список прокси пуст
        return [[None, None]]   # То возвращается адрес оригинальной машины

    proxy_full = []
    for proxys_str in proxys_strs:
        pp = proxys_str.strip().split(separator)
        if len(pp) > 1:         # Если есть разделитель в строке прокси
            if len(pp) < 3:     # Смотрим есть ли логин И пароль
                raise Exception(f'Нет логина или пароля у прокси [{pp[0]}]!')
            elif len(pp) > 3:   # Тут было бы что-то странное
                raise Exception(f'Слишком много разделителей у прокси [{pp[0]}]!')
            log_pass = (pp[1], pp[2])       # Если есть, то вносим их
        else:
            log_pass = None                 # Если нет, то вносим None
        proxy_full.append([pp[0], log_pass])
    if int(config['PROXY']['use_original_address']) == 1:   # Если нужно использовать адрес оригинальной машины
        proxy_full.append([None, None])                     # То добавляем его в список
    return proxy_full


def get_tokens():
    """
    Чтение токенов API
    :return: [API_Token, ]
    """
    config = configparser.ConfigParser()
    config.read("settings.ini", encoding='utf-8')  # Читаем конфиг
    tokens = config['VK']['access_token'].strip().split('\n')

    if len(tokens) == 0:
        raise Exception('Необходимо указать как минимум один токен API в конфигурационном файле (settings.ini)!')
    return tokens


class InfoProcess:
    """Класс для процесса сбора информации"""
    def __init__(self, process_id: int,
                 max_process_id: int,
                 user_ids: list,
                 tokens: dict,
                 data_folder: str,
                 proxy: str | None,
                 proxy_auth: list[str, str] | None,
                 barrier,
                 need_repeat):
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
            print(f'[INFO P_{self.process_id}] Ожидание начала сбора информации других процессов')
            self.barrier.wait()     # Ожидание всех остальных процессов
            self.informing(f'[INFO] Начинаем процесс сбора информации')

            if self.need_repeat.value == 1:
                self.need_repeat.value = 0

            self.grab_info_method('users')
            # self.grab_info_foaf()     # FOAF БОЛЬШЕ НЕ РАБОТАЕТ
            self.grab_info_method('groups')
            self.grab_info_method('walls')
            self.barrier.wait()     # Ожидание всех остальных процессов

            if self.need_repeat.value == 1:
                self.informing(f'[INFO] Требуется повторение процесса сбора информации\n\n')

    def grab_info_method(self, method):
        """
        Выбор метода сбора информации, после которого запускается конкурентный сбор данных через прокси процесса
        :param method: метод сбора, может быть ТОЛЬКО 'users', 'groups', 'walls'
        """
        self.barrier.wait()     # Ожидание всех остальных процессов
        # Выделяем токены, которые могут взаимодействовать с выбранным методом
        available_tokens = [key for key in self.tokens_dict.keys() if not self.tokens_dict[key][method]]
        if len(available_tokens) == 0:
            self.informing(f'[ERROR] Все токены ограничены в методе {method}!'
                           f'\n\tПроцесс сбора продолжится, но этот метод не будет собран до конца')
        elif self.process_id < len(available_tokens):
            # Делим список пользователей между процессами
            users_chunks = list_to_chunks(self.user_id_list, min(self.max_id, len(available_tokens)))
            my_token = available_tokens[self.process_id]    # Записываем токен для использования методом
            my_users = users_chunks[self.process_id]        # Записываем список пользователей для использования методом

            # Время ожидания завершения метода
            count_users_method = {'users': 6525, 'groups': 6575, 'walls': 2380}
            time_to_wait = int((len(my_users) / count_users_method[method]) * 2) + 1
            self.informing(f'[INFO] Собираем информацию по методу {method}, время ожидания ~{time_to_wait} мин.')

            # Запускаем конкурентный сбор данных по пользователям с использованием переменных процесса
            limits, need_repeat_from_method = asyncio.run(AIOInfoGrabber(
                my_users, self.data_folder, my_token, self.proxy, self.proxy_auth).start(method))

            # Если после выполнения метода нужно повторной собрать информацию
            if need_repeat_from_method and self.need_repeat.value == 0:
                self.need_repeat.value = 1

            # Если лимиты изменились, то прописываем их
            for limit_name in limits.keys():
                if limits[limit_name]:  # Если лимит сменился на True, то применяем его
                    self.tokens_dict[my_token][limit_name] = limits[limit_name]

    def grab_info_foaf(self):
        # Делим список пользователей между процессами
        users_chunks = list_to_chunks(self.user_id_list, self.max_id)
        my_users = users_chunks[self.process_id]  # Записываем список пользователей для использования методом

        # Время ожидания завершения метода
        time_to_wait = int((len(my_users) / 27000) * 2) + 1
        self.informing(f'[INFO] Собираем информацию по методу foaf, время ожидания ~{time_to_wait} мин.')

        # Запускаем конкурентный сбор данных по пользователям с использованием переменных процесса
        _, need_repeat_from_method = asyncio.run(AIOInfoGrabber(
            my_users, self.data_folder, '', self.proxy, self.proxy_auth).start('foaf'))

        if need_repeat_from_method and self.need_repeat.value == 0:
            self.need_repeat.value = 1

    def informing(self, message):
        if self.process_id == 0:
            print(message)


def take_data(all_ids: list, data_folder: str):
    manager = Manager()         # Менеджер управления данными для процессов

    proxys = get_proxys()       # Забираем все прокси
    token_keys = get_tokens()   # Забираем все токены

    # И преобразуем их в общий словарь с ограничениями по методам
    tokens = {}
    for key in token_keys:
        tokens[key] = manager.dict({'users': False, 'groups': False, 'walls': False})

    process_number = min(len(proxys), len(token_keys))  # Кол-во процессов
    barrier = manager.Barrier(process_number)           # Блокиратор для синхронизации потоков
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
