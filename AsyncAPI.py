import asyncio
import copy
import sys

import aiohttp
import aiosqlite
import time
import datetime
import lxml
import json
import math
import warnings
import configparser
from copy import deepcopy
from aiohttp import ClientSession, ClientTimeout
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from statistics import fmean, median
from DBWorks import AioDBWorks

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

config = configparser.ConfigParser()  # Создаём объект парсера
config.read("settings.ini", encoding='utf-8')  # Читаем конфиг


class AIOInfoGrabber:
    def __init__(self, users: list,
                 data_folder: str,
                 access_token: str,
                 round_seconds: int = 120,
                 proxy: str = None,
                 proxy_auth: list[str, str] = None):
        """
        Класс, предназначенный для сбора информации о множестве пользователей за малое время
        :param users: Список пользователей, которых нужно проверить
        :param data_folder: Путь до папки, где будут храниться данные по текущему разбору (data/<название xls дока>)
        :param access_token: Токен от VK API
        :param round_seconds: Раз в сколько секунд информация сохраняется на диск, стандартное значение - 120 секунд
        :param proxy: Прокси, если есть
        :param proxy_auth: Логин и пароль для прокси, если есть
        """
        self.all_users_id = sorted(list(set([int(item) for item in users])))
        self.data_folder = data_folder
        self.sessions = {}
        self.proxy = proxy
        self.proxy_auth = proxy_auth
        self.db_worker: AioDBWorks | None = None

        # Сколько запросов к API сделать за backup_seconds секунд
        # Задержка ответа на запрос о пользователях - 5 секунд, группах - 4 секунды, постах - 15 секунд, foaf - 0.6
        # Оставляется 10% на погрешность
        # Потом делим на 0.4 секунды, которые нужны чтобы API не заблокировался,
        # для foaf - 0.004, так как он не связан с API и может отвечать чаще
        # Все значения для стандартного значения в 120 секунд
        self.user_info_rounds = int((round_seconds - 5) / 1.1 / 0.4)    # 261 пакет id, по 25, всего 6 525 id
        self.group_rounds = int((round_seconds - 4) / 1.1 / 0.4)        # 263 пакета id, по 25, всего 6 575 id
        self.wall_rounds = int((round_seconds - 15) / 1.1 / 0.4)        # 238 покетов id, по 10, всего 2 380 id
        self.foaf_rounds = int((round_seconds - 0.6) / 1.1 / 0.004)     # 27 136 id

        # Ссылки на методы для сбора информации
        self.users_info_url = 'https://api.vk.com/method/execute.users_info'
        self.users_groups_url = 'https://api.vk.com/method/execute.groups_info'
        self.users_wall_url = 'https://api.vk.com/method/execute.walls_info'
        self.foaf_url = 'https://vk.com/foaf.php'

        self.access_token = access_token
        self.version = 5.199

        # Достигнуты ли лимиты метода
        self.limit_reached = {'users': False,
                              'groups': False,
                              'walls': False}

        # Если не проверили все профили, то нужен повтор
        self.need_repeat = False

        # Запрос полей для API, а так же заполнители профиля у открытых
        fields_list = ["user_id", "about", "activities", "books", "career", "city", "country",
                       "has_photo", "has_mobile", "home_town", "schools", "status",
                       "games", "interests", "military", "movies", "music", "occupation", "personal",
                       "quotes", "relation", "universities", "screen_name", "verified", "counters"]
        self.fields_str = ','.join(fields_list)     # Все нужные поля юзеров в строку через запятую

        # Поля заполнителей профиля
        self.open_fillers_list = fields_list[1:-3]
        self.close_fillers_list = ['city', 'has_photo', 'has_mobile', 'status', 'occupation']

        # Поля счетчиков профиля
        self.open_counters_list = ['albums', 'audios', 'followers', 'friends', 'pages', 'photos', 'subscriptions',
                                   'videos', 'video_playlists', 'clips_followers', 'gifts']
        self.close_counters_list = ['friends', 'pages', 'subscriptions', 'posts']

        # Сразу при объявлении класса запускаем
        # asyncio.run(self.start())

    async def start(self, method: str):
        """
        ВЫПОЛНЯТЬ С ПОМОЩЬЮ asyncio.run(self.start(method))!
        Выполняет сбор информации по выбранному методу.
        :param method: 'users', 'foaf', 'groups' и 'walls', только они.
        :return: Есть ли лимиты и нужен ли повтор сбора информации
        """
        if method not in ['users', 'foaf', 'groups', 'walls']:
            raise Exception('Некорректно указанный метод для AIOInfoGrabber(*).start(method)')

        # Разные подключения
        # Если есть прокси, то
        proxy_auth = aiohttp.BasicAuth(self.proxy_auth[0], self.proxy_auth[1]) if self.proxy_auth is not None else None
        self.sessions['vk'] = ClientSession(timeout=ClientTimeout(total=30), proxy=self.proxy, proxy_auth=proxy_auth)
        self.sessions['db'] = aiosqlite.connect(fr'{self.data_folder}\data.db')

        async with self.sessions['vk'], self.sessions['db']:
            self.db_worker = AioDBWorks(self.sessions['db'])
            await self.db_worker.create_tables()    # Создаем БД и таблички

            # === ПОЛЬЗОВАТЕЛИ ===
            if method == 'users':
                # Смотрим какие пользователи уже проверены и непроверенных проверяем
                checked_users = await self.db_worker.get_data_in_list('SELECT DISTINCT user_id FROM users')
                unchecked_users = sorted(list(set(self.all_users_id) - set(checked_users)))
                print(f'Всего: {len(self.all_users_id)}, '
                      f'Проверенно: {len(checked_users)}, '
                      f'Осталось: {len(unchecked_users)}')
                while len(unchecked_users) != 0 and not self.limit_reached['users']:
                    await self.users_info_process(self.all_users_id)    # Сбор данных из сети
                    checked_users = await self.db_worker.get_data_in_list('SELECT DISTINCT user_id FROM users')
                    unchecked_users = sorted(list(set(self.all_users_id) - set(checked_users)))
                    print(f'Всего: {len(self.all_users_id)}, '
                          f'Проверенно: {len(checked_users)}, '
                          f'Осталось: {len(unchecked_users)}')
                if len(unchecked_users) != 0:
                    self.need_repeat = True

            # === FOAF ===
            elif method == 'foaf':
                ids_to_foaf = await self.db_worker.get_data_in_list(
                    'SELECT DISTINCT user_id FROM users WHERE deactivated = 0 AND foaf_checked = 0')
                print(f'Предстоит проверить с помощью foaf: {len(ids_to_foaf)}')
                while len(ids_to_foaf) != 0:
                    await self.foaf_process(ids_to_foaf)
                    ids_to_foaf = await self.db_worker.get_data_in_list(
                        'SELECT DISTINCT user_id FROM users WHERE deactivated = 0 AND foaf_checked = 0')
                    print(f'Предстоит проверить с помощью foaf: {len(ids_to_foaf)}')
                if len(ids_to_foaf) != 0:
                    self.need_repeat = True

            # === ГРУППЫ ===
            elif method == 'groups':
                ids_to_groups = await self.db_worker.get_data_in_list(
                    'SELECT DISTINCT user_id FROM users WHERE deactivated = 0 AND is_close = 0 AND group_checked = 0')
                print(f'Предстоит проверить группы у {len(ids_to_groups)} пользователей')
                while len(ids_to_groups) != 0 and not self.limit_reached['groups']:
                    await self.groups_process(ids_to_groups)
                    ids_to_groups = await self.db_worker.get_data_in_list(
                        'SELECT DISTINCT user_id FROM users WHERE deactivated = 0 AND is_close = 0 AND group_checked = 0')
                    print(f'Предстоит проверить группы у {len(ids_to_groups)} пользователей')
                if len(ids_to_groups) != 0:
                    self.need_repeat = True

            # === ПОСТЫ ===
            elif method == 'walls':
                ids_to_posts = await self.db_worker.get_data_in_list(
                    'SELECT DISTINCT user_id FROM users WHERE deactivated = 0 AND is_close = 0 AND wall_checked = 0')
                print(f'Предстоит проверить посты у {len(ids_to_posts)} пользователей')
                while len(ids_to_posts) != 0 and not self.limit_reached['walls']:
                    await self.posts_process(ids_to_posts)
                    ids_to_posts = await self.db_worker.get_data_in_list(
                        'SELECT DISTINCT user_id FROM users WHERE deactivated = 0 AND is_close = 0 AND wall_checked = 0')
                    print(f'Предстоит проверить посты у {len(ids_to_posts)} пользователей')
                if len(ids_to_posts) != 0:
                    self.need_repeat = True

            return self.limit_reached, self.need_repeat

    # ========== ПРОЦЕССЫ ДЛЯ ОБРАБОТКИ API ==========
    async def users_info_process(self, users_id):
        """Разбитие на раунды сохранения и выполнение сбора информации по пользователям"""
        start_event = asyncio.Event()
        start_event.set()

        # Собираем раунды со списками id пользователей
        users_id_str = [str(item) for item in users_id]
        id_rounds_to_info = [','.join(round_ids) for round_ids in self.list_split(users_id_str, 25)]
        info_rounds = self.list_split(id_rounds_to_info, self.user_info_rounds)  # И разбиваем по раундам сохранения

        for round_ids in info_rounds:  # Примерно по две минуты на раунд
            events = [asyncio.Event() for _ in range(len(round_ids))]  # Создаем события для каждого потока
            # Создаем задачи для каждого потока
            tasks = [asyncio.create_task(self.users_info_request(round_ids[0], start_event, events[0]))]
            for i in range(1, len(round_ids)):
                tasks.append(asyncio.create_task(self.users_info_request(round_ids[i], events[i - 1], events[i])))

            # Запускаем задачи
            results = await asyncio.gather(*tasks)

            # Сохраняем результаты
            save_tasks = []
            for item in results:
                if 'execute_errors' in item:
                    self.need_repeat = True
                    limit_reached_here = False
                    # Если есть ошибка 29, то запрещаем ключу дальнейшее взаимодействие с методом
                    for error in item['execute_errors']:
                        if error['error_code'] == 29:
                            limit_reached_here = True
                            self.limit_reached['groups'] = True
                    if limit_reached_here:  # Если это была не ошибка 29, то продолжаем
                        continue
                if 'execute_errors' not in item and 'response' in item:
                    save_tasks.append(asyncio.create_task(self.write_users_info(item['response'])))
            await asyncio.gather(*save_tasks)

    async def foaf_process(self, users_id):
        """Разбитие на раунды сохранения и выполнение сбора информации по датам"""
        start_event = asyncio.Event()
        start_event.set()

        # Собираем раунды со списками id пользователей
        info_rounds = self.list_split(users_id, self.foaf_rounds)  # И разбиваем по раундам сохранения

        for round_ids in info_rounds:  # Примерно по две минуты на раунд
            events = [asyncio.Event() for _ in range(len(round_ids))]  # Создаем события для каждого потока
            # Создаем задачи для каждого потока
            tasks = [asyncio.create_task(self.foaf_request(round_ids[0], start_event, events[0]))]
            for i in range(1, len(round_ids)):
                tasks.append(asyncio.create_task(self.foaf_request(round_ids[i], events[i - 1], events[i])))

            # Запускаем задачи
            results = await asyncio.gather(*tasks)

            # Сохраняем результаты
            save_tasks = []
            for item in results:
                if item is not None:
                    save_tasks.append(asyncio.create_task(self.write_foaf(item)))
            await asyncio.gather(*save_tasks)
            await self.sessions['db'].commit()

    async def groups_process(self, users_id):
        """Разбитие на раунды сохранения и выполнение сбора информации по группам пользователей"""
        start_event = asyncio.Event()
        start_event.set()

        # Собираем раунды со списками id пользователей
        users_id_str = [str(item) for item in users_id]
        id_rounds_to_info = [','.join(round_ids) for round_ids in self.list_split(users_id_str, 25)]
        rounds = self.list_split(id_rounds_to_info, self.group_rounds)  # И разбиваем по раундам сохранения

        for round_ids in rounds:  # Примерно по две минуты на раунд
            events = [asyncio.Event() for _ in range(len(round_ids))]  # Создаем события для каждого потока
            # Создаем задачи для каждого потока
            tasks = [asyncio.create_task(self.groups_request(round_ids[0], start_event, events[0]))]
            for i in range(1, len(round_ids)):
                tasks.append(asyncio.create_task(self.groups_request(round_ids[i], events[i - 1], events[i])))

            # Запускаем задачи
            results = await asyncio.gather(*tasks)

            # Сохраняем результаты
            save_tasks = []
            for item in results:
                if 'execute_errors' in item:
                    self.need_repeat = True
                    limit_reached_here = False
                    # Если есть ошибка 29, то запрещаем ключу дальнейшее взаимодействие с методом
                    for error in item['execute_errors']:
                        if error['error_code'] == 29:
                            limit_reached_here = True
                            self.limit_reached['groups'] = True
                    if limit_reached_here:  # Если это была не ошибка 29, то продолжаем
                        continue
                if 'response' in item:
                    save_tasks.append(asyncio.create_task(self.write_groups(item['response'])))
            await asyncio.gather(*save_tasks)
            await self.sessions['db'].commit()

    async def posts_process(self, users_id):
        """Разбитие на раунды сохранения и выполнение сбора информации по постам пользователей"""
        start_event = asyncio.Event()
        start_event.set()

        # Собираем раунды со списками id пользователей
        users_id_str = [str(item) for item in users_id]
        id_rounds_to_info = [','.join(round_ids) for round_ids in self.list_split(users_id_str, 10)]
        rounds = self.list_split(id_rounds_to_info, self.wall_rounds)  # И разбиваем по раундам сохранения\

        for round_ids in rounds:  # Примерно по две минуты на раунд
            events = [asyncio.Event() for _ in range(len(round_ids))]  # Создаем события для каждого потока
            # Создаем задачи для каждого потока
            tasks = [asyncio.create_task(self.walls_request(round_ids[0], start_event, events[0]))]
            for i in range(1, len(round_ids)):
                tasks.append(asyncio.create_task(self.walls_request(round_ids[i], events[i - 1], events[i])))

            # Запускаем задачи
            results = await asyncio.gather(*tasks)

            # Сохраняем результаты
            save_tasks = []
            for item in results:
                if 'execute_errors' in item:
                    self.need_repeat = True
                    limit_reached_here = False
                    # Если есть ошибка 29, то запрещаем ключу дальнейшее взаимодействие с методом
                    for error in item['execute_errors']:
                        if error['error_code'] == 29:
                            limit_reached_here = True
                            self.limit_reached['groups'] = True
                    if limit_reached_here:  # Если это была не ошибка 29, то продолжаем
                        continue
                if 'response' in item:
                    save_tasks.append(asyncio.create_task(self.write_posts(item['response'])))
            await asyncio.gather(*save_tasks)
            await self.sessions['db'].commit()

    @staticmethod
    def list_split(data_list: list, items_in_round: int):
        """Разделение списка на подсписки с items_in_round кол-вом элементов в каждом"""
        rounds = []
        for i in range(math.ceil(len(data_list) / items_in_round)):
            rounds.append(data_list[i * items_in_round: i * items_in_round + items_in_round])
        return rounds

    # ========== ЗАПРОСЫ К API ==========
    async def users_info_request(self, users: str, wait_event: asyncio.Event, my_event: asyncio.Event):
        """
        Запрос данных о пользователях через специальный метод API приложения
        :param users: Строка с id пользователей через запятую, НЕ БОЛЬШЕ 25!
        :param wait_event: Событие, которого ждет процедура, чтобы отправить запрос.
        :param my_event: Событие, в котором процедура сообщает, что запрос отправлен.
        :return: Словарь с ответами от API
        """
        await wait_event.wait()
        await asyncio.sleep(0.4)
        my_event.set()
        params = {'users_id': users, 'fields': self.fields_str, 'access_token': self.access_token, 'v': self.version}
        async with self.sessions['vk'].post(url=self.users_info_url, params=params) as response:
            translate_json = await response.json()
            return translate_json

    async def foaf_request(self, user: int, wait_event: asyncio.Event, my_event: asyncio.Event):
        """
        Запрос данных о времени создания профиля и времени последнего захода
        :param user: id пользователя, которого нужно проверить.
        :param wait_event: Событие, которого ждет процедура, чтобы отправить запрос.
        :param my_event: Событие, в котором процедура сообщает, что запрос отправлен.
        :return: Словарь с ответом от FOAF
        """
        await wait_event.wait()
        await asyncio.sleep(0.0)    # Так нужно
        my_event.set()

        async with self.sessions['vk'].get(url=self.foaf_url, params={'id': user}) as response:
            src = await response.text()
            soup = BeautifulSoup(src, "lxml")
            today = datetime.datetime.now()

            # Достаем дату создания профиля (её нет только у удаленных профилей)
            try:
                create_date = soup.find("ya:created").get("dc:date")
                create_date = datetime.datetime.strptime(create_date.split("T")[0], "%Y-%m-%d")
                life_time = (today - create_date).days
            except:
                # На случай если профиль удалили, пока собирали информацию
                await self.db_worker.remove_from_all_tables(int(user))
                return None

            # Достаем время последнего захода
            try:
                last_logged = soup.find("ya:lastloggedin").get("dc:date")  # Не всегда есть
                last_logged = datetime.datetime.strptime(last_logged.split("T")[0], "%Y-%m-%d")
                last_log_time = (today - last_logged).days
            except:
                last_log_time = 60

            return {'id': user, 'life_time': life_time, 'last_log_time': last_log_time}

    async def groups_request(self, users: str, wait_event: asyncio.Event, my_event: asyncio.Event):
        """
        Запрос данных о группах пользователей через специальный метод API приложения
        :param users: Строка с id пользователей через запятую, НЕ БОЛЬШЕ 25!
        :param wait_event: Событие, которого ждет процедура, чтобы отправить запрос.
        :param my_event: Событие, в котором процедура сообщает, что запрос отправлен.
        :return: Словарь с ответами от API
        """
        await wait_event.wait()
        await asyncio.sleep(0.4)
        my_event.set()
        params = {'users_id': users, 'access_token': self.access_token, 'v': self.version}
        async with self.sessions['vk'].post(url=self.users_groups_url, params=params) as response:
            translate_json = await response.json()
            return translate_json

    async def walls_request(self, users: str, wait_event: asyncio.Event, my_event: asyncio.Event):
        """
        Запрос данных о постах пользователей через специальный метод API приложения
        :param users: Строка с id пользователей через запятую, НЕ БОЛЬШЕ 10!
        :param wait_event: Событие, которого ждет процедура, чтобы отправить запрос.
        :param my_event: Событие, в котором процедура сообщает, что запрос отправлен.
        :return: Словарь с ответами от API
        """
        await wait_event.wait()
        await asyncio.sleep(0.4)
        my_event.set()
        params = {'users_id': users, 'access_token': self.access_token, 'v': self.version}
        async with self.sessions['vk'].post(url=self.users_wall_url, params=params) as response:
            translate_json = await response.json()
            return translate_json

    # ========== УПОРЯДОЧИВАНИЕ ДАННЫХ ДЛЯ БД ==========
    @staticmethod
    async def user_data_analyse(user_dict: dict, fillers_list: list, counters_list: list):
        """
        Анализ данных профиля пользователя
        :param user_dict: JSON информация о пользователе
        :param fillers_list: Список заполнителей
        :param counters_list: Список счетчиков
        :return: Кортеж для записи в БД
        """
        rule = [None, '', [], 0]  # Правила, по которым считается, что параметра у пользователя нет

        # === ЗАПОЛНИТЕЛИ ===
        # Смотрим какие заполнители вообще есть
        profile_fillers = [0 if user_dict.get(filler) in rule else 1 for filler in fillers_list]
        # Отдельно смотрим наличие screen_name у пользователя
        profile_fillers.append(0 if user_dict['screen_name'] == 'id' + str(user_dict['id']) else 1)
        profile_fullness = sum(profile_fillers) / len(profile_fillers)  # Насколько заполнен профиль

        # === СЧЕТЧКИК ===
        # Смотрим, какие вообще счетчики есть
        haves_counter_list = [0 if user_dict['counters'].get(counter) in rule else 1 for counter in counters_list]
        # Собираем их значения
        profile_counter_list = [user_dict['counters'].get(counter, 0) for counter in counters_list]
        # Объединяем в структуру типа [есть ли счетчик1, сам счетчик1, есть ли счетчик2, сам счетчик2, ...]
        profile_counters = [item for pair in zip(haves_counter_list, profile_counter_list) for item in pair]
        counters_fullness = sum(haves_counter_list) / len(haves_counter_list)  # На сколько заполнены счетчики

        # Собираем всё в единый кортеж, главное - сохранить порядок
        save_values = [user_dict['id']]
        save_values.extend(profile_fillers)
        save_values.extend([profile_fullness])
        save_values.extend(profile_counters)
        save_values.extend([counters_fullness])
        return tuple(save_values)

    @staticmethod
    async def group_data_analyse(user_dict: list):
        """Анализ данных групп профиля"""
        items = user_dict[1]
        without_photo = 0
        closed_groups = 0
        type_page = 0
        type_group = 0
        for item in items['items']:
            without_photo += item.get('has_photo', 0)
            closed_groups += item['is_closed']
            if item['type'] == 'group':
                type_group += 1
            elif item['type'] == 'page':
                type_page += 1

        save_values = [int(user_dict[0]), items['count'], without_photo, closed_groups, type_page, type_group]
        return save_values

    @staticmethod
    async def wall_data_analyse(user_dict: list):
        """Анализ данных со стены профиля"""
        items = user_dict[1]
        posts = 0                   # Кол-во постов
        reposts = 0                 # Кол-во репостов
        max_id = items['count']     # Максимальный id с удаленными
        comments_counter = []       # Кол-во комментариев под всеми постами
        likes_counter = []          # Кол-во лайков под всеми постами
        views_counter = []          # Кол-во просмотров под всеми постами
        reposts_counter = []        # Кол-во репостов под всеми постами
        posts_with_text = 0         # Кол-во постов с текстом

        # Кол-во постов в ответе (если их всего больше 100, то и здесь обычно 100)
        posts_in_response = len(items['items']) if len(items['items']) != 0 else 1

        for item in items['items']:
            # Проходимся по всем постам и забираем разную статистику
            max_id = max(item['id'], max_id)    # Смотрим есть ли удаленные посты
            if 'copy_history' in item:          # Определяем репост это или нет
                reposts += 1
            else:
                posts += 1

            if 'comments' in item:
                comments_counter.append(item['comments']['count'])  # Смотрим сколько комментариев
            if 'likes' in item:
                likes_counter.append(item['likes']['count'])        # Смотрим сколько лайков
            if 'views' in item:
                views_counter.append(item['views']['count'])        # Смотрим сколько просмотров (не всегда есть)
            if 'reposts' in item:
                reposts_counter.append(item['reposts']['count'])    # Смотрим сколько репостов
            if item['text'].strip() != '':                          # И смотрим есть ли текст в посте
                posts_with_text += 1

        def mmmm(counter: list):
            """min, max, mean, median"""
            if len(counter) == 0:
                return [0, 0, 0, 0]
            else:
                return [min(counter), max(counter), fmean(counter), median(counter)]

        # Складываем все в правильном порядке
        save_values = [int(user_dict[0]), items['count'], posts/posts_in_response, reposts/posts_in_response, max_id]
        save_values.extend(mmmm(comments_counter))
        save_values.extend(mmmm(likes_counter))
        save_values.extend(mmmm(views_counter))
        save_values.extend(mmmm(reposts_counter))
        save_values.append(posts_with_text/posts_in_response)
        return save_values

    # ========== ЗАПИСЬ ДАННЫХ В БД ==========
    async def write_users_info(self, results: list):
        """Сохраняет данные по пользователям в БД"""
        for item in results:
            # Сначала сохраняем начальные данные о пользователе, его id, удален ли его профиль и закрыт ли
            await self.db_worker.save_user_result(item)

            # Потом разбираем более конкретно
            if item.get('deactivated') is None and not item['is_closed']:       # Если он не удален и открыт
                # То упорядочиваем данные как для открытого профиля
                save_values = await self.user_data_analyse(item, self.open_fillers_list, self.open_counters_list)
                await self.db_worker.save_open_profile_data(save_values)
            elif item.get('deactivated') is None and item['is_closed']:         # Профиль не удален, но закрыт
                # То упорядочиваем данные как для закрытого профиля
                save_values = await self.user_data_analyse(item, self.close_fillers_list, self.close_counters_list)
                await self.db_worker.save_close_profile_data(save_values)
            # И запоминаем изменения в БД
            await self.sessions['db'].commit()

    async def write_foaf(self, result: dict):
        """Записывает данные foaf в БД"""
        # Не спрашивай почему тут так мало. Так надо
        await self.db_worker.save_and_update_foaf_data(result)

    async def write_groups(self, results: list):
        """Сохраняет данные об группах пользователей в БД"""
        for item in results:
            if item[1] is False:
                await self.db_worker.remove_from_all_tables(int(item[0]))
                self.need_repeat = True
                # print(int(item[0]))
                # print(item)
            else:
                await self.db_worker.update_user_group(item)
                save_values = await self.group_data_analyse(item)
                await self.db_worker.save_group_data(save_values)

    async def write_posts(self, results: list):
        """Сохраняет данные об постах пользователей в БД"""
        for item in results:
            if item[1] is False:
                await self.db_worker.remove_from_all_tables(int(item[0]))
                self.need_repeat = True
                # print(int(item[0]))
                # print(item)
            else:
                await self.db_worker.update_user_wall(item)
                save_values = await self.wall_data_analyse(item)
                await self.db_worker.save_wall_data(save_values)




def main():
    pass


if __name__ == '__main__':
    main()
