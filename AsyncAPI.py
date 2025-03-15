import asyncio
import copy
import sys

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

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

config = configparser.ConfigParser()  # Создаём объект парсера
config.read("settings.ini", encoding='utf-8')  # Читаем конфиг


class AIOInfoGrabber:
    def __init__(self, users: list, data_folder: str, backup_seconds: int = 120):
        """
        Класс, предназначенный для сбора информации о множестве пользователей за малое время
        :param users: Список пользователей, которых нужно проверить (вообще все пользователи из документа)
        :param data_folder: Путь до папки, где будут храниться данные по текущему разбору (data/<название xls дока>)
        :param backup_seconds: Раз в сколько секунд информация сохраняется на диск, стандартное значение - 120 секунд
        """
        self.all_users_id = sorted(list(set([int(item) for item in users])))   # Чтобы точно было int, так как в БД тоже int
        self.data_folder = data_folder
        self.sessions = {}

        # Сколько запросов к API сделать за backup_seconds секунд
        # Задержка ответа на запрос о пользователях - 5 секунд, группах - 4 секунды, постах - 15 секунд, foaf - 0.6
        # Оставляется 10% на погрешность
        # Потом делим на 0.4 секунды, которые нужны чтобы API не заблокировался
        self.user_info_rounds = int((backup_seconds - 5) / 1.1 / 0.4)
        self.group_rounds = int((backup_seconds - 4) / 1.1 / 0.4)
        self.wall_rounds = int((backup_seconds - 15) / 1.1 / 0.4)
        self.foaf_rounds = int((backup_seconds - 0.6) / 1.1 / 0.004)

        # Ссылки на методы для сбора информации
        self.users_info_url = 'https://api.vk.com/method/execute.users_info'
        self.users_groups_url = 'https://api.vk.com/method/execute.groups_info'
        self.users_wall_url = 'https://api.vk.com/method/execute.walls_info'
        self.foaf_url = 'https://vk.com/foaf.php'

        self.access_token = config['VK']['access_token']
        self.version = config['VK']['version']

        # Запрос полей для API, а так же заполнители профиля у открытых
        fields_list = ["user_id", "about", "activities", "books", "career", "city", "country",
                       "has_photo", "has_mobile", "home_town", "schools", "status",
                       "games", "interests", "military", "movies", "music", "occupation", "personal",
                       "quotes", "relation", "universities", "screen_name", "verified", "counters"]
        self.fields_str = ','.join(fields_list)     # Все нужные поля юзеров в строку через запятую

        # Заполнители профилей
        self.open_fillers_list = fields_list[1:-3]
        self.close_fillers_list = ['city', 'has_photo', 'has_mobile', 'status', 'occupation']

        # Поля счетчиков
        self.open_counters_list = ['albums', 'audios', 'followers', 'friends', 'pages', 'photos', 'subscriptions',
                                   'videos', 'video_playlists', 'clips_followers', 'gifts']
        self.close_counters_list = ['friends', 'pages', 'subscriptions', 'posts']

        # Сразу при объявлении класса запускаем
        asyncio.run(self.start())

    async def start(self):
        # Разные подключения
        self.sessions['vk'] = ClientSession(timeout=ClientTimeout(total=30))
        self.sessions['db'] = aiosqlite.connect(fr'{self.data_folder}\data.db')

        async with self.sessions['vk'], self.sessions['db']:
            await self.create_tables()  # Создаем БД и таблички

            # Смотрим какие пользователи уже проверены и непроверенных проверяем
            # === ПОЛЬЗОВАТЕЛИ ===
            checked_users = await self.get_data_from_db('SELECT DISTINCT user_id FROM users')
            unchecked_users = sorted(list(set(self.all_users_id) - set(checked_users)))
            print(f'Всего: {len(self.all_users_id)}, '
                  f'Проверенно: {len(checked_users)}, '
                  f'Осталось: {len(unchecked_users)}')
            while len(unchecked_users) != 0:
                await self.users_info_process(self.all_users_id)    # Сбор данных из сети
                checked_users = await self.get_data_from_db('SELECT DISTINCT user_id FROM users')
                unchecked_users = sorted(list(set(self.all_users_id) - set(checked_users)))
                print(f'Всего: {len(self.all_users_id)}, '
                      f'Проверенно: {len(checked_users)}, '
                      f'Осталось: {len(unchecked_users)}')

            # === FOAF ===
            ids_to_foaf = await self.get_data_from_db(
                'SELECT DISTINCT user_id FROM users WHERE deactivated = 0 AND foaf_checked = 0')
            print(f'Предстоит проверить с помощью foaf: {len(ids_to_foaf)}')
            while len(ids_to_foaf) != 0:
                await self.foaf_process(ids_to_foaf)
                ids_to_foaf = await self.get_data_from_db(
                    'SELECT DISTINCT user_id FROM users WHERE deactivated = 0 AND foaf_checked = 0')
                print(f'Предстоит проверить с помощью foaf: {len(ids_to_foaf)}')

            # === ГРУППЫ ===
            ids_to_groups = await self.get_data_from_db(
                'SELECT DISTINCT user_id FROM users WHERE deactivated = 0 AND is_close = 0 AND group_checked = 0')
            print(f'Предстоит проверить группы у {len(ids_to_groups)} пользователей')
            while len(ids_to_groups) != 0:
                await self.groups_process(ids_to_groups)
                ids_to_groups = await self.get_data_from_db(
                    'SELECT DISTINCT user_id FROM users WHERE deactivated = 0 AND is_close = 0 AND group_checked = 0')
                print(f'Предстоит проверить группы у {len(ids_to_groups)} пользователей')

            # === ПОСТЫ ===

    async def get_data_from_db(self, request):
        db_response = await self.sessions['db'].execute(request)    # Отправка запроса к БД и ожидание ответа
        result = await db_response.fetchall()           # Преобразование ответа в список кортежей
        result_list = [item[0] for item in result]      # Преобразование в удобочитаемый список
        return result_list

    async def users_info_process(self, users_id):
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
                # if 'response' in item:
                if 'execute_errors' not in item and 'response' in item:
                    save_tasks.append(asyncio.create_task(self.write_users_info(item['response'])))
            await asyncio.gather(*save_tasks)

    async def foaf_process(self, users_id):
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
                save_tasks.append(asyncio.create_task(self.write_foaf(item)))
            await asyncio.gather(*save_tasks)
            await self.sessions['db'].commit()

    async def groups_process(self, users_id):
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
                    # Если есть ошибка 29, то запрещаем дальнейшее взаимодействие с методом, но цикл проходим
                    pass
                if 'response' in item:
                    save_tasks.append(asyncio.create_task(self.write_groups(item['response'])))
            await asyncio.gather(*save_tasks)

            await self.sessions['db'].commit()

    @staticmethod
    def list_split(data_list: list, items_in_round: int):
        """Разделение списка на подсписки с items_in_round кол-вом элементов в каждом"""
        rounds = []
        for i in range(math.ceil(len(data_list) / items_in_round)):
            rounds.append(data_list[i * items_in_round: i * items_in_round + items_in_round])
        return rounds

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
        """Запрос данных о времени создания профиля и времени последнего онлайна"""
        await wait_event.wait()
        await asyncio.sleep(0.0)
        my_event.set()

        async with self.sessions['vk'].get(url=self.foaf_url, params={'id': user}) as response:
            src = await response.text()
            soup = BeautifulSoup(src, "lxml")

            today = datetime.datetime.now()

            # Достаем дату создания профиля (её нет только у удаленных профилей)
            create_date = soup.find("ya:created").get("dc:date")
            create_date = datetime.datetime.strptime(create_date.split("T")[0], "%Y-%m-%d")
            life_time = (today - create_date).days

            # Достаем время последнего онлайна
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
        items = user_dict[1]

        try:
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

        except:
            print(int(user_dict[0]))
            print(user_dict)
            sys.exit()

        save_values = [int(user_dict[0]), items['count'], without_photo, closed_groups, type_page, type_group]
        return save_values



    async def create_tables(self):
        """Создание таблиц для БД"""
        # Таблица пользователей
        await self.sessions['db'].execute(
            """
            CREATE TABLE IF NOT EXISTS users
            (
                user_id INTEGER PRIMARY KEY,
                deactivated INTEGER, --0 или 1: 0 - действует, 1 - забанен/удален 
                is_close INTEGER, --закрыт ли профиль
                foaf_checked INTEGER DEFAULT 0, --просто проверены ди для этого профиля все остальное
                group_checked INTEGER DEFAULT 0,
                wall_checked INTEGER DEFAULT 0
            )
            """
        )

        # Таблица информации о закрытых пользователях
        await self.sessions['db'].execute(
            """
            CREATE TABLE IF NOT EXISTS users_info_close
            (
               user_id INTEGER PRIMARY KEY,
                --Заполнители профиля, только 0 или 1
                city INTEGER, has_photo INTEGER, has_mobile INTEGER, status INTEGER,
                occupation INTEGER, have_screen_name INTEGER,
                profile_fullness REAL, --На сколько из 6 параметров заполнен профиль (n/6) 
                
                --Счетчики, have_* только 0 или 1, остальные - полноценный int 
                have_friends INTEGER, friends INTEGER,
                have_pages INTEGER, pages INTEGER, 
                have_subscriptions INTEGER, subscriptions INTEGER,
                have_posts INTEGER, posts INTEGER,
                counters_fullness REAL --Сколько из 4 счетчиков есть у профиля (n/4)
            )
            """
        )

        # Таблица информации об открытых пользователях
        await self.sessions['db'].execute(
            """
            CREATE TABLE IF NOT EXISTS users_info_open
            (
                user_id INTEGER PRIMARY KEY, --ID пользователя
                --Заполнители профиля, только 0 или 1
                about INTEGER, activities INTEGER, books INTEGER, career INTEGER, city INTEGER, country INTEGER,
                has_photo INTEGER, has_mobile INTEGER, home_town INTEGER, schools INTEGER, status INTEGER, 
                games INTEGER, interests INTEGER, military INTEGER, movies INTEGER, music INTEGER,
                occupation INTEGER, personal INTEGER, quotes INTEGER, relation INTEGER, universities INTEGER,
                have_screen_name INTEGER,
                profile_fullness REAL, --На сколько из 22 параметров заполнен профиль (n/22) 
                            
                --Счетчики, have_* только 0 или 1, остальные - полноценный int 
                have_albums INTEGER, albums INTEGER, 
                have_audios INTEGER, audios INTEGER,
                have_followers INTEGER, followers INTEGER,
                have_friends INTEGER, friends INTEGER,
                have_pages INTEGER, pages INTEGER, 
                have_photos INTEGER, photos INTEGER,
                have_subscriptions INTEGER, subscriptions INTEGER,
                have_videos INTEGER, videos INTEGER,
                have_video_playlists INTEGER, video_playlists INTEGER,
                have_clips_followers INTEGER, clips_followers INTEGER, 
                have_gifts INTEGER, gifts INTEGER,
                counters_fullness REAL --Сколько из 11 счетчиков есть у профиля (n/11)
            );
            """
        )

        # Таблица информации о времени жизни и последнем онлайне пользователей
        await self.sessions['db'].execute(
            """
            CREATE TABLE IF NOT EXISTS users_foaf
            (
                user_id INTEGER PRIMARY KEY,
                life_days INTEGER,
                last_log_days INTEGER    
            );
            """
        )

        # Таблица информации о группах открытых пользователей
        await self.sessions['db'].execute(
            """
            CREATE TABLE IF NOT EXISTS users_groups
            (
                user_id INTEGER PRIMARY KEY,
                groups_count INTEGER,
                groups_without_photo INTEGER,
                closed_groups INTEGER,
                type_page INTEGER,
                type_group INTEGER
            );
            """
        )

        # Таблица информации о постах открытых пользователей
        await self.sessions['db'].execute(
            """
            CREATE TABLE IF NOT EXISTS users_posts
            (
                user_id INTEGER PRIMARY KEY,
                posts_count INTEGER, 
                posts_to_all_rel REAL, --Отношение постов ко всем
                reposts_to_all_rel REAL, --Отношение репостов ко всем 
                posts_max_id INTEGER, --Количество постов со всеми удаленными 
                posts_to_live_rel REAL, --Сколько постов в день за жизнь
                min_comms INTEGER, max_comms INTEGER, avg_comms REAL, mid_comms INTEGER, --Стат по комментариям
                min_likes INTEGER, max_likes INTEGER, avg_likes REAL, mid_likes INTEGER, --Стат по лайкам
                min_views INTEGER, max_views INTEGER, avg_views REAL, mid_views INTEGER, --Стат по просмотрам
                min_reposts INTEGER, max_reposts INTEGER, avg_reposts REAL, mid_reposts INTEGER, --Стат по репостам
                posts_with_text_rel REAL --Отношение постов с текстами ко всем 
            );
            """
        )

        # И запись изменений на диск
        await self.sessions['db'].commit()

    async def write_users_info(self, results: list):
        """
        Вытаскивает данные из ответов API и сохраняет их в БД SQLite
        :param results: ответ API
        """
        for item in results:
            # Сначала заполняем начальные данные о пользователе, его id, удален ли его профиль и закрыт ли
            await self.sessions['db'].execute(
                'INSERT INTO users (user_id, deactivated, is_close) VALUES(?, ?, ?)',
                (item['id'],
                 0 if item.get('deactivated') is None else 1,
                 1 if item['is_closed'] else 0)
            )

            # Потом разбираем более конкретно
            if item.get('deactivated') is None and not item['is_closed']:       # Если он не удален и открыт
                save_values = await self.user_data_analyse(item, self.open_fillers_list, self.open_counters_list)
                await self.sessions['db'].execute(                              # Сохраняем в БД
                    f'INSERT INTO users_info_open VALUES({", ".join(["?" for _ in range(47)])})',
                    save_values)
            elif item.get('deactivated') is None and item['is_closed']:         # Профиль не удален, но закрыт
                save_values = await self.user_data_analyse(item, self.close_fillers_list, self.close_counters_list)
                await self.sessions['db'].execute(
                    f'INSERT INTO users_info_close VALUES({", ".join(["?" for _ in range(17)])})',
                    save_values)

            # И запоминаем изменения
            await self.sessions['db'].commit()

    async def write_foaf(self, result: dict):
        await self.sessions['db'].execute(
            'UPDATE users SET foaf_checked = ? WHERE user_id = ?',
            (1, result['id'])
        )

        await self.sessions['db'].execute(  # Сохраняем в БД
            f'INSERT INTO users_foaf VALUES(?, ?, ?)',
            (result['id'], result['life_time'], result['last_log_time']))

    async def write_groups(self, results: list):
        for item in results:
            await self.sessions['db'].execute(
                'UPDATE users SET group_checked = ? WHERE user_id = ?',
                (1, int(item[0]))
            )

            save_values = await self.group_data_analyse(item)
            await self.sessions['db'].execute(
                f'INSERT INTO users_groups VALUES({", ".join(["?" for _ in range(6)])})',
                save_values)

    async def write_walls(self, results: list):
        pass




def main():
    pass


if __name__ == '__main__':
    main()
