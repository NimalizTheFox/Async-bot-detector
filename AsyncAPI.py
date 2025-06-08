import asyncio
import aiohttp
import datetime
import math
from aiohttp import ClientSession, ClientTimeout
from statistics import fmean, median
from DBWorks import AioDBWorks


def get_current_time() -> str:
    """Возвращает строку с текущим временем, нужно для логирования"""
    cur_time = datetime.datetime.now()
    return cur_time.strftime('%H:%M:%S')


class AIOInfoGrabber:
    def __init__(self, users: list,
                 data_folder: str,
                 access_token: str,
                 proxy: str = None,
                 proxy_auth: list[str, str] = None,
                 need_prints: bool = False,
                 round_seconds: int = 120):
        """
        Класс, предназначенный для сбора информации о множестве пользователей за малое время
        :param users: Список пользователей, которых нужно проверить
        :param data_folder: Путь до папки, где будут храниться данные по текущему разбору (data/<название xls дока>)
        :param access_token: Токен от VK API
        :param proxy: Прокси, если есть
        :param proxy_auth: Логин и пароль для прокси, если есть
        :param need_prints: Нужны ли информационные принты
        :param round_seconds: Раз в сколько секунд информация сохраняется на диск, стандартное значение - 120 секунд
        """
        self.all_users_id = sorted(list(set([int(item) for item in users])))
        self.data_folder = data_folder
        self.proxy = proxy
        self.proxy_auth = proxy_auth
        self.requests_session: ClientSession | None = None
        self.db_worker: AioDBWorks | None = None
        self.need_print = need_prints

        # Сколько запросов к API сделать за backup_seconds секунд
        # Задержка ответа на запрос о пользователях - 5 секунд, группах - 4 секунды, постах - 15 секунд
        # Оставляется 10% на погрешность
        # Потом делим на 0.4 секунды, которые нужны чтобы API не заблокировался,
        # Все значения для стандартного значения в 120 секунд
        self.user_info_rounds = int((round_seconds - 5) / 1.1 / 0.4)    # 261 пакет id, по 25, всего 6 525 id
        self.group_rounds = int((round_seconds - 4) / 1.1 / 0.4)        # 263 пакета id, по 25, всего 6 575 id
        self.wall_rounds = int((round_seconds - 15) / 1.1 / 0.4)        # 238 покетов id, по 10, всего 2 380 id

        # Ссылки на методы для сбора информации
        self.users_info_url = 'https://api.vk.com/method/execute.users_info'
        self.users_groups_url = 'https://api.vk.com/method/execute.groups_info'
        self.users_wall_url = 'https://api.vk.com/method/execute.walls_info'

        # Данные для API
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

    async def start(self, method: str) -> tuple[dict[str, bool], bool]:
        """
        ВЫПОЛНЯТЬ С ПОМОЩЬЮ asyncio.run(AIOInfoGrabber(*).start(method))!
        Выполняет сбор информации по выбранному методу.
        :param method: 'users', 'groups' и 'walls', только они.
        :return: Словарь лимитов и нужен ли повтор
        """
        if method not in ['users', 'groups', 'walls']:
            raise Exception('Некорректно указанный метод для AIOInfoGrabber(*).start(method)')

        # Разные подключения
        # Если есть прокси, то
        proxy_auth = aiohttp.BasicAuth(self.proxy_auth[0], self.proxy_auth[1]) if self.proxy_auth is not None else None
        self.requests_session = ClientSession(timeout=ClientTimeout(total=30), proxy=self.proxy, proxy_auth=proxy_auth)

        self.db_worker = AioDBWorks(fr'{self.data_folder}\data.db')
        await self.db_worker.connect()
        await self.db_worker.create_tables()

        # Используем одну сессию для всех запросов, так как это быстрее
        async with self.requests_session:
            # === ПОЛЬЗОВАТЕЛИ ===
            if method == 'users':
                # Смотрим какие пользователи уже проверены и непроверенных проверяем
                checked_users = await self.db_worker.get_checked_profiles()
                unchecked_users = sorted(list(set(self.all_users_id) - set(checked_users)))
                if self.need_print:
                    print(f'\tВсего: {len(self.all_users_id)}, '
                          f'Проверенно: {len(checked_users)}, '
                          f'Осталось: {len(unchecked_users)}')
                while len(unchecked_users) != 0 and not self.limit_reached['users']:
                    await self.users_info_process(unchecked_users)    # Сбор данных из сети
                    checked_users = await self.db_worker.get_checked_profiles()
                    unchecked_users = sorted(list(set(self.all_users_id) - set(checked_users)))
                    if self.need_print:
                        print(f'\tВсего: {len(self.all_users_id)}, '
                              f'Проверенно: {len(checked_users)}, '
                              f'Осталось: {len(unchecked_users)}')
                if len(unchecked_users) != 0:
                    self.need_repeat = True

            # === ГРУППЫ ===
            elif method == 'groups':
                ids_to_groups = await self.db_worker.get_profiles_to_group_check()
                if self.need_print:
                    print(f'\tПредстоит проверить группы у {len(ids_to_groups)} пользователей')
                while len(ids_to_groups) != 0 and not self.limit_reached['groups']:
                    await self.groups_process(ids_to_groups)
                    ids_to_groups = await self.db_worker.get_profiles_to_group_check()
                    if self.need_print:
                        print(f'\tПредстоит проверить группы у {len(ids_to_groups)} пользователей')
                if len(ids_to_groups) != 0:
                    self.need_repeat = True

            # === ПОСТЫ ===
            elif method == 'walls':
                ids_to_posts = await self.db_worker.get_profiles_to_wall_check()
                if self.need_print:
                    print(f'\tПредстоит проверить посты у {len(ids_to_posts)} пользователей')
                while len(ids_to_posts) != 0 and not self.limit_reached['walls']:
                    await self.posts_process(ids_to_posts)
                    ids_to_posts = await self.db_worker.get_profiles_to_wall_check()
                    if self.need_print:
                        print(f'\tПредстоит проверить посты у {len(ids_to_posts)} пользователей')
                if len(ids_to_posts) != 0:
                    self.need_repeat = True

            # Возврааем словарь достигнутых лимитов и нужно ли повторение
            return self.limit_reached, self.need_repeat

    # ========== ПРОЦЕССЫ ДЛЯ ОБРАБОТКИ API ==========
    async def users_info_process(self, users_id):
        """Разбитие на раунды сохранения и выполнение сбора информации по пользователям"""
        start_event = asyncio.Event()
        start_event.set()

        # Собираем раунды со списками id пользователей (по 25 id в раунде)
        users_id_str = [str(item) for item in users_id]     # Т.к. все id хранятся в int - преобразуем в str
        id_rounds_to_info = [','.join(round_ids) for round_ids in self.list_split(users_id_str, 25)]
        info_rounds = self.list_split(id_rounds_to_info, self.user_info_rounds)  # И разбиваем по раундам сохранения
        if self.need_print:
            print(f'\t[{get_current_time()}] Всего раундов: {len(info_rounds)}')

        # Проходимся по всем раундам с id (примерно по 2 минуты на раунд)
        for round_ids in info_rounds:
            # Если API больше не отвечает на метод, то заканчиваем
            if self.limit_reached['users']:
                if self.need_print:
                    print(f'\t[{get_current_time()}][ERROR] Достигнут лимит метода users!')
                break

            if self.need_print:
                print(f'\t[{get_current_time()}] Начинается раунд {info_rounds.index(round_ids) + 1}')

            # Подготавливаем задачи, в которых будут крутиться потоки со сбором информации
            events = [asyncio.Event() for _ in range(len(round_ids))]  # Создаем события для каждого потока
            # Создаем задачи для каждого потока
            tasks = [asyncio.create_task(self.users_info_request(round_ids[0], start_event, events[0]))]
            for i in range(1, len(round_ids)):
                tasks.append(asyncio.create_task(self.users_info_request(round_ids[i], events[i - 1], events[i])))

            # Запускаем задачи и ждем их завершения
            results = await asyncio.gather(*tasks)

            # Сохраняем результаты
            save_tasks = []
            for item in results:
                if 'execute_errors' in item:
                    self.need_repeat = True     # Если API вернул ошибку, то нужно будет снова собрать информацию
                    limit_reached_here = False
                    # Если есть ошибка 29, то запрещаем ключу дальнейшее взаимодействие с методом
                    for error in item['execute_errors']:
                        if error['error_code'] == 29:
                            limit_reached_here = True
                            self.limit_reached['groups'] = True
                    if limit_reached_here:  # Если это была ошибка 29, то переходим к следующему ответу
                        continue
                if 'response' in item:
                    save_tasks.append(asyncio.create_task(self.write_users_info(item['response'])))
            await asyncio.gather(*save_tasks)

        # Иногда БД капризничает и не записывает некоторые профили в таблички, так что перепроверяем.
        # (Это было один раз и я не уверен с чем это было связано, но на всякий случай оставлю)
        # Собираем из БД информацию по закрытым и открытым профилям
        close_profiles, close_info, open_profiles, open_info = await self.db_worker.get_all_profiles_info()

        # Если профиль есть в списке профилей, но по неу нет информации, то его нужно перепроверить
        to_recheck = sorted(list(set(close_profiles) - set(close_info)) + list(set(open_profiles) - set(open_info)))

        # Если такие профили есть, то удаляем их, чтобы на следующем круге перепроверить
        if len(to_recheck) > 0:
            self.need_repeat = True
            # print(f'\t[{get_current_time()}] Нужно перепроверить {len(to_recheck)} профилей')
        for item in to_recheck:
            await self.db_worker.remove_from_all_tables(int(item))
        await self.db_worker.save_db()

    async def groups_process(self, users_id):
        """Разбитие на раунды сохранения и выполнение сбора информации по группам пользователей"""
        start_event = asyncio.Event()
        start_event.set()

        # Собираем раунды со списками id пользователей
        users_id_str = [str(item) for item in users_id]
        id_rounds_to_info = [','.join(round_ids) for round_ids in self.list_split(users_id_str, 25)]
        rounds = self.list_split(id_rounds_to_info, self.group_rounds)  # И разбиваем по раундам сохранения

        if self.need_print:
            print(f'\t[{get_current_time()}] Всего раундов {len(rounds)}')

        for round_ids in rounds:  # Примерно по две минуты на раунд
            if self.limit_reached['groups']:
                if self.need_print:
                    print(f'\t[{get_current_time()}][ERROR] Достигнут лимит метода groups!')
                break

            if self.need_print:
                print(f'\t[{get_current_time()}] Начинается раунд {rounds.index(round_ids) + 1}')

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
                    if limit_reached_here:  # Если это была ошибка 29, то переходим к следующему ответу
                        continue
                if 'response' in item:
                    save_tasks.append(asyncio.create_task(self.write_groups(item['response'])))
            await asyncio.gather(*save_tasks)

    async def posts_process(self, users_id):
        """Разбитие на раунды сохранения и выполнение сбора информации по постам пользователей"""
        start_event = asyncio.Event()
        start_event.set()

        # Собираем раунды со списками id пользователей
        users_id_str = [str(item) for item in users_id]
        id_rounds_to_info = [','.join(round_ids) for round_ids in self.list_split(users_id_str, 10)]
        rounds = self.list_split(id_rounds_to_info, self.wall_rounds)  # И разбиваем по раундам сохранения\

        if self.need_print:
            print(f'\t[{get_current_time()}] Всего раундов {len(rounds)}')

        for round_ids in rounds:  # Примерно по две минуты на раунд
            if self.limit_reached['walls']:
                if self.need_print:
                    print(f'\t[{get_current_time()}][ERROR] Достигнут лимит метода walls!')
                break

            if self.need_print:
                print(f'\t[{get_current_time()}] Начинается раунд {rounds.index(round_ids) + 1}')

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
                            self.limit_reached['walls'] = True
                    if limit_reached_here:  # Если это была ошибка 29, то переходим к следующему ответу
                        continue
                if 'response' in item:
                    save_tasks.append(asyncio.create_task(self.write_posts(item['response'])))
            await asyncio.gather(*save_tasks)

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
        await wait_event.wait()     # Ожидание своей очереди на отправку запроса
        await asyncio.sleep(0.4)    # Ждем минимум 0.34 секунды, чтобы API не выдал ошибку о слишком частых запросах
        my_event.set()              # Говорим следующему в очереди потоку начинать считать свои 0.34 секунды
        params = {'users_id': users, 'fields': self.fields_str, 'access_token': self.access_token, 'v': self.version}

        # Отправляем запрос к API и ждем ответа
        async with self.requests_session.post(url=self.users_info_url, params=params) as response:
            translate_json = await response.json()  # Преобразуем ответ в понятный словарь
            return translate_json

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
        async with self.requests_session.post(url=self.users_groups_url, params=params) as response:
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
        async with self.requests_session.post(url=self.users_wall_url, params=params) as response:
            translate_json = await response.json()
            return translate_json

    # ========== УПОРЯДОЧИВАНИЕ ДАННЫХ ДЛЯ БД ==========
    @staticmethod
    async def user_data_analyse(user_dict: dict, fillers_list: list, counters_list: list) -> tuple:
        """
        Анализ данных профиля пользователя
        :param user_dict: JSON информация о пользователе
        :param fillers_list: Список заполнителей (поля у которых нет четкой структуры (например - статус))
        :param counters_list: Список счетчиков
        :return: Кортеж для записи в БД
        """
        rule = [None, '', [], 0]  # Правила, по которым считается, что параметра у пользователя нет

        # === ЗАПОЛНИТЕЛИ ===
        # Смотрим какие заполнители вообще есть
        profile_fillers = [0 if user_dict.get(filler) in rule else 1 for filler in fillers_list]

        # Отдельно смотрим наличие screen_name у пользователя, ставим 0 если он стандартный и не менялся
        profile_fillers.append(0 if user_dict['screen_name'] == 'id' + str(user_dict['id']) else 1)
        profile_fullness = sum(profile_fillers) / len(profile_fillers)  # Насколько заполнен профиль

        # === СЧЕТЧКИК ===
        # Смотрим, какие вообще счетчики есть
        if user_dict.get('counters') is not None:   # У некоторых профилей вообще нет счетчиков, как так - без понятия
            # Если ли данный счетчик
            haves_counter_list = [0 if user_dict['counters'].get(counter) in rule else 1 for counter in counters_list]
            # И значение счетчика
            profile_counter_list = [user_dict['counters'].get(counter, 0) for counter in counters_list]
        else:
            # Если счетчиков у профиля нет, то везде 0
            haves_counter_list = [0 for _ in counters_list]
            profile_counter_list = [0 for _ in counters_list]

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
    async def write_users_info(self, results: list) -> None:
        """Сохраняет данные по пользователям в БД"""
        for item in results:
            # Записываем общую информацию о профиле (его id, удален ли, закрыт ли)
            await self.db_worker.save_user_result(item)

            # Если профиль не удален и открыт, то упорядочиваем данные как для открытого профиля
            if item.get('deactivated') is None and not item['is_closed']:
                save_values = await self.user_data_analyse(item, self.open_fillers_list, self.open_counters_list)
                await self.db_worker.save_open_profile_data(save_values)        # Записываем конкретно для открытого

            # Если профиль не удален, но закрыт, то упорядочиваем данные как для закрытого профиля
            elif item.get('deactivated') is None and item['is_closed']:
                save_values = await self.user_data_analyse(item, self.close_fillers_list, self.close_counters_list)
                await self.db_worker.save_close_profile_data(save_values)       # Записываем конкретно для закрытого

            # И запоминаем изменения в БД
            await self.db_worker.save_db()

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
            await self.db_worker.save_db()

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
            await self.db_worker.save_db()


def main():
    pass


if __name__ == '__main__':
    main()
