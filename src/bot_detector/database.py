import aiosqlite


class DatabaseManager:
    def __init__(self, file: str):
        """
        Класс, предназначенный для асинхронной работы с БД
        :param file: Файл Базы Данных
        """
        self.session: aiosqlite.Connection | None = None
        self.db_file = file

    async def connect(self):
        """Соединение с БД"""
        self.session = await aiosqlite.connect(self.db_file)

    async def close(self):
        await self.session.close()

    async def save_db(self):
        """Сохранение БД, вынесено в отдельную функцию, чтоб сохранять БД после раундов"""
        await self.session.commit()

    async def get_data_in_list(self, request: str):
        async with self.session.cursor() as curr:
            db_response = await curr.execute(request)   # Отправка запроса к БД и ожидание ответа
            result = await db_response.fetchall()               # Преобразование ответа в список кортежей
            result_list = [item[0] for item in result]          # Преобразование в удобочитаемый список
            return result_list

    async def get_checked_profiles(self):
        """Возвращает все id, которые уже записаны в БД"""
        return await self.get_data_in_list('SELECT user_id FROM users')

    async def get_profiles_to_group_check(self):
        """Возвращает id профилей, у которых еще не проверены группы """
        return await self.get_data_in_list(
            'SELECT user_id FROM users WHERE deactivated = 0 AND is_close = 0 AND group_checked = 0')

    async def get_profiles_to_wall_check(self):
        """Возвращает id профилей, у которых еще не проверена стена """
        return await self.get_data_in_list(
            'SELECT user_id FROM users WHERE deactivated = 0 AND is_close = 0 AND wall_checked = 0')

    async def get_all_profiles_info(self):
        """Собирает все данные по всем профилям (и только профилям, без групп и стен)"""
        close_profiles = await self.get_data_in_list(
            'SELECT user_id FROM users WHERE deactivated = 0 AND is_close = 1')
        close_info = await self.get_data_in_list(
            'SELECT user_id FROM users_info_close')
        open_profiles = await self.get_data_in_list(
            'SELECT user_id FROM users WHERE deactivated = 0 AND is_close = 0')
        open_info = await self.get_data_in_list(
            'SELECT user_id FROM users_info_open')
        return close_profiles, close_info, open_profiles, open_info

    async def get_batched_data(self, is_close: bool, batch_size=1000):
        """Генератор, который выдает по batch_size записей из нужной таблицы"""
        last_id = 0
        async with self.session.cursor() as curr:
            while True:
                await curr.execute(f"""
                    SELECT * FROM users_info_{'close' if is_close else 'open'} 
                    WHERE user_id > ? 
                    ORDER BY user_id 
                    LIMIT ?
                """, (last_id, batch_size))

                batch = await curr.fetchall()
                if not batch:
                    return

                last_id = batch[-1][0]  # Запоминаем последний ID
                yield batch

    async def get_result_data(self):
        async with self.session.cursor() as curr:
            db_response = await curr.execute('SELECT * FROM results')
            result = await db_response.fetchall()
            return result

    async def save_analyse_result(self, data: list):
        async with self.session.cursor() as curr:
            for item in data:
                await curr.execute("""
                    INSERT INTO results (user_id, bot_prob)
                    VALUES (?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        bot_prob = excluded.bot_prob
                    """, item)
            await self.session.commit()

    async def save_user_result(self, data):
        """Сохраняет первоначальные данные о пользователе, то есть его id, удален ли он и закрыт ли"""
        async with self.session.cursor() as curr:
            await curr.execute(
                'INSERT INTO users (user_id, deactivated, is_close) VALUES(?, ?, ?)',
                (data['id'],
                 0 if data.get('deactivated') is None else 1,
                 1 if data['is_closed'] else 0))

    async def save_open_profile_data(self, data):
        """Сохраняет упорядоченные данные об открытом профиле в БД"""
        async with self.session.cursor() as curr:
            await curr.execute(  # Сохраняем в БД
                f'INSERT INTO users_info_open VALUES({", ".join(["?" for _ in range(46)])})', data)

    async def save_close_profile_data(self, data):
        """Сохраняет упорядоченные данные о закрытом профиле в БД"""
        async with self.session.cursor() as curr:
            await curr.execute(
                f'INSERT INTO users_info_close VALUES({", ".join(["?" for _ in range(17)])})', data)

    async def update_user_group(self, data):
        """Сохраняет отметку о прохождении профилем проверки групп в БД"""
        async with self.session.cursor() as curr:
            await curr.execute(
                'UPDATE users SET group_checked = ? WHERE user_id = ?', (1, int(data[0])))

    async def save_group_data(self, data):
        """Сохраняет упорядоченные данные о группах профиля в БД"""
        async with self.session.cursor() as curr:
            await curr.execute(
                f'INSERT INTO users_groups VALUES({", ".join(["?" for _ in range(6)])})', data)

    async def update_user_wall(self, data):
        """Сохраняет отметку о прохождении профилем проверки постов в БД"""
        async with self.session.cursor() as curr:
            await curr.execute(
                'UPDATE users SET wall_checked = ? WHERE user_id = ?', (1, int(data[0])))

    async def save_wall_data(self, data):
        """Сохраняет упорядоченные данные о постах профиля в БД"""
        async with self.session.cursor() as curr:
            await curr.execute(
                f'INSERT INTO users_posts VALUES({", ".join(["?" for _ in range(22)])})', data)

    async def remove_from_all_tables(self, profile_id):
        """Удаляет пользователя из всех таблиц в БД для его переопределения"""
        async with self.session.cursor() as curr:
            await curr.execute(f'DELETE FROM users WHERE user_id = ?', (profile_id, ))
            await curr.execute(f'DELETE FROM users_info_open WHERE user_id = ?', (profile_id, ))
            await curr.execute(f'DELETE FROM users_info_close WHERE user_id = ?', (profile_id, ))
            await curr.execute(f'DELETE FROM users_groups WHERE user_id = ?', (profile_id, ))
            await curr.execute(f'DELETE FROM users_posts WHERE user_id = ?', (profile_id, ))
            await curr.execute(f'DELETE FROM results WHERE user_id = ?', (profile_id, ))

            await self.session.commit()

    async def create_tables(self):
        """Создание таблиц для БД"""
        async with self.session.cursor() as curr:
            # Таблица пользователей
            await curr.execute(
                """
                CREATE TABLE IF NOT EXISTS users
                (
                    user_id INTEGER PRIMARY KEY,
                    deactivated INTEGER, --0 или 1: 0 - действует, 1 - забанен/удален 
                    is_close INTEGER, --закрыт ли профиль
                    group_checked INTEGER DEFAULT 0,
                    wall_checked INTEGER DEFAULT 0
                )
                """
            )

            # Таблица информации о закрытых пользователях
            await curr.execute(
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
            await curr.execute(
                """
                CREATE TABLE IF NOT EXISTS users_info_open
                (
                    user_id INTEGER PRIMARY KEY, --ID пользователя
                    --Заполнители профиля, только 0 или 1
                    about INTEGER, activities INTEGER, books INTEGER, career INTEGER, city INTEGER,
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

            # Таблица информации о группах открытых пользователей
            await curr.execute(
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
            await curr.execute(
                """
                CREATE TABLE IF NOT EXISTS users_posts
                (
                    user_id INTEGER PRIMARY KEY,
                    posts_count INTEGER, 
                    posts_to_all_rel REAL, --Отношение постов ко всем
                    reposts_to_all_rel REAL, --Отношение репостов ко всем 
                    posts_max_id INTEGER, --Количество постов со всеми удаленными 
                    min_comms INTEGER, max_comms INTEGER, avg_comms REAL, mid_comms INTEGER, --Стат по комментариям
                    min_likes INTEGER, max_likes INTEGER, avg_likes REAL, mid_likes INTEGER, --Стат по лайкам
                    min_views INTEGER, max_views INTEGER, avg_views REAL, mid_views INTEGER, --Стат по просмотрам
                    min_reposts INTEGER, max_reposts INTEGER, avg_reposts REAL, mid_reposts INTEGER, --Стат по репостам
                    posts_with_text_rel REAL --Отношение постов с текстами ко всем 
                );
                """
            )

            # Таблица для записи результатов анализа
            await curr.execute(
                """
                CREATE TABLE IF NOT EXISTS results
                (
                    user_id INTEGER PRIMARY KEY,
                    bot_prob REAL --Вероятность того, что профиль - бот
                );
                """
            )

            # И запись изменений на диск
            await self.session.commit()
