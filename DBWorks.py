import aiosqlite


class DBWorks:
    def __init__(self, session: aiosqlite.connect):
        """
        Класс, предназначенный для асинхронной работы с БД
        :param session: Сессия БД
        """
        self.session = session

    async def get_data_in_list(self, request):
        db_response = await self.session.execute(request)   # Отправка запроса к БД и ожидание ответа
        result = await db_response.fetchall()               # Преобразование ответа в список кортежей
        result_list = [item[0] for item in result]          # Преобразование в удобочитаемый список
        return result_list

    async def save_user_result(self, data):
        """Сохраняет первоначальные данные о пользователе, то есть его id, удален ли он и закрыт ли"""
        await self.session.execute(
            'INSERT INTO users (user_id, deactivated, is_close) VALUES(?, ?, ?)',
            (data['id'],
             0 if data.get('deactivated') is None else 1,
             1 if data['is_closed'] else 0))

    async def save_open_profile_data(self, data):
        """Сохраняет упорядоченные данные об открытом профиле в БД"""
        await self.session.execute(  # Сохраняем в БД
            f'INSERT INTO users_info_open VALUES({", ".join(["?" for _ in range(47)])})', data)

    async def save_close_profile_data(self, data):
        """Сохраняет упорядоченные данные о закрытом профиле в БД"""
        await self.session.execute(
            f'INSERT INTO users_info_close VALUES({", ".join(["?" for _ in range(17)])})', data)

    async def save_and_update_foaf_data(self, data):
        """Сохраняет отметку об прохождении профилем проверки foaf и сохраняет даты из foaf в БД"""
        await self.session.execute(
            'UPDATE users SET foaf_checked = ? WHERE user_id = ?', (1, data['id']))
        await self.session.execute(  # Сохраняем в БД
            f'INSERT INTO users_foaf VALUES(?, ?, ?)',
            (data['id'], data['life_time'], data['last_log_time']))

    async def update_user_group(self, data):
        """Сохраняет отметку о прохождении профилем проверки групп в БД"""
        await self.session.execute(
            'UPDATE users SET group_checked = ? WHERE user_id = ?', (1, int(data[0])))

    async def save_group_data(self, data):
        """Сохраняет упорядоченные данные о группах профиля в БД"""
        await self.session.execute(
            f'INSERT INTO users_groups VALUES({", ".join(["?" for _ in range(6)])})', data)

    async def update_user_wall(self, data):
        """Сохраняет отметку о прохождении профилем проверки постов в БД"""
        await self.session.execute(
            'UPDATE users SET wall_checked = ? WHERE user_id = ?', (1, int(data[0])))

    async def save_wall_data(self, data):
        """Сохраняет упорядоченные данные о постах профиля в БД"""
        await self.session.execute(
            f'INSERT INTO users_posts VALUES({", ".join(["?" for _ in range(22)])})', data)

    async def remove_from_all_tables(self, profile_id):
        """Удаляет пользователя из всех таблиц в БД для его переопределения"""
        await self.session.execute(f'DELETE FROM users WHERE user_id = ?', profile_id)
        await self.session.execute(f'DELETE FROM users_info_open WHERE user_id = ?', profile_id)
        await self.session.execute(f'DELETE FROM users_info_close WHERE user_id = ?', profile_id)
        await self.session.execute(f'DELETE FROM users_foaf WHERE user_id = ?', profile_id)
        await self.session.execute(f'DELETE FROM users_groups WHERE user_id = ?', profile_id)
        await self.session.execute(f'DELETE FROM users_posts WHERE user_id = ?', profile_id)
        await self.session.commit()

    async def create_tables(self):
        """Создание таблиц для БД"""
        # Таблица пользователей
        await self.session.execute(
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
        await self.session.execute(
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
        await self.session.execute(
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
        await self.session.execute(
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
        await self.session.execute(
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
        await self.session.execute(
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

        # И запись изменений на диск
        await self.session.commit()
