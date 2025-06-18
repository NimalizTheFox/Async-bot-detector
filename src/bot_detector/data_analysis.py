import asyncio
import math
import datetime

from .database import DatabaseManager


def get_current_time() -> str:
    """Возвращает строку с текущим временем, нужно для логирования"""
    cur_time = datetime.datetime.now()
    return cur_time.strftime('%H:%M:%S')


async def analyse_all_profiles(data_folder: str):
    """Проводит все собранные профили через нейросеть для определения вероятности бота"""
    db = DatabaseManager(fr'{data_folder}\data.db')
    await db.connect()
    await db.create_tables()

    _, close_profiles, _, open_profiles = await db.get_all_profiles_info()

    print(f'[{get_current_time()}][INFO] Загружаем PyTorch для нейросети')
    from src.bot_detector.neural_models import PredictionModel

    for is_close in [False, True]:
        data_len = math.ceil((len(close_profiles) if is_close else len(open_profiles))/1000)
        print(f'[{get_current_time()}][INFO] Анализируем {"закрытые" if is_close else "открытые"} профили')

        nn_worker = PredictionModel(is_close)                                # Грузим нейронку
        generator = db.get_batched_data(is_close=is_close)   # Генератором забираем данные в батчах из БД

        iterator = 0
        async for batch in generator:
            iterator += 1
            print(f'\r\tГруппа (х1000): {iterator}/{data_len}', end='')

            data_to_neuro = [(row[0], row[1:]) for row in batch]    # Убираем id для нейронки
            result = nn_worker.model_predict(data_to_neuro)       # Получаем предсказание
            await db.save_analyse_result(result)             # И сохраняем все в таблицу
        print('')

        await generator.aclose()
    await db.close()


def start_analyse(data_folder: str):
    """Запускает проверку на ботность у всех собранных профилей"""
    print(f'\n\n[{get_current_time()}][INFO] Начинаем анализ!')
    asyncio.run(analyse_all_profiles(data_folder))


if __name__ == '__main__':
    start_analyse(r'..\data\not_bots')
