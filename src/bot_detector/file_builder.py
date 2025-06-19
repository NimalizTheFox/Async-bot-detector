import asyncio

import pandas as pd

from .database import DatabaseManager


async def build_output_file(data_folder: str, sheet_dict: dict, output_folder: str, original_file_name: str):
    """Создает выходной .xlsx файл"""
    output_file = fr'{output_folder}\{original_file_name} Прогноз.xlsx'

    db = DatabaseManager(fr'{data_folder}\data.db')
    await db.connect()
    await db.create_tables()

    results = await db.get_result_data()

    # Преобразуем список с результатами в словарь
    value_dict = {item[0]: item[1] for item in results}

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for sheet_name, id_list in sheet_dict.items():
            # Создаем данные для листа в нужном порядке
            sheet_data = []
            for item_id in id_list:
                # Если ID есть в данных - добавляем значение, иначе NaN
                value = value_dict.get(item_id, None)
                if value is None:
                    sheet_data.append([str(item_id), 1, 'Удален'])
                else:
                    sheet_data.append([str(item_id), value, ''])

            # Создаем DataFrame
            df = pd.DataFrame(sheet_data, columns=["ID", "Значение", "Статус"])

            # Записываем на лист
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    await db.close()


async def build_statistic_file(data_folder: str, sheet_dict: dict, output_folder: str, original_file_name: str):
    """Создает .txt файл со статистикой по ботам"""
    output_file = fr'{output_folder}\{original_file_name} Статистика.txt'

    db = DatabaseManager(fr'{data_folder}\data.db')
    await db.connect()
    await db.create_tables()

    results = await db.get_result_data()

    # Преобразуем список с результатами в словарь
    value_dict = {item[0]: item[1] for item in results}

    with open(output_file, 'w', encoding='utf-8') as file:
        for sheet_name, id_list in sheet_dict.items():
            ids_number = len(id_list)
            if ids_number == 0:
                file.write(f'{sheet_name} -\tАккаунты: 0,\tБоты: 0,\tОтношение: 0.0')
                continue

            bots = 0
            for item_id in id_list:
                value = value_dict.get(item_id, 1.0)
                if value >= 0.5:
                    bots += 1
            file.write(
                f'{sheet_name} -\tАккаунты: {ids_number},\tБоты: {bots},\tОтношение: {round(bots/ids_number, 4)}')

    await db.close()


def create_output_file(data_folder: str, sheet_dict: dict, output_folder: str, original_file_name: str):
    print('[INFO] Собираем выходной .xlsx файл')
    asyncio.run(build_output_file(data_folder, sheet_dict, output_folder, original_file_name))


def create_statistic_file(data_folder: str, sheet_dict: dict, output_folder: str, original_file_name: str):
    print('[INFO] Собираем файл статистики')
    asyncio.run(build_statistic_file(data_folder, sheet_dict, output_folder, original_file_name))
