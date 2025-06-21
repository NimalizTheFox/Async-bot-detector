import pandas as pd
import openpyxl


def txt_parser(file_path: str) -> tuple[list, dict]:
    """
    Разбирает .txt файл на список id.

    ТОЛЬКО ЧИСЛОВЫЕ ID! **123456** и **id123456** можно, но **vasya_pupkin** уже нельзя.
    Еще - группы (**club123456**) **не** обрабатываются, только профили пользователей!
    :param file_path: Путь до разбираемого файла.
    :return: Список всех ID и словарь с ID на каждой странице
    """
    with open(file_path, 'r') as file:
        ids = file.read().strip().split('\n')

    result_list = []
    for profile_id in ids:
        if not profile_id.isdigit():
            if not profile_id[:2] == 'id' or not profile_id[2:].isdigit():
                print(f'[ID WARNING] В файле есть id с буквами: "{profile_id}". Он не будет включен в список id. ')
            else:
                profile_id = profile_id[2:]
                result_list.append(int(profile_id))
        else:
            result_list.append(int(profile_id))

    return list(set(result_list)), {'1': result_list}


def xlsx_parser(file_path: str, columns_with_id: list, have_headings: bool) -> tuple[list, dict]:
    """
    Разбирает .xlsx файл на список id.

    ТОЛЬКО ЧИСЛОВЫЕ ID! **123456** и **id123456** можно, но **vasya_pupkin** уже нельзя.
    Еще - группы (**club123456**) **не** обрабатываются, только профили пользователей!
    :param file_path: Путь до разбираемого файла
    :param columns_with_id: Список индексов колонок с id профилей
    :param have_headings: Есть ли у файла заголовки
    :return: Список всех ID и словарь с ID на каждой странице
    """
    with pd.ExcelFile(file_path, engine='openpyxl') as xls:
        all_ids = []        # Список всех id в файле
        sheet_dict = {}     # Для сортировки выходного файла

        for sheet_name in xls.sheet_names:
            # Берем информацию с листа и преобразуем её в список
            # (Да, оно выделено как предупреждение, но лишь потому что разрабы pd забыли поменять тип данных у usecols)
            flat_list = list(pd.read_excel(
                xls, sheet_name, usecols=columns_with_id, header=0 if have_headings else None
            ).values.flatten().tolist())

            # Избавляемся от nan
            ids_list = [item for item in flat_list if item == item]

            new_list = []
            for item in ids_list:
                # Если это ссылка, то получаем последнюю часть
                if '/' in item:
                    item = item.split('/')[-1]

                    # И если оно начинается с id, то добавляем в список
                    if item.startswith('id'):
                        new_list.append(item[2:])
                else:
                    # Если это не ссылка, а так же id в начале или это просто число, то записываем его в список
                    if item.startswith('id'):
                        new_list.append(item[2:])
                    elif item.isdigit():
                        new_list.append(item)

            # Избавляемся от id групп, оставляем только числа
            ids_list = [int(item[item.rfind('/') + 3:])
                        for item in ids_list
                        if item[item.rfind('/') + 1: item.rfind('/') + 3] == 'id']

            # Записываем в список всех id и в словарь для сортировки
            all_ids.extend(ids_list)
            sheet_dict[sheet_name] = ids_list

        return list(set(all_ids)), sheet_dict
