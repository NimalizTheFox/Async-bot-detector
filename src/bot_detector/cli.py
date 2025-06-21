import os
import sys
import argparse

from src.bot_detector.config_manager import TokenManager, ProxyManager
from src.bot_detector.file_parser import txt_parser, xlsx_parser
from src.bot_detector.data_collector import take_data
from src.bot_detector.data_analysis import start_analyse
from src.bot_detector.paths import DATA_DIR
from src.bot_detector.file_builder import create_statistic_file, create_output_file


def red(text: str):
    """Делает текст красным"""
    return "\033[31m{}\033[0m".format(text)


def green(text: str):
    """Делает текст зеленым"""
    return "\033[32m{}\033[0m".format(text)


def parsing_arguments(args) -> tuple[argparse.ArgumentParser, argparse.Namespace]:
    """Парсит аргументы из командной строки"""
    parser = argparse.ArgumentParser(description='CLI для асинхронного бот-детектора')
    subparsers = parser.add_subparsers(dest='command', help='Доступные команды')

    # === Команды для анализа ===
    parser_analyse = subparsers.add_parser('analyse', help='Начать анализ для профилей из файла')
    parser_analyse.add_argument('input', type=str,
                                help='Путь к файлу с данными для анализа (только .xlsx или .txt)')
    parser_analyse.add_argument('-o', '--output', type=str,
                                help='Папка для выходного файла (по умолчанию - там же где входной файл)')
    parser_analyse.add_argument('-f', '--original_off', action='store_true',
                                help='Убирает адрес оригинальной машины из списка '
                                     'прокси (игнорируется если нет прокси).')
    parser_analyse.add_argument('-s', '--statistic', action='store_true',
                                help='Помещает в папку с выходным файлом .txt файл со статистикой по ботам')
    parser_analyse.add_argument('-c', '--columns', type=str,
                                help='Только для .xlsx файлов! Нужно указать индексы колонок, в которых лежат '
                                     'id профилей (отсчет индексов идет от нуля). '
                                     'Если их несколько, то писать через запятую без пробелов: 0,1,2')
    parser_analyse.add_argument('-t', '--titled', action='store_true',
                                help='Только для .xlsx файлов! Указать, если внутри файла есть заголовки.')

    # === Команды для управления прокси ===
    parser_proxy = subparsers.add_parser('proxy', help='Управление прокси')
    parser_proxy.add_argument('-f', '--original_off', action='store_true',
                              help='Убирает адрес оригинальной машины из списка прокси (игнорируется если нет прокси).')
    proxy_subparser = parser_proxy.add_subparsers(dest='proxy_command', help='Действия с прокси')

    # Добавить прокси в список
    new_proxy = proxy_subparser.add_parser('new', help='Добавить прокси в список')
    new_proxy.add_argument('-a', '--address', type=str, required=True, help='Адрес прокси')
    new_proxy.add_argument('-l', '--login', type=str, help='Логин для авторизации прокси (если есть)')
    new_proxy.add_argument('-p', '--password', type=str, help='Пароль для авторизации прокси (если есть)')

    # Удалить прокси из списка
    del_proxy = proxy_subparser.add_parser('delete', help='Удалить прокси из списка')
    del_proxy.add_argument('-a', '--address', type=str, required=True, help='Адрес прокси, который нужно удалить')

    # Вывести список
    proxy_subparser.add_parser('show', help='Показать все доступные прокси')

    # === Команды для управления токенами API ===
    parser_token = subparsers.add_parser('token', help='Управление токенами VK API')
    token_subparser = parser_token.add_subparsers(dest='token_command', help='Действия с токенами VK API')

    # Добавить токен
    new_token = token_subparser.add_parser('new', help='Добавить токен VK API в список')
    new_token.add_argument('-t', '--token', type=str, required=True, help='Сам токен VK API')

    # Добавить токен
    del_token = token_subparser.add_parser('delete', help='Удалить токен VK API из списка')
    del_token.add_argument('-t', '--token', type=str, required=True, help='Сам токен VK API')

    # Вывести список токенов API
    token_subparser.add_parser('show', help='Вывести список со всеми токенами VK API')

    # Если нет аргументов, то выводится справка
    if len(args) == 0:
        parser.print_help()
        sys.exit(1)

    return parser, parser.parse_args(args)


def show_list(message: str, data: list) -> None:
    print(message)
    for item in data:
        print('\t', item)


def main(args):
    parser, args = parsing_arguments(args)

    if args.command == 'analyse':
        # Проверяем входной файл
        if not os.path.isfile(args.input):
            raise parser.error(red('[ANALYSE INPUT] Такого файла не существует!'))
        elif not os.path.splitext(args.input)[1] in ['.txt', '.xlsx']:
            raise parser.error(red('[ANALYSE INPUT] Расширение файла не ".txt" и не ".xlsx"!'))
        elif os.path.splitext(args.input)[1] == '.xlsx':
            if not args.columns:
                raise parser.error(red('[ANALYSE INPUT] Не указаны колонки, где хранятся профили (-c, --columns)!'))

        # Проверяем выходной файл
        if not args.output:
            args.output = os.path.split(args.input)[0]
        elif not os.path.isdir(args.output):
            raise parser.error(red('[ANALYSE OUTPUT] Такой папки не существует!'))

        original_file_name = os.path.splitext(os.path.split(args.input)[1])[0]

        # Разбираем входной файл
        if os.path.splitext(args.input)[1] == '.xlsx':
            user_ids, sheet_dict = xlsx_parser(args.input,
                                               [int(item) for item in args.columns.split(',')],
                                               True if args.titled else False)
        else:
            user_ids, sheet_dict = txt_parser(args.input)

        # Создаем папку для БД
        data_folder = DATA_DIR / original_file_name
        if not os.path.exists(data_folder):
            os.mkdir(data_folder)

        take_data(user_ids, data_folder)
        start_analyse(data_folder)
        create_output_file(data_folder, sheet_dict, args.output, original_file_name)

        if args.statistic:
            create_statistic_file(data_folder, sheet_dict, args.output, original_file_name)

        print(green('[INFO] Программа закончила работу'))

    elif args.command == 'proxy':
        proxy = ProxyManager(False if args.original_off else True)

        if args.proxy_command == 'new':
            if not args.login:
                new_proxy = [args.address, None]
            else:
                new_proxy = [args.address, [args.login, args.password]]
            proxys = proxy.proxy_append(new_proxy)
            if proxys:
                print(green(f'[PROXY NEW] Прокси {new_proxy} успешно добавлен!'))
                show_list('Новый список прокси:', proxys)
            else:
                print(red(f'[PROXY NEW] Прокси {new_proxy} уже есть в списке!'))
                show_list('Список прокси:', proxy.get_proxies())

        elif args.proxy_command == 'delete':
            deleted_proxy, proxys = proxy.proxy_remove(args.address)
            if deleted_proxy:
                print(green(f'[PROXY DELETE] Прокси {deleted_proxy} успешно удален!'))
                show_list('Новый список прокси:', proxys)
            else:
                print(red(f'[PROXY DELETE] Такого прокси нет в списке!'))
                show_list('Список прокси:', proxys)

        elif args.proxy_command == 'show':
            show_list('Список прокси:', proxy.get_proxies())

    elif args.command == 'token':
        token_manager = TokenManager()
        if args.token_command == 'new':
            tokens = token_manager.token_append(args.token)
            if tokens:
                print(green(f'[TOKEN NEW] Токен {args.token} успешно добавлен!'))
                show_list('Новый список токенов:', tokens)
            else:
                print(red(f'[TOKEN NEW] Токен {args.token} уже есть в списке!'))
                show_list('Список токенов:', token_manager.get_tokens())

        elif args.token_command == 'delete':
            deleted_token, tokens = token_manager.token_remove(args.token)
            if deleted_token:
                print(green(f'[TOKEN DELETE] Токен {deleted_token} успешно удален!'))
                show_list('Новый список токенов:', tokens)
            else:
                print(red(f'[TOKEN DELETE] Такого токена нет в списке!'))
                show_list('Список токенов:', tokens)

        elif args.token_command == 'show':
            show_list('Список токенов:', token_manager.get_tokens())


if __name__ == '__main__':
    main(sys.argv[1:])
