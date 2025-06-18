import os
import sys
import argparse

from src.ConfigWorks import proxy_append, token_append, proxy_remove, token_remove, get_proxies, get_tokens


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
    parser_analyse.add_argument('-s', '--sorted', action='store_true',
                                help='Сортирует выходной файл в соответствии с входным файлом')
    parser_analyse.add_argument('-t', '--statistic', action='store_true',
                                help='Помещает в папку с выходным файлом .txt файл со статистикой по ботам')

    # === Команды для управления прокси ===
    parser_proxy = subparsers.add_parser('proxy', help='Управление прокси')
    proxy_subparser = parser_proxy.add_subparsers(dest='proxy_command', help='Действия с прокси')

    # Добавить прокси в список
    new_proxy = proxy_subparser.add_parser('new', help='Добавить прокси в список')
    new_proxy.add_argument('-a', '--address', required=True, help='Адрес прокси')
    new_proxy.add_argument('-l', '--login', help='Логин для авторизации прокси (если есть)')
    new_proxy.add_argument('-p', '--password', help='Пароль для авторизации прокси (если есть)')

    # Удалить прокси из списка
    del_proxy = proxy_subparser.add_parser('delete', help='Удалить прокси из списка')
    del_proxy.add_argument('-a', '--address', required=True, help='Адрес прокси, который нужно удалить')

    # Вывести список
    proxy_subparser.add_parser('show', help='Показать все доступные прокси')

    # === Команды для управления токенами API ===
    parser_token = subparsers.add_parser('token', help='Управление токенами VK API')
    token_subparser = parser_token.add_subparsers(dest='token_command', help='Действия с токенами VK API')

    # Добавить токен
    new_token = token_subparser.add_parser('new', help='Добавить токен VK API в список')
    new_token.add_argument('-t', '--token', required=True, help='Сам токен VK API')

    # Добавить токен
    del_token = token_subparser.add_parser('delete', help='Удалить токен VK API из списка')
    del_token.add_argument('-t', '--token', required=True, help='Сам токен VK API')

    # Вывести список токенов API
    token_subparser.add_parser('show', help='Вывести список со всеми токенами VK API')

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
            raise parser.error(red("[ANALYSE INPUT] Расширение файла не '.txt' или '.xlsx'!"))

        # Проверяем выходной файл
        if not args.output:
            args.output = os.path.split(args.input)[0]
        elif not os.path.isdir(args.output):
            raise parser.error(red('[ANALYSE OUTPUT] Такой папки не существует!'))

        # TODO Действие (нужно отыскать где id и вывести сколько их всего)
        pass

    elif args.command == 'proxy':
        if args.proxy_command == 'new':
            if not args.login:
                new_proxy = [args.address, None]
            else:
                new_proxy = [args.address, [args.login, args.password]]
            proxys = proxy_append(new_proxy)
            if proxys:
                print(green(f'[PROXY NEW] Прокси {new_proxy} успешно добавлен!'))
                show_list('Новый список прокси:', proxys)
            else:
                print(red(f'[PROXY NEW] Прокси {new_proxy} уже есть в списке!'))
                show_list('Список прокси:', get_proxies())

        elif args.proxy_command == 'delete':
            deleted_proxy, proxys = proxy_remove(args.address)
            if deleted_proxy:
                print(green(f'[PROXY DELETE] Прокси {deleted_proxy} успешно удален!'))
                show_list('Новый список прокси:', proxys)
            else:
                print(red(f'[PROXY DELETE] Такого прокси нет в списке!'))
                show_list('Список прокси:', proxys)

        elif args.proxy_command == 'show':
            show_list('Список прокси:', get_proxies())

    elif args.command == 'token':
        if args.token_command == 'new':
            tokens = token_append(args.token)
            if tokens:
                print(green(f'[TOKEN NEW] Токен {args.token} успешно добавлен!'))
                show_list('Новый список токенов:', tokens)
            else:
                print(red(f'[TOKEN NEW] Токен {args.token} уже есть в списке!'))
                show_list('Список токенов:', get_tokens())

        elif args.token_command == 'delete':
            deleted_token, tokens = token_remove(args.token)
            if deleted_token:
                print(green(f'[TOKEN DELETE] Токен {deleted_token} успешно удален!'))
                show_list('Новый список токенов:', tokens)
            else:
                print(red(f'[TOKEN DELETE] Такого токена нет в списке!'))
                show_list('Список токенов:', tokens)

        elif args.token_command == 'show':
            show_list('Список токенов:', get_tokens())


if __name__ == '__main__':
    main(sys.argv[1:])
