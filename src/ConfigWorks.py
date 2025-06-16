import json
import configparser

config_path = r'..\settings.ini'


def write_token(tokens: list[str]) -> None:
    """Записывает список с токенами в конфиг"""
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')
    config.set('VK', 'access_token', json.dumps(tokens))
    with open('settings.ini', 'w', encoding='utf-8') as file:
        config.write(file)


def write_proxy(proxys: list[list[str, list | None]]) -> None:
    """Записывает список с прокси (и их логином/паролем) в конфиг"""
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')
    config.set('PROXY', 'proxy', json.dumps(proxys))
    with open('settings.ini', 'w', encoding='utf-8') as file:
        config.write(file)


def get_tokens() -> list[str]:
    """Достает из конфига все токены vk API, если их нет, то вызывает исключение"""
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')
    tokens = config.get('VK', 'access_token')

    if len(tokens) == 0:
        raise Exception('Необходимо указать как минимум один токен API в конфигурационном файле (settings.ini)!')

    return json.loads(tokens)


def get_proxys() -> list[list[str | None, list | None]]:
    """Достает из конфига все прокси и их данные аутентификации
    Если в файле конфигурации нет никаких прокси, то выдается None, то есть адрес машины"""
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')
    proxys = config.get('PROXY', 'proxy')

    # Если никаких прокси нет, то используем только эту машину
    if len(proxys) == 0:
        return [[None, None]]

    # Преобразуем строку в список
    proxys = json.loads(proxys)

    # И если нужно использовать адрес оригинальной машины, то добавляем его
    if int(config.get('PROXY', 'use_original_address')) == 1:
        proxys.append([None, None])

    return proxys
