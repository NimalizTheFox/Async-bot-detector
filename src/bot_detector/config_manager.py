import json
import configparser
from .paths import CONFIG_FILE


def write_tokens(tokens: list[str]) -> None:
    """Записывает список с токенами в конфиг"""
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE, encoding='utf-8')

    tokens = list(set(tokens))

    config.set('VK', 'access_token', json.dumps(tokens))
    with open(CONFIG_FILE, 'w', encoding='utf-8') as file:
        config.write(file)


def write_proxy(proxies: list[list[str | None, list | None]]) -> None:
    """Записывает список с прокси (и их логином/паролем) в конфиг"""
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE, encoding='utf-8')

    # Избавляемся от дубликатов и адреса оригинальной машины
    uniques_proxies = []
    for proxy in proxies:
        if proxy not in uniques_proxies and proxy != [None, None]:
            uniques_proxies.append(proxy)
    proxies = uniques_proxies

    config.set('PROXY', 'proxy', json.dumps(proxies))
    with open(CONFIG_FILE, 'w', encoding='utf-8') as file:
        config.write(file)


def get_tokens() -> list[str]:
    """Достает из конфига все токены vk API, если их нет, то вызывает исключение"""
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE, encoding='utf-8')
    tokens = config.get('VK', 'access_token')
    return json.loads(tokens)


def get_proxies() -> list[list[str | None, list[str, str] | None]]:
    """Достает из конфига все прокси и их данные аутентификации
    Если в файле конфигурации нет никаких прокси, то выдается None, то есть адрес машины"""
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE, encoding='utf-8')
    proxies = config.get('PROXY', 'proxy')

    # Если никаких прокси нет, то используем только эту машину
    if len(proxies) == 0:
        return [[None, None]]

    # Преобразуем строку в список
    proxies = json.loads(proxies)

    # И если нужно использовать адрес оригинальной машины, то добавляем его
    if int(config.get('PROXY', 'use_original_address')) == 1:
        proxies.append([None, None])

    return proxies


# === Управление прокси ===
def proxy_append(new_proxy: list[str, list[str, str] | None]) -> list[list[str, list[str, str] | None]] | None:
    """Добавить прокси в конфиг
    :return: Список прокси"""
    proxies = get_proxies()

    if new_proxy in proxies:
        return None

    proxies.append(new_proxy)
    write_proxy(proxies)
    return proxies


def proxy_remove(proxy_address: str) -> tuple[list[str, list[str, str] | None] | None, list]:
    """Удалить прокси из конфига
    :return: Удаленный прокси, Список прокси"""
    deleted_proxy = None
    proxies = get_proxies()

    for proxy in proxies:
        if proxy[0] == proxy_address:
            deleted_proxy = proxy
            break

    if deleted_proxy:
        proxies.remove(deleted_proxy)
        write_proxy(proxies)
    return deleted_proxy, proxies


# === Управление токенами ===
def token_append(new_token: str) -> list[str] | None:
    """Добавить токен API в конфиг
    :return: Список токенов"""
    tokens = get_tokens()

    if new_token in tokens:
        return None

    tokens.append(new_token)
    write_tokens(tokens)
    return tokens


def token_remove(token_to_remove: str) -> tuple[str | None, list[str]]:
    """Удаление токена API из конфига
    :return: Удаленный токен, Список токенов"""
    deleted_token = None
    tokens = get_tokens()

    for token in tokens:
        if token == token_to_remove:
            deleted_token = token_to_remove
            break

    if deleted_token:
        tokens.remove(deleted_token)
        write_tokens(tokens)
    return deleted_token, tokens
