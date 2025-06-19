import json
import configparser

from .paths import CONFIG_FILE


class TokenManager:
    def __init__(self):
        """Управление токенами API"""
        self.config = configparser.ConfigParser()
        self.config.read(CONFIG_FILE, encoding='utf-8')

    def _write_tokens(self, tokens: list[str]) -> None:
        """Записывает список с токенами в конфиг"""
        tokens = list(set(tokens))  # Убираем дубликаты
        self.config.set('VK', 'access_token', json.dumps(tokens))
        with open(CONFIG_FILE, 'w', encoding='utf-8') as file:
            self.config.write(file)

    def get_tokens(self) -> list[str]:
        """Достает из конфига все токены vk API"""
        return json.loads(self.config.get('VK', 'access_token'))

    def token_append(self, new_token: str) -> list[str] | None:
        """Добавить токен API в конфиг
        :return: Список токенов"""
        tokens = self.get_tokens()

        if new_token in tokens:
            return None

        tokens.append(new_token)
        self._write_tokens(tokens)
        return tokens

    def token_remove(self, token_to_remove: str) -> tuple[str | None, list[str]]:
        """Удаление токена API из конфига
        :return: Удаленный токен, Список токенов"""
        tokens = self.get_tokens()
        if token_to_remove not in tokens:
            # Если токена нет в списке, то возвращаем None
            return None, tokens
        else:
            # Если есть, то удаляем и возвращаем его самого + обновленный список
            tokens.remove(token_to_remove)
            self._write_tokens(tokens)
            return token_to_remove, tokens


class ProxyManager:
    def __init__(self, need_original_address: bool = True):
        """Управление прокси"""
        self.need_original = need_original_address

        self.config = configparser.ConfigParser()
        self.config.read(CONFIG_FILE, encoding='utf-8')

    def _write_proxy(self, proxies: list[list[str | None, list | None]]) -> None:
        """Записывает список с прокси (и их логином/паролем) в конфиг"""
        # Избавляемся от дубликатов и адреса оригинальной машины
        uniques_proxies = []
        for proxy in proxies:
            if proxy not in uniques_proxies and proxy != [None, None]:
                uniques_proxies.append(proxy)
        proxies = uniques_proxies

        self.config.set('PROXY', 'proxy', json.dumps(proxies))
        with open(CONFIG_FILE, 'w', encoding='utf-8') as file:
            self.config.write(file)

    def get_proxies(self) -> list[list[str | None, list[str, str] | None]]:
        """Достает из конфига все прокси и их данные аутентификации
        Если в файле конфигурации нет никаких прокси, то выдается None, то есть адрес машины"""
        proxies = self.config.get('PROXY', 'proxy')

        # Преобразуем строку в список
        proxies = json.loads(proxies)

        # Если никаких прокси нет, то используем только эту машину
        if len(proxies) == 0:
            return [[None, None]]

        # И если нужно использовать адрес оригинальной машины, то добавляем его
        if self.need_original:
            proxies.append([None, None])
        return proxies

    def proxy_append(self, new_proxy: list[str, list[str, str] | None]) -> list[list[str, list[str, str] | None]] | None:
        """Добавить прокси в конфиг
        :return: Список прокси"""
        proxies = self.get_proxies()

        # Если есть точно такой же прокси - вернуть None
        if new_proxy in proxies:
            return None

        proxies.append(new_proxy)
        self._write_proxy(proxies)
        return proxies

    def proxy_remove(self, proxy_address: str) -> tuple[list[str, list[str, str] | None] | None, list]:
        """Удалить прокси из конфига
        :return: Удаленный прокси, Список прокси"""
        deleted_proxy = None
        proxies = self.get_proxies()

        # Поиск нужного прокси по адресу
        for proxy in proxies:
            if proxy[0] == proxy_address:
                deleted_proxy = proxy
                break

        # Если прокси с таким адресом есть, то удаляем его
        if deleted_proxy:
            proxies.remove(deleted_proxy)
            self._write_proxy(proxies)
        return deleted_proxy, proxies
