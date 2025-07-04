from pathlib import Path
import sys


def get_base_path():
    """Определяет базовый путь в зависимости от режима выполнения"""
    if getattr(sys, 'frozen', False):
        # Режим .exe: используем временную папку распаковки
        return Path(sys._MEIPASS)
    else:
        # Режим разработки: корень проекта (3 уровня вверх)
        return Path(__file__).resolve().parent.parent.parent


# Автоматическое определение корня проекта
PROJECT_ROOT = get_base_path()

# Основные директории
DATA_DIR = PROJECT_ROOT / 'data'
MODELS_DIR = PROJECT_ROOT / 'models'

# Файлы
CONFIG_FILE = PROJECT_ROOT / 'settings.ini'
OPEN_MODEL = MODELS_DIR / 'open_model_state_dict.pt'
CLOSE_MODEL = MODELS_DIR / 'close_model_state_dict.pt'

_settings_original = """[VK]
access_token = []

[PROXY]
proxy = []"""


def ensure_directories():
    """Создает необходимые директории при первом запуске + проверка структуры"""
    DATA_DIR.mkdir(exist_ok=True, parents=True)

    if not OPEN_MODEL.exists():
        raise FileNotFoundError('Не найдена модель для открытых профилей!')
    if not CLOSE_MODEL.exists():
        raise FileNotFoundError('Не найдена модель для закрытых профилей!')

    if not CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'w', encoding='utf-8') as file:
            file.write(_settings_original)
