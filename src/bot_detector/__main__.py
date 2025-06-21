from multiprocessing import freeze_support

from src.bot_detector.cli import main, sys
from src.bot_detector.paths import ensure_directories


if __name__ == '__main__':
    freeze_support()
    ensure_directories()
    main(sys.argv[1:])
