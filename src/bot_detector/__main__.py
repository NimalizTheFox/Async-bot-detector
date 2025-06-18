from .cli import main, sys
from .paths import ensure_directories


if __name__ == '__main__':
    ensure_directories()
    main(sys.argv[1:])
