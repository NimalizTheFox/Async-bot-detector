import time
import os

from .data_collector import take_data
from .data_analysis import start_analyse
from .paths import DATA_DIR, PROJECT_ROOT


def main():
    start_time = time.time_ns()

    with open(PROJECT_ROOT / '0738. 738.txt', 'r') as file:
        ids = file.read().strip().split('\n')

    data_folder = DATA_DIR / '738'

    if not os.path.exists(data_folder):
        os.mkdir(data_folder)

    take_data(ids, data_folder)
    start_analyse(data_folder)

    finish_time = time.time_ns()

    print('finish:', (finish_time - start_time)/1000000000, 's')


if __name__ == '__main__':
    main()
