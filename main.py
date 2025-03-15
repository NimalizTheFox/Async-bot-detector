import asyncio
import random
import time
import os
from AsyncAPI import AIOInfoGrabber


def main():
    start_time = time.time_ns()

    with open('0738. 738.txt', 'r') as file:
        ids = file.read().strip().split('\n')

    if not os.path.exists(r'data\738'):
        os.mkdir(r'data\738')

    AIOInfoGrabber(ids, r'data\738')

    finish_time = time.time_ns()

    print('finish:', (finish_time - start_time)/1000000000, 's')



if __name__ == '__main__':
    # asyncio.run(main())
    main()