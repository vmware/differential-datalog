#!/usr/bin/python

import csv
from sys import (
    argv,
    exit,
)


def main(args):
    """Generate a transaction trace for dbpedia_ownership from given csv data"""
    # The csv file that contains the data from which to generate transactions.
    file = str(args[0])
    # Maximum number of insert operations to create
    limit = int(args[1])
    # Amount of insert statements that each transaction should contain.
    size = int(args[2])

    with open(file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == limit:
                break

            if line_count % size == 0:
                print('start;')

            print(f'insert Subsidiaries("{row[0]}", "{row[1]}");')

            if line_count % size == (size - 1):
                print('commit;')

            line_count += 1

        if line_count % size != 0:
            print('commit;')

    print('exit;')
    return 0


if __name__ == "__main__":
    exit(main(argv[1:]))
