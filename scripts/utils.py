
import csv
from functools import partial

csv_writer = partial(csv.writer, lineterminator='\n')
csv_dict_writer = partial(csv.DictWriter, lineterminator='\n')


def read_csv(filename, has_header=False):
    with open(filename) as file:
        if has_header:
            fields = next(csv.reader(file))
            return fields, tuple(csv.DictReader(file, fields))
        else:
            return tuple(csv.reader(file))
