
from itertools import chain
import sys
from urllib.parse import urlparse

from utils import csv_dict_writer, read_csv


def format_name(url):
    return urlparse(url).netloc.replace('www.', '').replace('.', '-')


eu_country_codes = (
    'AT', 'BE', 'HR', 'BG', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR',
    'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK',
    'SI', 'ES', 'SE', 'GB')

def format_tags(row):
    yield row['country_name'].replace(' ', '').lower()
    yield 'level.global' if row['country_code'] == 'EU' else 'level.national'
    if row['country_code'] in eu_country_codes:
        yield 'eu-official'

    software_platform, _, _ = row['software_platform'].partition(';')
    if any(s == software_platform for s in ('CKAN', 'DKAN', 'udata')):
        yield software_platform.lower()


def format_publisher(presiding_body):
    return next((p for p in presiding_body.split(';') if p.startswith('en:')),
                ''
                ).replace('en:', '')


def main(file_in, file_out):
    own_rows = [r for r in read_csv('portals.csv', has_header=True)[1]
                if r['url'] and r['url'] != 'N/A']

    fields, rows = read_csv(file_in, has_header=True)
    rows = sorted(chain(rows,
                        ({'name': format_name(r['url']),
                          'title': r['title'],
                          'url': r['url'],
                          'publisher': format_publisher(r['presiding_body']),
                          'publisher_classification': 'Government',
                          'tags': ' '.join(format_tags(r)),
                          'country': r['country_code'],
                          'generator': r['software_platform'],
                          'api_endpoint': r['metadata_api_endpoint']}
                         for r in own_rows)), key=lambda r: r['name'])
    with open(file_out, 'w') as file:
        writer = csv_dict_writer(file, fields)
        writer.writeheader()
        writer.writerows(rows)

if __name__ == '__main__':
    main(*sys.argv[1:])
