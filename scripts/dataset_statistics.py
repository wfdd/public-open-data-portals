
"""Collect dataset statistics from CKAN APIs."""

import asyncio
import csv
import datetime as dt
from functools import partial, reduce
import itertools as it
from os import environ
from pathlib import Path

import aiohttp
from logbook import StderrHandler as StderrLogger, error

CsvWriter = partial(csv.writer, lineterminator='\n')
CsvDictWriter = partial(csv.DictWriter, lineterminator='\n')


def prep_getter(session, sem_value=20):
    class Getter:
        _semaphore = asyncio.Semaphore(sem_value)

        def __init__(self, *args, **kwargs):
            self._timeout = kwargs.pop('timeout', 30)
            self._args = args
            self._kwargs = kwargs

        async def __aenter__(self):
            with aiohttp.Timeout(self._timeout):
                async with self._semaphore:
                    self._response = await session.get(*self._args, **self._kwargs)
                if self._response.status != 200:
                    await self.__aexit__()
                    raise ValueError('Received error code', self._response.status)
                return self._response

        async def __aexit__(self, *args):
            self._response.close()

    return Getter


def read_csv(filename, has_header=False):
    with open(filename) as file:
        if has_header:
            fields = next(csv.reader(file))
            return fields, tuple(csv.DictReader(file, fieldnames=fields))
        else:
            return tuple(csv.reader(file))


async def get_ckan_license_usage(license, api_endpoint, get):
    url = api_endpoint + '/action/package_search'
    try:
        async with get(url, params={'q': 'license_id:' + license, 'rows': '0'}) \
                as resp:
            count = int((await resp.json())['result']['count'])
    except Exception as e:
        error('{}: {!r}', url, e)
        raise
    return license, count


async def get_ckan_package_counts(country_code, api_endpoint, get):
    try:
        if country_code == 'US':
            # The 'license_list' action isn't available for the US
            url = api_endpoint.replace('/3', '/2') + '/rest/licenses'
            async with get(url) as resp:
                licenses = tuple(i['id'] for i in (await resp.json()))
        else:
            url = api_endpoint + '/action/license_list'
            async with get(url) as resp:
                licenses = tuple(i['id'] for i in (await resp.json())['result'])

        url = api_endpoint + '/action/package_search'
        async with get(url, params={'rows': '0'}) as resp:
            total = int((await resp.json())['result']['count'])
    except Exception as e:
        error('{}: {!r}', url, e)
        return
    try:
        per_license = await asyncio.gather(
            *(get_ckan_license_usage(i, api_endpoint, get) for i in licenses))
    except:
        return
    return country_code, total, per_license


async def get_cyprus_counts(get):
    def get(query, _get=get):
        return _get('https://api.morph.io/wfdd/data-gov-cy-scraper/data.json',
                    params={'key': environ['MORPH_API_KEY'], 'query': query})

    async with get('SELECT count(*) FROM data WHERE meta__last_updated = '
                   '(SELECT max(meta__last_updated) FROM data)') as resp:
        total, = await resp.json()
        total  = total['count(*)']
    async with get('''\
SELECT license, count(license) FROM data
WHERE meta__last_updated = (SELECT max(meta__last_updated) FROM data)
GROUP BY license''') as resp:
        per_license = tuple((i['license'], int(i['count(license)']))
                            for i in (await resp.json()))
    return 'CY', total, per_license


async def gather_country_stats(rows):
    ckan_apis = {r['country_code']: r['has_api'].partition(';')[0].partition(':')[-1]
                 for r in rows if '/api/3' in r['has_api']}
    with aiohttp.ClientSession(connector=aiohttp.TCPConnector(use_dns_cache=True)) \
            as session:
        getter = prep_getter(session)
        triplets = it.chain(
            (await asyncio.gather(*(get_ckan_package_counts(*i, getter)
                                    for i in ckan_apis.items()))),
            [(await get_cyprus_counts(getter))])
        return tuple(filter(None, triplets))


def consolidate_licenses(licenses_by_country):
    all_licenses = reduce(set.union,
                          ((n for n, c in v if c > 0)
                           for v in licenses_by_country.values()),
                          set())
    all_licenses = ((i, i.lower()) for i in sorted(all_licenses, key=str.lower))
    csv_path = str(Path(__file__).parent/'license_matches.csv')
    existing_keys = tuple(i for i, _ in read_csv(csv_path))
    with open(csv_path, 'a') as file:
        writer = CsvWriter(file)
        writer.writerows((a, b) for a, b in all_licenses if a not in existing_keys)
    input('Press any key to continue')  # Pause before reloading the dedupe CSV
    return dict(read_csv(csv_path))


def create_licenses_csv(licenses_by_country, today):
    all_licenses = consolidate_licenses(licenses_by_country)
    with open('licenses.csv', 'w') as file:
        writer = CsvDictWriter(file, ('country_code',
                                       *sorted(set(all_licenses.values())),
                                       'last_updated'))
        writer.writeheader()
        writer.writerows({'country_code': k, 'last_updated': today,
                          **{all_licenses[n]: c for n, c in v if c > 0}}
                         for k, v in sorted(licenses_by_country.items()))


def update_portals_csv(fields, rows, dataset_totals, today):
    with open('portals.csv', 'w') as file:
        writer = CsvDictWriter(file, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            if row['country_code'] in dataset_totals:
                row = {**row,
                       'total_datasets': dataset_totals[row['country_code']],
                       'last_updated': today}
            writer.writerow(row)


def main():
    fields, rows = read_csv('portals.csv', has_header=True)
    loop = asyncio.get_event_loop()
    country_stats = loop.run_until_complete(gather_country_stats(rows))

    today = dt.date.today().isoformat()
    create_licenses_csv({c: l for c, _, l in country_stats}, today)
    update_portals_csv(fields, rows, {c: t for c, t, _ in country_stats}, today)

if __name__ == '__main__':
    with StderrLogger():
        main()
