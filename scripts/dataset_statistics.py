
"""Collect dataset statistics from CKAN APIs."""

import asyncio
import csv
import datetime as dt
from functools import partial, reduce, wraps
import inspect
from json.decoder import JSONDecodeError
from os import environ
from pathlib import Path
import traceback as tb
import typing

import aiohttp
from logbook import StderrHandler as StderrLogger, error as _error
import yaml

csv_writer = partial(csv.writer, lineterminator='\n')
csv_dict_writer = partial(csv.DictWriter, lineterminator='\n')


def prep_getter(session, sem_value=20):
    class Getter:
        _semaphore = asyncio.Semaphore(sem_value)

        def __init__(self, *args, **kwargs):
            self._timeout = kwargs.pop('timeout', 30)
            self._as_json = kwargs.pop('json', True)
            self._args = args
            self._kwargs = kwargs

        async def __aenter__(self):
            with aiohttp.Timeout(self._timeout):
                async with self._semaphore:
                    self._response = await session.get(*self._args, **self._kwargs)
                if self._response.status != 200:
                    await self.__aexit__()
                    raise ValueError('Received error code', self._response.status)
                if self._as_json:
                    try:
                        return await self._response.json()
                    except JSONDecodeError:
                        # For Taiwan
                        return await self._response.json(encoding='utf-8-sig')
                else:
                    return self._rsponse

        async def __aexit__(self, *args):
            self._response.close()

    return Getter


def error(api_endpoint, e):
    _error('{}: {!r}\n{}', api_endpoint, e,
           ''.join(tb.format_list(i for i in tb.extract_tb(e.__traceback__)
                                  if i.filename == __file__)).rstrip())


def rescue_api_call(return_value=None):
    def _prep_log_wrapper(ns, coro=False):
        exec("""\
{}def wrapper(*args, **kwargs):
    try:
        return {}fn(*args, **kwargs)
    except Exception as e:
        error(api_endpoint or args[api_endpoint_param], e)
        return _return_value
""".format(*(('async ', 'await ') if coro else ('',)*2)), ns)
        return ns['wrapper']

    def decorate(fn, _return_value=return_value):
        try:
            api_endpoint_param = next(
                i for i, v in enumerate(inspect.signature(fn).parameters)
                if v == 'api_endpoint')
        except StopIteration:
            api_endpoint = fn.__name__
        else:
            api_endpoint = None
        return wraps(fn)(_prep_log_wrapper({**globals(), **locals()},
                                           inspect.iscoroutinefunction(fn)))
    return decorate

def read_csv(filename, has_header=False):
    with open(filename) as file:
        if has_header:
            fields = next(csv.reader(file))
            return fields, tuple(csv.DictReader(file, fields))
        else:
            return tuple(csv.reader(file))


async def get_ckan_license_usage(license, api_endpoint, get) -> \
        typing.Tuple[str, int]:
    async with get(api_endpoint + '/action/package_search',
                   params={'q': 'license_id:' + license, 'rows': '0'}) \
            as json:
        return license, int(json['result']['count'])


@rescue_api_call(())
async def get_ckan_packages_per_license(licenses, api_endpoint, get) -> \
        typing.Union[list, tuple]:
    return await asyncio.gather(*(get_ckan_license_usage(i['id'], api_endpoint, get)
                                  for i in licenses))


@rescue_api_call()
async def get_ckan_package_counts(country_code, api_endpoint, get) -> \
        typing.Union[typing.Tuple[str, int, list, typing.Iterable], None]:
    if country_code == 'US':
        # The 'license_list' action isn't available for the US
        async with get(api_endpoint.replace('/3', '/2') + '/rest/licenses') \
                as json:
            licenses = json
    else:
        async with get(api_endpoint + '/action/license_list') as json:
            licenses = json['result']

    async with get(api_endpoint + '/action/package_search', params={'rows': '0'}) \
            as json:
        total_packages = int(json['result']['count'])
    return country_code, total_packages, licenses, \
           (await get_ckan_packages_per_license(licenses, api_endpoint, get))


@rescue_api_call()
async def get_cyprus_counts(get) -> typing.Union[tuple, None]:
    def get(query, _get=get):
        return _get('https://api.morph.io/wfdd/data-gov-cy-scraper/data.json',
                    params={'key': environ['MORPH_API_KEY'], 'query': query})

    async with get('SELECT count(*) FROM data WHERE meta__last_updated = '
                   '(SELECT max(meta__last_updated) FROM data)') as json:
        total_packages, = json
        total_packages  = total_packages['count(*)']
    async with get('''\
SELECT license, count(license) FROM data
WHERE meta__last_updated = (SELECT max(meta__last_updated) FROM data)
GROUP BY license''') as json:
        total_per_license = tuple((i['license'], int(i['count(license)']))
                                  for i in json)
    return 'CY', total_packages, tuple(l for l, _ in total_per_license), \
           total_per_license


def gather_country_stats(loop, rows):
    ckan_apis = {r['country_code']: r['metadata_api_endpoint'] for r in rows
                 if '/api/3' in r['metadata_api_endpoint']}
    with aiohttp.ClientSession(connector=
                aiohttp.TCPConnector(verify_ssl=False, use_dns_cache=True)) \
            as session:
        getter = prep_getter(session)
        quadruplets = asyncio.gather(*(get_ckan_package_counts(*i, getter)
                                       for i in ckan_apis.items()),
                                     get_cyprus_counts(getter))
        quadruplets = loop.run_until_complete(quadruplets)
        return tuple(filter(None, quadruplets))


def dedupe_licenses(licenses_by_country):
    all_licenses = reduce(set.union,
                          ((n for n, c in v if c > 0)
                           for *_, v in licenses_by_country),
                          set())
    all_licenses = ((i, i.lower()) for i in sorted(all_licenses, key=str.lower))
    csv_path = str(Path(__file__).parent/'data'/'license_mappings.csv')
    existing_keys = {k for k, _ in read_csv(csv_path)}
    with open(csv_path, 'a') as file:
        csv_writer(file).writerows((a, b) for a, b in all_licenses
                                   if a not in existing_keys)
    with open(str(Path(__file__).parent/'data'/'license_details.yaml'), 'w') \
            as file:
        yaml.safe_dump({c: l for c, l, _ in licenses_by_country}, file,
                       allow_unicode=True, default_flow_style=False)
    input('Press any key to continue')  # Pause before reloading the CSV
    return dict(read_csv(csv_path))


def create_licenses_csv(licenses_by_country):
    today = dt.date.today().isoformat()
    all_licenses = dedupe_licenses(licenses_by_country)
    with open('licenses.csv', 'w') as file:
        writer = csv_dict_writer(file, ('country_code',
                                        *sorted(set(all_licenses.values())),
                                        'last_updated'))
        writer.writeheader()
        writer.writerows({'country_code': k, 'last_updated': today,
                          **{all_licenses[n]: c for n, c in v if c > 0}}
                         for k, _, v in sorted(licenses_by_country))


def update_portals_csv(fields, rows, dataset_totals):
    today = dt.date.today().isoformat()
    with open('portals.csv', 'w') as file:
        writer = csv_dict_writer(file, fields)
        writer.writeheader()
        for row in rows:
            if row['country_code'] in dataset_totals:
                row = {**row,
                       'total_datasets': dataset_totals[row['country_code']],
                       'last_updated': today}
            writer.writerow(row)


def main():
    fields, rows = read_csv('portals.csv', has_header=True)
    country_stats = gather_country_stats(asyncio.get_event_loop(), rows)
    create_licenses_csv(tuple((c, d, l) for c, _, d, l in country_stats))
    update_portals_csv(fields, rows, {c: t for c, t, *_ in country_stats})

if __name__ == '__main__':
    with StderrLogger():
        main()
