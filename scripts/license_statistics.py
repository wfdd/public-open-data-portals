
"""Collects licensing statistics from CKAN APIs."""

import asyncio
import csv
import datetime as dt
import io

import aiohttp
from logbook import StderrHandler as StderrLogger, error


def extract_api_endpoints():
    with open('public-open-data-portals.csv') as file:
        fields = next(csv.reader(file))
    with open('public-open-data-portals.csv') as file:
        rows = tuple(csv.DictReader(file))
    return (fields,
            rows,
            tuple({**r, 'api_endpoint': r['has_api'].split(';')[0]
                                                    .partition(':')[-1]}
                  for r in rows if '/api/3' in r['has_api']))


async def get_license_usage(endpoint, license, s, p):
    try:
        with aiohttp.Timeout(30):
            async with p, s.get(endpoint + '/action/package_search',
                                params={'q': 'license_id:' + license}) as resp:
                count = (await resp.json())['result']['count']
    except Exception as e:
        error(endpoint + '/action/package_search: ' + repr(e))
        raise
    print(endpoint + '/action/package_search', license, count)
    return license, count


async def gather_country_statistics(data, s, p):
    try:
        with aiohttp.Timeout(30):
            async with p, s.get(data['api_endpoint'] + '/action/license_list') \
                    as resp:
                licenses = tuple(i['id'] for i in (await resp.json())['result'])
    except Exception as e:
        error(data['api_endpoint'] + '/action/license_list: ' + repr(e))
        counts = ()
    else:
        try:
            counts = await asyncio.gather(*(get_license_usage(data['api_endpoint'], i, s, p)
                                            for i in licenses))
        except:
            counts = ()
    return (data['country_code'], counts)


async def gather_countries(data, p):
    with aiohttp.ClientSession(connector=aiohttp.TCPConnector(use_dns_cache=True)) \
            as s:
        return await asyncio.gather(*(gather_country_statistics(i, s, p)
                                      for i in data))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    fields, rows, rows_with_ckan_apis = extract_api_endpoints()
    with StderrLogger():
        licenses = dict(loop.run_until_complete(gather_countries(
            rows_with_ckan_apis, asyncio.Semaphore(20))))
    new_rows = []
    today = dt.date.today().isoformat()
    for row in rows:
        if row['country_code'] in licenses:
            country_licenses = licenses[row['country_code']]
            country_licenses = ';'.join('{}[{}]'.format(n, c)
                                        for n, c in sorted(country_licenses,
                                                           key=lambda i: i[1],
                                                           reverse=True)
                                        if int(c) > 0)
            row = {**row,
                   'licenses_used': country_licenses, 'last_updated': today}
        new_rows.append(row)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fields)
    writer.writeheader()
    writer.writerows(new_rows)
    print(buffer.getvalue())
