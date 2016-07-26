
"""Collects licensing statistics from CKAN APIs."""

import asyncio
import csv
import datetime as dt
import functools as ft
import io

import aiohttp
from logbook import StderrHandler as StderrLogger, error


def read_csv(filename):
    with open(filename) as file:
        fields = next(csv.reader(file))
        return fields, tuple(csv.DictReader(file, fieldnames=fields))


async def get_license_usage(api_endpoint, license, s, p):
    resource_url = api_endpoint + '/action/package_search'
    try:
        with aiohttp.Timeout(30):
            async with p, s.get(resource_url,
                                params={'q': 'license_id:' + license}) as resp:
                count = (await resp.json())['result']['count']
    except Exception as e:
        error('{}: {!r}', resource_url, e)
        raise
    print(resource_url, license, count)
    return license, count


async def gather_country_statistics(country_code, api_endpoint, s, p):
    try:
        with aiohttp.Timeout(30):
            if country_code == 'US':
                # The 'license_list' action isn't available for the US
                resource_url = api_endpoint.replace('3', '2') + '/rest/licenses'
                async with p, s.get(resource_url) as resp:
                    if resp.status != 200:
                        raise ValueError('Received error code', resp.status)
                    licenses = tuple(i['id'] for i in (await resp.json()))
            else:
                resource_url = api_endpoint + '/action/license_list'
                async with p, s.get(resource_url) as resp:
                    if resp.status != 200:
                        raise ValueError('Received error code', resp.status)
                    licenses = tuple(i['id'] for i in (await resp.json())['result'])
    except Exception as e:
        error('{}: {!r}', resource_url, e)
        counts = ()
    else:
        try:
            counts = await asyncio.gather(*(get_license_usage(api_endpoint, i, s, p)
                                            for i in licenses))
        except:
            counts = ()
    return country_code, counts


async def gather_countries(data, p):
    with aiohttp.ClientSession(connector=aiohttp.TCPConnector(use_dns_cache=True)) \
            as s:
        return dict(await asyncio.gather(*(gather_country_statistics(*i, s, p)
                                           for i in data.items())))


def main():
    loop = asyncio.get_event_loop()
    fields, rows = read_csv('portals.csv')
    today = dt.date.today().isoformat()

    ckan_apis = {r['country_code']: r['has_api'].split(';')[0].partition(':')[-1]
                 for r in rows if '/api/3' in r['has_api']}
    licenses = loop.run_until_complete(gather_countries(ckan_apis,
                                                        asyncio.Semaphore(20)))
    # new_rows = []
    # for row in rows:
    #     if row['country_code'] in licenses:
    #         country_licenses = licenses[row['country_code']]
    #         country_licenses = ';'.join('{}[{}]'.format(n, c)
    #                                     for n, c in sorted(country_licenses,
    #                                                        key=lambda i: i[1],
    #                                                        reverse=True)
    #                                     if int(c) > 0)
    #         row = {**row, 'licenses_used': country_licenses, 'last_updated': today}
    #     new_rows.append(row)
    # buffer = io.StringIO()
    # writer = csv.DictWriter(buffer, fields)
    # writer.writeheader()
    # writer.writerows(new_rows)
    # print(buffer.getvalue())

    all_licenses = sorted(ft.reduce(set.union,
                                    ((n.lower() for n, c in v if int(c) > 0)
                                     for v in licenses.values()),
                                    set()))
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, ('country_code', *all_licenses, 'last_updated'))
    writer.writeheader()
    writer.writerows({'country_code': k, 'last_updated': today,
                      **{n.lower(): c for n, c in v if int(c) > 0}}
                     for k, v in sorted(licenses.items()))
    print(buffer.getvalue())


if __name__ == '__main__':
    with StderrLogger():
        main()
