A catalogue of public national and supranational [open data portals](https://ec.europa.eu/digital-single-market/en/open-data-portals).

## Aims and motivation

Broadly, to keep a record of the worldwide deployment of national open-data
portals.  To see how these portals compare in their implementation and
capabilities.  To map the adoption of free (libre) technological solutions.
To identify innovation in the public sector.  To attempt to transfer innovation
across national boundaries.  To determine the degree of involvement of and the
influence corporate interests have had on open-data portals on a national and
global scale.

## Fields

Generally: enumerations should be interleaved with semicolons; and the leftmost
colon wtihin a cell or an enumeration is assumed to be the separator of a
key–value pair.

### portals.csv

A general overview of each portal.

#### `country_code`

The ISO 3166-1:2 country code.

#### `country_name`

The name of the country or territory in English.

#### `url`

The portal's canonical URL.

#### `software_platform`

The name of the (user-facing) software package the portal is based on, if any.

#### `is_open_source`

Whether the source code has been made publicly available under a free-software
license and in full.

#### `source_code_url`

Where to find the source code if it is made available, regardless of whether
it's open source.

#### `has_metadata_api`

Whether the portal provides a dataset metadata API.

#### `metadata_api_endpoint`

The metedata API endpoint, provided it `has_metadata_api`.

#### `has_object_api`

Whether the portal provides an API with direct access to the data.  This
corresponds to the CKAN `datastore` API, which is not enabled by default.

#### `object_api_endpoint`

The object API endpoint, provided it `has_object_api`.

#### `has_bulk_download`

Whether the portal provides a bulk download option for datasets.

#### `total_datasets`

The total number of datasets as of `last_updated`.

#### `presiding_body`

The name of the government agency that oversees the operation of the portal,
both in English and in the native language(s).  This can be the
competent authority as prescribed by law (e.g. a ministry) or a division of the
competent authority delegated with the operation of the portal, if granularity
is desired.

#### `outsourced_development`

Whether the government employed a private entity to develop the portal.  The
contractor will often also be responsible for maintenance.

#### `received_consulting`

Whether the government hired a private consultant to assist with the portal,
broadly construed.

#### `date_launched`

The date when the portal was opened to the public.

#### `resources`

Reading material.

#### `notes`

Anything you might like to make a note of.

#### `last_updated`

Date when the row was last updated.

### licenses.csv

Dataset breakdown by country and license.  The license counts can be mined from
CKAN APIs.

#### `country_code`

The ISO 3166-1:2 country code.

#### `last_updated`

Date when the row was last updated.

## License

[CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/).

## See also

### API docs

* [CKAN](http://docs.ckan.org/en/latest/api/index.html)
* [DKAN](http://docs.getdkan.com/dkan-documentation/dkan-api)
* [Socrata](https://dev.socrata.com/docs/endpoints.html)

### Other catalogues

* http://opengeocode.org/opendata/
* http://dataportals.org/search
* https://www.opendatasoft.com/a-comprehensive-list-of-all-open-data-portals-around-the-world/
* http://data.europa.eu/euodp/en/about
