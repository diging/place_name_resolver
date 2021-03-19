# Place name resolver

This is a Python script to retrieve coordinates for place names such as university names, city names, etc. The script will search Wikipedia articles for matching entries for a place name and if a matching entry has coordinates, those coordinates will be returned. I requires an Elasticsearch instance that has all Wikipedia articles indexed (see [this script](https://github.com/diging/cord19-geo-explorer-utils/tree/732dfde5310a04d188bdec14ae0d7a2f659b1d51/wikipedia)).

## Requirements
- Python 3.7
- Dependencies listed in `requirements.txt` (e.g. installed through pip)

## Usage

There are two ways the place name resolver can be used, either as standalone script or as a library.

### Standalone script

To use the place name resolver as a standalone script, call it as follows:

```
python place_resolver.py <input-file> <index-name>
```

in which:
- `input-file` is the path to a plain text file that has one place name per line or a csv file. If a csv file is passed the parameter `place-column` has to be configured (see below). See the included example files (`test.csv`, `test.txt`).
- `index-name` is the name of the Elasticsearch index to be used

There are a few more configuration options that can be passed in when calling the script:
```
python place_resolver.py <input-file> <index-name> <elastic-host> -p <elastic-port> --url-prefix <elastic-prefix> -u <username> -s <password> -o <output-file> -t <timeout> --id-column <id-column> --place-column <place-column> --verbose
```

- `elastic-host`: the Elasticsearch host; if not provided, `localhost` will be used by default. Needs to be passed as third parameter.
- `elastic-port`: the port of your Elasticsearch; if not provided, `9200` will be used by default. Needs to be passed using `-p` or `--port`.
- `elastic-prefix`: if your Elasticsearch is not running at root, this would be the path to your instance. Needs to be passed using `-f` or `--url-prefix`.
- `username`: if your Elasticsearch is secured through basic auth, this parameter lets you specify the username. Needs to be passed using `-u` or `--es-user`.
- `password`: if your Elasticsearch is secured through basic auth, this parameter lets you specify the password. Needs to be passed using `-s` or `--es-password`.
- `output-file`: the output file; defaults to `results.csv`.
- `timeout`: timeout to wait for Elasticsearch in seconds; defaults to 3600. Needs to be passed using `-t` or `--timeout`.
- `id-column`: if a csv file is passed as input and there is an ID column that should be included in the results file, then set this parameter to the title of the id column. Needs to be passed using `--id-column`.
- `place-column`: if a csv file is passed as input, then this parameter must be set to the title of the column that contains the place name. Needs to be passed using `--place-column`.
- `verbose`: enables logging. A log file `place_resolver.log` will be created. Can be set using `--verbose`.

The script will create a csv file with the following columns:
- Original Place Name: the place name that was searched
- Found Place Name: the place that was found
- Coords: the found coordinates
- Wikipedia: link to the matched Wikipedia article
- Original Id: if the input file was a csv with an id column specified, the id will be added in this column.

### Programmatic usage

The place name resolver can be used as follows directly from your Python code:

```
from place_resolver import *

resolver = PlaceResolver("elastic.host.org", "index-name")
resolver.resolve_place("Arizona State University")
```

The following optional parameter can be passed when creating a `PlaceResolver` object:
- `port`: the port of your Elasticsearch; defaults to `9200`
- `url_prefix`: if your Elasticsearch is not running at root, this would be the path to your instance; defaults to `''`
- `auth_user`: if your Elasticsearch is secured through basic auth, this parameter lets you specify the username; defaults to `None
- `auth_pwd`: if your Elasticsearch is secured through basic auth, this parameter lets you specify the password; defaults to `None`
- `timeout`: timeout to wait for Elasticsearch in seconds; defaults to `3600`
- `silent`: enables logging; if set to `False`, a log file place_resolver.log will be created; defaults to `True`

`resolve_place` returns a dictionary with of the following format:
```
{
   'place_name': 'Arizona State University', 
   'wikipedia_entry_title': 'Arizona State University', 
   'coodinates': [33.421, -111.933], 
   'wikipedia_entry_url': 'https://en.wikipedia.org/wiki/Arizona State University'
}
```

