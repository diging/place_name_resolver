import elasticsearch
import sys, getopt
import json
import re
import logging
from place_reader import PlaceReader

SILENT = True

class PlaceResolver:

    place_indicators = ["republic", "land", "state", "countr", "place", "cit", "park", \
                    "region", "continent", "district", "metro", "town", "captial", \
                    "village", "settlement", "university"]

    def __init__(self, host, index_name, port=9200, url_prefix='', auth_user=None, auth_pwd=None, timeout=3600, silent=True):
        log_setup(silent)
        self.host = host
        self.port = port
        self.index_name = index_name
        if auth_user:
          print("Using authentication for " + auth_user)
          self.es = elasticsearch.Elasticsearch([{'host': host, 'port': port, 'url_prefix': url_prefix}], timeout=timeout, retry_on_timeout=True, http_auth=auth_user+":"+auth_pwd, connection_class=elasticsearch.RequestsHttpConnection) if host else elasticsearch.Elasticsearch()
        else:
          self.es = elasticsearch.Elasticsearch([{'host': host, 'port': port, 'url_prefix': url_prefix}], timeout=timeout, retry_on_timeout=True) if host else elasticsearch.Elasticsearch()


    def find_in_title(self, title):
        results = self.es.search(index=self.index_name, body={"query": {"match": { "title": title}}})
        entries = results['hits']['hits']
        log("INFO: Searching for '{}:'".format(title), always=True)
        found_entries = []
        for entry in entries:
            entry_data = entry['_source']
            complete_text = entry_data['complete_text']
            if complete_text.strip().lower().startswith("#redirect"):
                redirect_title, redirect_entry = self.follow_redirect(entry_data)
                if redirect_entry:
                    log("INFO: '{}' redirects to '{}'".format(entry_data['title'].strip(), redirect_title))
                else:
                    log("WARN: '{}' is a redirect, but can't resolve redirect.")
                found_entries.append(redirect_entry)
            else:
                log("INFO: Found '{}'".format(entry_data['title'].strip()))
                found_entries.append(entry_data)

        return found_entries

    def follow_redirect(self, entry):
        redirect_pattern = r"#([rR][eE][Dd][Ii][Rr][Ee][Cc][Tt])\s*\[\[(.+?)\]\]"
        m = re.search(redirect_pattern, entry['complete_text'])
        if m:
            redirect_title = m.group(2)
            results = self.es.search(index=self.index_name, body={"query": {"match": { "title_keyword": redirect_title}}})
            # return first entry if something was found
            # we assume Elasticsearch returns the correct entry for the given title first
            if results and results['hits']['hits']:
                return redirect_title, results['hits']['hits'][0]['_source']
        return redirect_title, None

    def filter_place_entries(self, entries):
        place_indicators = ["republic", "land", "state", "countr", "place", "cit", "park", \
                            "region", "continent", "district", "metro", "town", "captial", \
                            "village", "settlement", "universit"]

        place_entries = []
        for entry in entries:
            if not entry:
                continue
            if 'categories' in entry.keys():
                place_catgories = list(filter(lambda c: any(pi in c.lower() for pi in place_indicators), entry['categories']))
            else:
                place_catgories = []
            if place_catgories and 'coordinates' in entry.keys() and entry['coordinates']:
                place_entries.append(entry)

        return place_entries

    def resolve_place(self, place_name):
        entries = self.find_in_title(place_name)
        potential_place_entries = self.filter_place_entries(entries)

        log("INFO: Found {} potential place entries.".format(len(potential_place_entries)))
        for entry in potential_place_entries:
            log("INFO: {} has coordinates {}.".format(entry['title'], entry['coordinates']))
        result = {'place_name': place_name}
        if potential_place_entries:
            result.update({
                'wikipedia_entry_title': potential_place_entries[0]['title'],
                'coodinates': potential_place_entries[0]['coordinates'],
                'wikipedia_entry_url': "https://en.wikipedia.org/wiki/" + potential_place_entries[0]['title']
            })
            log("INFO: Selecting: '{}' at {}".format(potential_place_entries[0]['title'], potential_place_entries[0]['coordinates']), always=True)

        return result

def log(msg, always=False):
    if not SILENT or always:
        print(msg)

def log_setup(silent):
    SILENT = silent
    if not SILENT:
        logging.basicConfig(filename='place_resolver.log', filemode='w', level=logging.INFO)


### execution setup
### main
def main():
    # required arguments
    INPUT_FILE       = None
    ES_INDEX_NAME    = None
    ES_HOST          = 'localhost'

    # optional arguments
    ES_PORT = 9200
    ES_URL_PREFIX = None
    ES_AUTH_USER = None
    ES_AUTH_PASSWORD = None
    ES_TIMEOUT = 3600

    # the following arguments are for CSV files only
    ID_COLUMN_NAME = None
    PLACE_COLUMN_NAME = None

    if len(sys.argv) < 3:
        print('Usage:\n\t resolve-institutions.py <inputfile> <index-name> <es-host> \n')
        print('Optional arguments:\n\t <port> <url-prefix> <es-user> <es-password> <timeout>\n')

    argument_list = sys.argv[4:]
    options = "p:f:u:s:t:"
    long_options = ["port=", "url-prefix=", "es-user=", "es-password=", "timeout=", "id-column=", "place-column=", "verbose"]

    try:
        # Parsing argument
        arguments, values = getopt.getopt(argument_list, options, long_options)
        for current_argument, current_value in arguments:
            if current_argument in ("-p", "--port"):
                ES_PORT = current_value
            if current_argument in ("-f", "--url-prefix"):
                ES_URL_PREFIX = current_value
            if current_argument in ("-u", "--es-user"):
                ES_AUTH_USER = current_value
            if current_argument in ("-s", "--es-password"):
                ES_AUTH_PASSWORD = current_value
            if current_argument in ("-t", "--timeout"):
                ES_TIMEOUT = current_value
            if current_argument in ("--id-column"):
                ID_COLUMN_NAME = current_value
            if current_argument in ("--place-column"):
                PLACE_COLUMN_NAME = current_value
            if current_argument in ("--verbose"):
                log_setup(FALSE)
    except getopt.error as err:
        print (str(err))
        sys.exit(2)

    INPUT_FILE       = sys.argv[1]
    ES_INDEX_NAME    = sys.argv[2]
    ES_HOST          = sys.argv[3]

    resolver = PlaceResolver(ES_HOST, ES_INDEX_NAME, port=ES_PORT, url_prefix=ES_URL_PREFIX, auth_user=ES_AUTH_USER, auth_pwd=ES_AUTH_PASSWORD)

    extra_parameters = {}
    if ID_COLUMN_NAME:
        extra_parameters.update({
            'id_column_name': ID_COLUMN_NAME
        })
    if PLACE_COLUMN_NAME:
        extra_parameters.update({
            'place_column_name': PLACE_COLUMN_NAME
        })

    place_reader = PlaceReader(INPUT_FILE, **extra_parameters)
    for place in place_reader.read_places():
        result = resolver.resolve_place(place["place_name"])
        if "id" in place:
            result.update({
                "id": place["id"]
            })
        print(result)

if __name__ == "__main__":
    main()