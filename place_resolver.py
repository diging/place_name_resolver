import elasticsearch
import sys, getopt
import json
import re
import logging
import math
import csv
import textdistance
from place_reader import PlaceReader

SILENT = True

class PlaceResolver:

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
            # first we check if this could be a fit or if the article is
            # too far off when comparing the strings
            # similarity = textdistance.hamming.normalized_similarity(title, entry_data['title'])
            similarity = textdistance.jaro_winkler(title, entry_data['title'])
            # we assume that anything under 0.4 is just wrong because too different
            if similarity < 0.4:
                log("WARN: Skipping {} because not similar enough to {} (score {}).".format(entry_data['title'], title, similarity))
                continue
            if complete_text.strip().lower().startswith("#redirect"):
                redirect_title, redirect_entry = self.follow_redirect(entry_data)
                if redirect_entry:
                    log("INFO: '{}' redirects to '{}'".format(entry_data['title'].strip(), redirect_title))
                else:
                    log("WARN: '{}' is a redirect, but can't resolve redirect.".format(entry_data['title'].strip()))
                found_entries.append(redirect_entry)
            else:
                log("INFO: Found '{}'".format(entry_data['title'].strip()))
                found_entries.append(entry_data)

        return found_entries

    def follow_redirect(self, entry):
        redirect_pattern = r"#([rR][eE][Dd][Ii][Rr][Ee][Cc][Tt])\s*\[\[(.+?)\]\]"
        m = re.search(redirect_pattern, entry['complete_text'])
        redirect_title = ''
        if m:
            redirect_title = m.group(2)
            results = self.es.search(index=self.index_name, body={"query": {"match": { "title_keyword": redirect_title}}})
            # return first entry if something was found
            # we assume Elasticsearch returns the correct entry for the given title first
            if results and results['hits']['hits']:
                return redirect_title, results['hits']['hits'][0]['_source']
        else:
            log("WARN: '{}' is a redirect, but can't find redirection.".format(entry['complete_text']))
        return redirect_title, None

    def filter_place_entries(self, entries):
        place_indicators = ["republic", "land", "state", "countr", "place", "cit", "park", \
                        "region", "continent", "district", "metro", "town", "captial", \
                        "village", "settlement", "universit", "organization", "institut", \
                        "academy", "hospital"]

        place_entries = []
        for entry in entries:
            if not entry:
                continue
            if 'categories' in entry.keys():
                print(entry['categories'])
                place_catgories = list(filter(lambda c: any(pi in c.lower() for pi in place_indicators), entry['categories']))
            else:
                place_catgories = []

            # sometimes the coords were not correctly indexed, so we'll look for them again
            if not 'coordinates' in entry.keys() or not entry['coordinates']:
                coord_pattern = r"\{\{ *([Cc][Oo][Oo][Rr][Dd]\|.+?)\}\}"
                m = re.search(coord_pattern, entry['complete_text'])
                if m:
                    coords = m.group(1)
                    entry['coordinates'] = coords
            if place_catgories and 'coordinates' in entry.keys() and entry['coordinates']:
                place_entries.append(entry)
            else:
                log("INFO: skipping {} because no coordinates or no matching categories.".format(entry['title']))

        return place_entries

    def process_length_3_coords(self, coords):
        decimal = coords[0]
        minutes = coords[1]
        seconds = coords[2]
        decimal_degrees = 0;
        if decimal == 0:
            decimal_degrees = ((minutes / 60.0) + (seconds / 3600.0));
        else:
            # python doesn't have a clean way to get the symbol of a number so we use copysign
            decimal_degrees = math.copysign(1,decimal) * abs(decimal) + (minutes / 60.0) + (seconds / 3600.0)
        return round(decimal_degrees, 6)

    def process_length_2_coords(self, coords):
        decimal = coords[0]
        minutes = coords[1]
        return round(math.copysign(1,decimal) * abs(decimal) + (minutes / 60.0), 6)



    def parse_string(self, string, coords):
        try:
            # check if string is a int/float and cast to float
            coords.append(float(string))
        except:
            pass
        # Check if a string needs to be converted to decimal degrees by looking for ordinal directions
        # The length of coordinates is used to determine what data is present
        # Example String Coord|40|00|00|N|116|19|36|E|region:CN-11_type:edu|display=inline,title
        if string in ["N", "E", "n", "e"]:
            if len(coords) == 3:
                processed_coords =  self.process_length_3_coords(coords)
            elif len(coords) == 2:
                processed_coords =  self.process_length_2_coords(coords)
            elif len(coords) == 1:
                processed_coords = coords[0]
            coords.clear()
            return processed_coords
        elif string in ["S", "W", "s", "w"]:
            # handle negative coords
            if len(coords) == 3:
                return (self.process_length_3_coords(coords) * -1)
            elif len(coords) == 2:
                return (self.process_length_2_coords(coords) * -1)
            elif len(coords) == 1:
                return (coords[0] * -1)

    def clean_coordinates(self, coord):
        split_string =  coord.split("|")
        coords = []
        formatted_coords = []
        for string in split_string:
            parsed_string = self.parse_string(string, coords)
            if parsed_string:
                formatted_coords.append(parsed_string)
                coords.clear()
        if coords:
            return coords
        return formatted_coords

    def resolve_place(self, place_name):
        entries = self.find_in_title(place_name)
        potential_place_entries = self.filter_place_entries(entries)

        log("INFO: Found {} potential place entries.".format(len(potential_place_entries)))
        for entry in potential_place_entries:
            log("INFO: {} has coordinates {}.".format(entry['title'], entry['coordinates']))
        result = {'place_name': place_name, 'wikipedia_entry_title': '', 'coodinates':'', 'wikipedia_entry_url': ''}
        if potential_place_entries:
            print(potential_place_entries[0]['coordinates'])
            result.update({
                'wikipedia_entry_title': potential_place_entries[0]['title'],
                'coodinates': self.clean_coordinates(potential_place_entries[0]['coordinates']),
                'wikipedia_entry_url': "https://en.wikipedia.org/wiki/" + potential_place_entries[0]['title']
            })
            log("INFO: Selecting: '{}' at {} - {}".format(potential_place_entries[0]['title'], potential_place_entries[0]['coordinates'], self.clean_coordinates(potential_place_entries[0]['coordinates'])), always=True)

        return result

def log(msg, always=True):
    if not SILENT or always:
        print(msg)

def log_setup(silent):
    global SILENT
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

    OUTPUT_FILE = "results.csv"

    if len(sys.argv) < 3:
        print('Usage:\n\t resolve-institutions.py <inputfile> <index-name> \n')
        print('Optional arguments:\n\t <es-host> -p <elastic-port> --url-prefix <elastic-prefix> -u <username> -s <password> -o <output-file> -t <timeout> --id-column <id-column> --place-column <place-column> --verbose\n')
        exit()

    argument_list = sys.argv[4:]
    options = "p:f:u:s:t:o:"
    long_options = ["port=", "url-prefix=", "es-user=", "es-password=", "timeout=", "id-column=", "place-column=", "output=", "verbose"]

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
            if current_argument in ("-o", "--output"):
                OUTPUT_FILE = current_value
            if current_argument in ("--verbose"):
                log_setup(False)
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

    if OUTPUT_FILE:
        file = open(OUTPUT_FILE, "w")
        csvwriter = csv.writer(file)
        csvwriter.writerow(["Original Place Name", "Found Place Name", "Coords", "Wikipedia", "Original Id"])

    for place in place_reader.read_places():
        result = resolver.resolve_place(place["place_name"])
        if "id" in place:
            result.update({
                "id": place["id"]
            })
        if file:
            csvwriter.writerow([result["place_name"], result["wikipedia_entry_title"], result["coodinates"], result["wikipedia_entry_url"], result["id"]])
            file.flush()

    if OUTPUT_FILE:
        file.close()

if __name__ == "__main__":
    main()
