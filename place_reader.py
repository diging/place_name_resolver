import os
import csv

class UnknownFileFormatError(Exception):
    """Exception raised when a file with an invalid file extention is passed.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class ReaderMisconfigurationError(Exception):
    """Exception raised when a file with an invalid file extention is passed.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class CsvReader:
    """Reader class for csv files.

    Methods
    -------
    get_accepted_file_format()
        This class reads files for file extension "csv". Returns "csv"
    read_lines()
        Reads the lines (places) in the file. Returns a generator to iterate
        over all lines. Each place will be returned in the form
        { "id": <id or None>, "place_name": <place name> }.
    """

    def __init__(self, filepath, kwargs=[]):
        self.filepath = filepath
        if not "place_column_name" in kwargs:
            raise ReaderMisconfigurationError("No column name for place names configured. " +
            "Please pass the name of the column that contains the place names.")

        self.place_column_name = kwargs["place_column_name"]
        self.id_column_name = None
        if "id_column_name" in kwargs:
            self.id_column_name = kwargs["id_column_name"]

    def get_accepted_file_extension():
        return "csv"

    def read_lines(self):
        with open(self.filepath) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                yield { "id": row[self.id_column_name].strip() if self.id_column_name else None, "place_name": row[self.place_column_name].strip() }

class TextReader:
    """Reader class for simple text files. This class assumes that there is one
    place name per line.

    Methods
    -------
    get_accepted_file_format()
        This class reads files for file extension "txt". Returns "txt"
    read_lines()
        Reads the lines (places) in the file. Returns a generator to iterate
        over all lines. Each place will be returned in the form
        { "id": None, "place_name": <place name> }.
    """

    def __init__(self, filepath, kwargs=[]):
        self.filepath = filepath

    def get_accepted_file_extension():
        return "txt"

    def read_lines(self):
        with open(self.filepath) as input_file:
            for line in input_file:
                yield { "id": None, "place_name": line.strip() }

class PlaceReader:
    """Reader to read all places in a file. This class analyses the passed filepath
    and checks if there is a reader class available (based on the file extension of
    the passed file).

    New readers can be added by adding them to the dictionary 'readers'
    (maps file extensions to reader classes). To be compatible with this class, new
    readers need to have two methods:
    - get_accepted_file_extension(): returns the file extension the reader can handle
    - read_lines(): returns an iterable over all places. Each item in the iterable needs
        have the format { "id": <id or None>, "place_name": <place name> }

    FIXME: pass dictionary into the place reader

    Methods
    -------
    __init__(filepath)
        Initialize an object of this class. Expects the filepath to the file
        that should be read.
    read_places()
        Reads the places in the file. Returns a generator to iterate
        over all places. Each place will be returned in the form
        { "id": <id or None>, "place_name": <place name> }.
    """

    readers = {
        TextReader.get_accepted_file_extension(): TextReader,
        CsvReader.get_accepted_file_extension(): CsvReader,
    }

    def __init__(self, filepath, **kwargs):
        self.filepath = filepath
        self.kwargs = kwargs
        filename, extension = os.path.splitext(filepath)

        self.format = extension.lower()[1:]
        if not self.format:
            raise UnknownFileFormatError("File extension {} is unknown.".format(self.format))

    def read_places(self):
        if not self.format in self.readers:
            raise UnknownFileFormatError("No reader for file format '{}' available.".format(self.format))

        reader_class = self.readers[self.format]

        reader_instance = reader_class(self.filepath, self.kwargs)
        for line in reader_instance.read_lines():
            yield line
