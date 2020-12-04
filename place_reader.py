import os

class UnknownFileFormatError(Exception):
    """Exception raised when a file with an invalid file extention is passed.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

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

    def __init__(self, filepath):
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
    (maps file extensions to reader classes).
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
    }

    def __init__(self, filepath, **kwargs):
        self.filepath = filepath
        filename, extension = os.path.splitext(filepath)

        self.format = extension.lower()[1:]
        if not self.format:
            raise UnknownFileFormatError("File extension {} is unknown.".format(self.format))

    def read_places(self):
        reader_class = self.readers[self.format]
        if not reader_class:
            raise UnknownFileFormatError("No reader for file format {} available.".format(self.format))

        reader_instance = reader_class(self.filepath)
        for line in reader_instance.read_lines():
            yield line
