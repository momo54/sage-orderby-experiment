from os import listdir
from os.path import basename

def list_files(directory, extension):
    return (f for f in listdir(directory) if f.endswith('.' + extension))

def query_name(file):
    return basename(file).split('.')[0]
