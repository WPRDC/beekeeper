import os, csv

from parameters.local_parameters import CITY_KEYFILEPATH, REFERENCE_DIR

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def local_dir(base_dir):
    local_directory = base_dir
    if not os.path.isdir(local_directory):
        os.makedirs(local_directory)
    return local_directory

def fetch_city_file(filename):
    """For this function to be able to get a file from the City's FTP server,
    it needs to be able to access the appropriate key file."""
    local_directory = local_dir(REFERENCE_DIR)
    cmd = "sftp -i {} pitt@ftp.pittsburghpa.gov:/pitt/{} {}".format(CITY_KEYFILEPATH, filename, local_directory)
    results = os.popen(cmd).readlines()
    #for result in results:
    #    print(" > {}".format(result))
    return local_directory + '/' + filename #results

def fetch_data_file(b):
    if 'reference' not in b:
        raise ValueError("Unable to find 'reference' field to obtain source file parameters from.")
    ref = b['reference']
    metafields = ['publisher', 'type', 'file']
    for m in metafields:
        if m not in ref:
            raise ValueError(f"Unable to find '{m}' field to obtain source file parameters from.")

    # Finally we've confirmed that all the necessary fields are present.
    if ref['publisher'] == 'pgh' and ref['type'] == 'ftp':
        filename = ref['file']
        directory = ref.get('directory', '')
        local_filepath = fetch_city_file(filename)
        return local_filepath
    else:
        raise ValueError(f"get_data does not know how to handle a reference dict of {ref}.")

def get_data_by_field(local_filepath, field):
    reader = csv.DictReader(open(local_filepath))
    return [row[field] for row in reader]
