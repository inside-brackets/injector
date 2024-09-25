import time
from os import listdir, getcwd, mkdir, sep
from os.path import isfile, join, isdir
import csv
from ast import literal_eval
import shutil
import traceback

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import yaml

INJECT_FILES_FOLDER_PATH = "files"
PARSED_FILES_FOLDER_PATH = "parsed"
LAST_MC_FILE = "last_mc.txt"
CSV_FIELDS = ["mc_number", "company_name", "dba_name", "address", "phone_number", "usdot_number", "power_units", "email", "c_status", "cargo_carried"]
FIELDS_TO_UPDATE = ("company_name", "dba_name", "address", "power_units", "email", "cargo_carried")
STATUSES_TO_UPDATE = ["unassigned", "unassigned", "deactivated","didnotpick"]

class Injector:
    def __init__(self, db_uri, db_name, collection_name):
        """
        Initialize an Injector object.

        Args:
            db_uri (str): The MongoDB connection URI.
            db_name (str): The name of the database.
            collection_name (str): The name of the collection to inject data into.
        """
        self.client = MongoClient(db_uri, server_api=ServerApi('1'))
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def inject(self, carriers):
        """
        Inject carriers' data into the MongoDB collection. This categoriezes the injection into 3 scenarios:
        
            1. The carrier is already in the database but is processed by someone. So it skips such carriers.
            2. The carrier is in the database but not yet reached. In that case it deos partial update.
            3. If the carrier doesn't exist it creates one.

        Args:
            carriers (list): A list of carrier data dictionaries to be injected.
        """
        for i, carrier in enumerate(carriers):
            search_filter = {"mc_number": carrier["mc_number"]}
            update = {"$set": {k: carrier[k] for k in FIELDS_TO_UPDATE if k in carrier}}

            db_carrier = self.collection.find_one(search_filter, projection={'appointment': False})

            print(f'INDEX {i} : MC {carrier["mc_number"]}', end="")

            if db_carrier:
                if db_carrier["c_status"] not in STATUSES_TO_UPDATE:
                    print(f' : SKIPPING CARRIER WITH STATUS "{db_carrier["c_status"]}"')
                else:
                    self.collection.update_one(search_filter, update)
                    print(" : UPDATED")
            else:
                self.collection.insert_one(carrier)
                print(" : CREATED")

            save_mc(carrier["mc_number"])

    def delete_one(self, mc):
        """
        Delete a carrier with a specific MC number from the MongoDB collection.

        Args:
            mc (int): The MC number of the carrier to be deleted.
        """
        self.collection.delete_one({"mc_number": mc})


def save_mc(mc):
    """
    Save the last MC number to a file.

    Args:
        mc (int): The last MC number to be saved.
    """
    with open("last_mc.txt", 'w+') as f:
        f.write(str(mc))


def x_minus_y(x, y):
    """
    Return elements that are in list x but not in list y.

    Args:
        x (list): The first list.
        y (list): The second list.

    Returns:
        list: The elements in x that are not in y.
    """
    return [item for item in x if item not in y]


def transform(carriers):
    """
    Transform carrier data by converting certain fields to the appropriate data types.

    Args:
        carriers (list): A list of carrier data dictionaries to be transformed.

    Returns:
        list: The transformed carrier data.
    """
    for i, carrier in enumerate(carriers):
        print(f'Transforming Carries: {i}', end='\r')
        carrier["mc_number"] = int(carrier["mc_number"])
        carrier["usdot_number"] = int(carrier["usdot_number"])
        carrier["cargo_carried"] = literal_eval(carrier["cargo_carried"])

        try:
            carrier["power_units"] = int(
                carrier["power_units"].replace(",", ""))
        except:
            pass

    return carriers


def extract_number(x):
    """
    Extract an integer from a string.

    Args:
        x (str): The input string.

    Returns:
        int: The extracted integer.
    """
    return int(x.split('.')[0])


def clean_filenames(filenames):
    """
    Filter filenames, keeping only CSV files with numeric names.

    Args:
        filenames (list): A list of filenames.

    Returns:
        list: Filtered list of filenames.
    """
    return [f for f in filenames if '.csv' in f and f.split('.')[0].isdigit()]


def get_valid_filepaths(dir):
    """
    Return valid file paths in a directory. File name should be purely numeric and file should be of type '.csv' e.g '0.csv'

    Args:
        dir (str): The directory name.

    Returns:
        list: List of valid file paths.
    """
    files_path = join(getcwd(), dir)
    filenames = [f for f in listdir(files_path) if isfile(join(files_path, f))]
    valid_filenames = clean_filenames(filenames)
    sorted_filenames = sorted(valid_filenames, key=extract_number)

    return sorted_filenames


def get_last_run():
    """
    Retrieve the last MC number from a file.

    Returns:
        int: The last MC number.
    """
    last_mc = 0
    
    try:
        with open(LAST_MC_FILE, 'r') as f:
            last_mc = f.readline()
            if last_mc.isdigit():
                last_mc = int(last_mc)
    except FileNotFoundError as fnf:
        with open(LAST_MC_FILE, 'w') as f:
            pass

    return last_mc


def get_config():
    """
    Read a YAML configuration file and return the parsed configuration.

    Returns:
        dict: The parsed configuration as a dictionary.
    """
    with open("config.yaml", "r") as stream:
        try:
            config = yaml.safe_load(stream)
            return config
        except yaml.YAMLError as exc:
            print(exc)


def move_file(from_path, to_path):
    """
    Move a file from one location to another.

    Args:
        from_path (str): The source file path.
        to_path (str): The destination file path.
    """
    if sep in to_path:
        dir_name = to_path.split(sep)[-2]
        if not isdir(dir_name):
            mkdir(dir_name)

    shutil.move(from_path, to_path)


def convert_list_to_dict(keys, values):
    if len(values) != len(keys):
        raise "Invalid values for fields"
    return dict(zip(keys, values))


def parse_carriers(file_path, last_mc):
    expected_field_count = len(CSV_FIELDS)  # Number of fields in the inject file.
    
    carriers = []

    with open(file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None) # skip the headers

        for row in csv_reader:
            # Skip empty lines
            if not any(row):
                continue
            
            # Skip if recently injected 
            if last_mc and last_mc > int(row[0]):
                continue

            # Check if the row has the expected number of fields
            if len(row) == expected_field_count:
                carriers.append(convert_list_to_dict(CSV_FIELDS, row))
            else:
                # Handle rows with the wrong number of fields
                # we split the row with (expected_len - 1) fields into two list of expected_len fields
                try:
                    cargo_carried = row[9].split("]")[0]+']'
                    mc = row[9].split("]")[1]
                    r1 = row[:9] + [cargo_carried]
                    r2 = [mc] + row[10:]
                    carriers.append(convert_list_to_dict(CSV_FIELDS, r1))
                    carriers.append(convert_list_to_dict(CSV_FIELDS, r2))
                except BaseException as e:
                    print(f"Could not parse {row}")
                    print(e)
    
    return carriers


def driver():
    """
    The main entry point for the script. It loads data, injects it into the database,
    and moves processed files to another directory.
    """
    config = get_config()
    injector = Injector(
        config['db_uri'], config['db_name'], config['collection_name']
    )

    filenames = get_valid_filepaths(INJECT_FILES_FOLDER_PATH)

    last_mc = get_last_run()
    if last_mc:
        print("Continuing where you left off....")

    for filename in filenames:
        print("LOADING FILE: ", filename, "\n")
        carriers = parse_carriers(join(INJECT_FILES_FOLDER_PATH, filename), last_mc)

        print(f"INJECTING: {len(carriers)} carriers")
        injector.inject(transform(carriers))

        move_file(join(INJECT_FILES_FOLDER_PATH, filename), join(PARSED_FILES_FOLDER_PATH, filename))


if __name__ == "__main__":
    try:
        driver()
    except Exception:
        print(traceback.format_exc())
        input("")
