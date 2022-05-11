import re
from os import listdir, path
import json
import time
import pandas as pd
import requests
import requests_cache
import re
from concurrent.futures import ThreadPoolExecutor


def merge_training_files(training_folder, training_agr_path, verbose=False):
    """
    Collects trainingfiles in a subfolder and merges them

    Function looks for all the files matching the /train-*/ pattern in training_folder,
    aggregates these files and dumps the aggregated file in training_agr_path

    :param training_folder: location to look for training files
    :param training_agr_path: location to dump aggregated training files
    :return: None
    """
    # importing the csv files
    # The path for listing items
    path = training_folder

    # All files in the source folder
    files = listdir(path)

    # Filter out non training data
    r = re.compile("train-*")
    files = list(filter(r.match, files))

    # Aggregate training files
    valid_columns = ["tconst", "primaryTitle", "originalTitle", "startYear", "endYear", "runtimeMinutes", "numVotes",
                     "label"]
    data = pd.DataFrame(columns=valid_columns)
    size_counter = 0
    for filename in files:
        filepath = path + filename
        _data = pd.read_csv(path + filename, index_col=0)
        data = pd.concat([data, _data], ignore_index=True)

        size_counter += _data.shape[0]
        if verbose: print(f'file: {filename} - {_data.shape[0]} | total {size_counter}')
    if verbose: print(f' >> data shape: {data.shape}')

    data.to_csv(training_agr_path)


def construct_writers_directors(writer_path, director_path, output_path):
    """
    gather the writer and director file, and make massage them into a useful format

    :param writer_path: location of writing.json
    :param director_path: location of directing.json
    :param output_path: location to store created file
    :return: None
    """
    with open(writer_path, 'r') as w:
        _ = json.load(w)
        writers = {}
        for d in _:
            writers[d["movie"]] = d["writer"]

    with open(director_path, 'r') as d:
        _ = json.load(d)
        directors = {}
        for key, movie in _['movie'].items():
            directors[movie] = _["director"][key]

    # print(f'writer count: {len(writers)}, director count: {len(directors)}')

    d = {k: {'writer': w, 'director': directors[k]} for k, w in writers.items()}

    del _, writers, directors

    with open(output_path, "w") as f:
        json.dump(d, f)


__api_url = 'http://www.omdbapi.com'
__api_key = '6e0762d4'
__headers = {'user-agent': 'cinema/0.0.11'}
requests_cache.install_cache('omdb_cache', expire_after=300, backend='memory')


def _omdb_lookup(tconst):
    payload = {'apikey': __api_key,
               'plot': 'short',
               'r': 'json',
               'type': 'movie',
               'v': '1',
               'i': tconst}

    result = requests.get(__api_url, headers=__headers, params=payload)

    if result.status_code != requests.codes.ok:
        raise ConnectionError

    data = result.json()

    d = {tconst: {
        'title': data['Title'],
        'year': data['Year'],
        'awards': (data['Awards'] != 'N/A'),
        'genre': data['Genre']}}
    d[tconst].update({i['Source']: int(re.sub(r'[^\w\s]','',i['Value'])[0:2]) for i in data["Ratings"]})
    return d

def mega_construct_omdb(tconst_list, file_path):
    if path.isfile(file_path) is False:
        empty = {'_': '__'}  # just add some empty bs
        with open(file_path, "w+") as f:
            json.dump(empty, f)
    with open(file_path, "r") as f:
        file_data = json.load(f)
        checked_keys = file_data.keys()

    missing_keys = list(set(tconst_list) - set(checked_keys))

    retrieved_keys = {}
    try:
        processes = []
        with ThreadPoolExecutor() as executor:
            for key in missing_keys:
                processes.append(executor.submit(retrieved_keys.update(_omdb_lookup(key))))
    except Exception as e:
        print(e)  # print break
    finally:
        file_data.update(retrieved_keys)
        with open(file_path, "w") as f:
            json.dump(file_data, f)


def automate_omdb_retrieval(tconst_list, omdb_path):
    print("total tconsts: ", len(tconst_list))
    units_togo = 1
    while units_togo > 0:
        with open(omdb_path, "r") as f:
            file_data = json.load(f)
            checked_keys = file_data.keys()
        missing_keys = list(set(tconst_list) - set(checked_keys))
        print("current missing tconsts: ", len(missing_keys))

        mega_construct_omdb(tconst_list, omdb_path)
        units_togo = len(missing_keys)
        if units_togo > 0:
            time.sleep(10)
    print("finished")
