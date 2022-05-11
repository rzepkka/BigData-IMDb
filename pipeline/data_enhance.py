import json
import datetime
import pandas as pd


def _writer_director_lookup(tconst, writer_director_dict):
    """
    matches given tconst to writer and directors, performs a similarity check

    :param tconst: tconst to lookup in the writer director store
    :param writer_director_dict:
    :return:
    """
    writer = writer_director_dict[tconst]['writer']
    director = writer_director_dict[tconst]['director']
    same = (writer == director)
    return writer, director, same


def match_writer_director(dataframe, writer_director_path):
    """
    Adds writers and directors of a tconst to a pandas dataframe

    :param dataframe: dataframe to match
    :param writer_director_path: location of writer_director.json
    :return: dataframe with additional columns
    """
    with open(writer_director_path, 'r') as w_d_json:
        w_d_dict = json.load(w_d_json)
        dataframe['writer'], dataframe['director'], dataframe['same_writer_director'] = \
            zip(*[_writer_director_lookup(t, w_d_dict) for t in dataframe['tconst']])


def _omdb_lookup(tconst, omdb_dict, genres):
    """

    :param tconst: tconst to look for
    :param omdb_dict: dicrtionary of omdb information about tconsts
    :param genres: all unique genres in in omdb
    :return: a dict with info about tconst
    """
    movie = omdb_dict[tconst]
    mk = movie.keys()

    genres_in_movie = movie['genre'].split(', ')
    new_columns = {}
    genre_encode = {g: (g in genres_in_movie) for g in genres}
    new_columns.update(genre_encode)
    new_columns['tconst']       = tconst
    new_columns['imdb_score']   = movie['Internet Movie Database'] if ('Internet Movie Database' in mk) else -1
    new_columns['rotten_score'] = movie['Rotten Tomatoes'] if ('Rotten Tomatoes' in mk) else -1
    new_columns['meta_score']   = movie['Metacritic'] if ('Metacritic' in mk) else -1
    new_columns['awards']       = movie['awards']

    return new_columns


def _check_genres(omdb_dict):
    genres = set()
    for k, v in omdb_dict.items():
        genres_in_movie = v['genre'].split(', ')
        genres.update(genres_in_movie)
    return list(genres)


def match_omdb(df, omdb_path):
    with open(omdb_path, 'r') as omdb_json:
        omdb_dict = json.load(omdb_json)
        genres = _check_genres(omdb_dict)

        row_list = []
        for _, tconst in df['tconst'].iteritems():
            d = _omdb_lookup(tconst, omdb_dict, genres)
            row_list.append(d)

    omdb_df = pd.DataFrame(row_list)
    df = df.merge(omdb_df, how = 'inner', on = ['tconst', 'tconst'])
    return df


def years_until_today(dataframe, col_name):
    dataframe['yearsFromRelease'] = dataframe[col_name].apply(lambda x: datetime.date.today().year - x)