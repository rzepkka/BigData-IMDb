#%%

#%%
import pandas as pd
import numpy as np
import re
from unidecode import unidecode
from scipy import stats


def fix_dtypes(dataframe):
    """
    sets the dtypes of a dataframe, returns the fixed dataframe
    :param dataframe: dataframe to fix
    :return: None
    """
    integer_columns = ["startYear", "endYear", "runtimeMinutes", "numVotes"]
    bool_columns = ["label", "same_writer_director"]
    string_columns = ["tconst", "primaryTitle", "originalTitle", "writer", "director"]

    for c in integer_columns:
        if c in dataframe.columns:
            dataframe[c] = pd.to_numeric(dataframe[c], errors="coerce")
    for c in string_columns:
        if c in dataframe.columns:
            dataframe[c] = dataframe[c].astype('string')
    for c in bool_columns:
        if c in dataframe.columns:
            dataframe[c] = dataframe[c].astype('boolean')

    # print(dataframe.dtypes)


def col_a_if_a_else_b(dataframe, col_a, col_b, new_col_name, t="num"):
    for c in [col_a, col_b]:
        if c not in dataframe.columns:
            raise Exception(f'column "{c}" not in dataframe')
    if new_col_name in [col_a, col_b]:
        raise Exception(f'new column "{new_col_name}" cannot be "{col_a}" or "{col_b}"')

    if t == "num":
        dataframe[new_col_name] = np.where(~np.isnan(dataframe[col_a]), dataframe[col_a], dataframe[col_b])
    if t == "text":
        dataframe[new_col_name] = np.where(dataframe[col_a].astype(bool), dataframe[col_a], dataframe[col_b])

    dataframe.drop([col_a, col_b], inplace=True, axis=1)


def _clean_text(text: str):
    """
    scrubs special characters from text and lowers it
    :param text: text to clean
    :return: cleaned text
    """
    text = re.sub(r'[^\w\s]','',text)
    text = text.lower()
    text = unidecode(text)
    return text


def clean_text_df(dataframe, column_names):
    """
    fixes text in column name of dataframe
    :param dataframe:
    :param column_name:
    :return: None
    """
    for column_name in column_names:
        dataframe[column_name] = dataframe[column_name].apply(_clean_text)


def fill_missing_numerics(dataframe, column_names, to_cut=0.004):
    for column_name in column_names:
        num_missing = list(dataframe[column_name].dropna())
        trim = round(stats.trim_mean(num_missing, to_cut),2)
        #     dataframe[column_name] = dataframe[column_name].apply(lambda x: float(trim) if(pd.isnull(x)) else x)
        dataframe[column_name] = dataframe[column_name].fillna(float(trim))

    return dataframe