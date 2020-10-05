import os
import requests
import pandas as pd
from datetime import datetime
from easydict import EasyDict as edict


# ----------------------------------------------------------------------------------------------------------------------
# Global variables
# ----------------------------------------------------------------------------------------------------------------------

BASE_URL = "https://query1.finance.yahoo.com/v7/finance/download/{}?period1={}&period2={}&interval=1d&events=history"


# ----------------------------------------------------------------------------------------------------------------------
# Auxiliary functions
# ----------------------------------------------------------------------------------------------------------------------


def _make_request(url):
    """ Make a request and return the table data as dataframe.

    :param url: resource url
    :return: pandas dataframe
    """

    print("Pulling data from:\n{}".format(url))

    req = requests.get(url)
    url_content = req.content

    # TODO: pass the content to a dataframe without saving .csv to disk
    with open("tmp.csv", "wb") as fp:
        fp.write(url_content)
    df = pd.read_csv("tmp.csv")
    os.remove("tmp.csv")

    return df


def _download_index(index_name, start_date, end_date):
    """ Pull date for a specific index.

    :param index_name: Market index short name
    :param start_date: string in YYYY-MM-DD format
    :param end_date: string in YYYY-MM-DD format
    :return: pandas dataframe
    """

    start_timestamp = int(datetime.timestamp(datetime.strptime(start_date, "%Y-%m-%d")))
    end_timstamp = int(datetime.timestamp(datetime.strptime(end_date, "%Y-%m-%d")))

    url = BASE_URL.format(requests.utils.quote("^{}".format(index_name)), start_timestamp, end_timstamp)
    df = _make_request(url)

    if len(df) == 0:
        # indices like "BTC-USD" and "DAX" don't need a prepended "^" in the request URL.
        print("Failed to pull data data from:\n'{}'".format(url))
        url = BASE_URL.format(index_name, start_timestamp, end_timstamp)
        df = _make_request(url)

    if len(df) == 0:
        raise RuntimeError("Failed to pull date for index: ", index_name)

    return df

# ----------------------------------------------------------------------------------------------------------------------
# Utility functions
# ----------------------------------------------------------------------------------------------------------------------


def download(start_date, end_date, indices=None):
    """ Download all data for the predefined `indexes`

    :param start_date: string in YYYY-MM-DD format
    :param end_date: string in YYYY-MM-DD format
    :param indices: list of market index names
    :return: dictionary with dataframes
    """
    if indices is None:
        indices = ["AORD", "BTC-USD", "DAX", "DJI", "HSI", "N225", "NYA", "GSPC"]

    if not isinstance(indices, list):
        raise ValueError("Argument `indices` must be a list of strings.")

    dfs = edict({})

    for idx in indices:
        idx_key = idx.lower().replace("-", "_")
        idx_value = _download_index(idx, start_date, end_date)
        dfs.update({idx_key:idx_value})

    return dfs
