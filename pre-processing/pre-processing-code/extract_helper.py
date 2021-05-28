import json
import mimetypes
import os
import time
import urllib
from urllib.error import URLError

import boto3
import pandas as pd
import re

from requests import HTTPError

def load_descriptor():
    with open('source_descriptor.json') as f:
        data = json.load(f)
    return data;

def source_unknown_filetype(source_dataset_url, sheet_name=None, imposter=False, columns=None, skip_header=None, skip_footer=None):

    descriptor = load_descriptor();

    my_sheet_name = descriptor['sheet_name']
    if (sheet_name is not None):
        my_sheet_name = sheet_name

    my_imposter = descriptor['imposter']
    if (imposter):
        my_imposter = imposter

    my_columns = descriptor['columns']
    if (columns is not None):
        my_columns = columns

    my_skip_header = descriptor['skip_header']
    if (skip_header is not None):
        my_skip_header = skip_header

    my_skip_footer = descriptor['skip_footer']
    if (skip_footer is not None):
        my_skip_footer = skip_footer

    data_set_name = os.environ['DATA_SET_NAME']
    data_dir = '/tmp'
    file_location = os.path.join(data_dir, data_set_name+'.csv')

    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    retries = 5
    for attempt in range(retries):
        try:

            if imposter:
                class AppURLopener(urllib.request.FancyURLopener):
                    version = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.69 Safari/537.36"
                urllib._urlopener = AppURLopener()
                urllib._urlopener.retrieve(source_dataset_url, file_location)
                filetype = mimetypes.guess_type(file_location, strict=True)[0]
                if filetype == 'application/vnd.ms-excel':
                    return source_excel(source_dataset_url, my_sheet_name,my_imposter,my_columns,my_skip_header, my_skip_footer)
            else:
                filetype = mimetypes.guess_type(source_dataset_url, strict=True)[0]
                if filetype == 'application/vnd.ms-excel':
                    return source_excel(source_dataset_url, my_sheet_name,my_imposter,my_columns,my_skip_header, my_skip_footer)
                else:
                    # TODO, create function like source_csv to use imposter and column names
                    return pd.read_csv(source_dataset_url)

        except HTTPError as e:
            if attempt == retries:
                raise Exception('HTTPError: ', e.code)
            time.sleep(0.2 * attempt)
        except URLError as e:
            if attempt == retries:
                raise Exception('URLError: ', e.reason)
            time.sleep(0.2 * attempt)
        else:
            break

def data_frame_append_file(source_dataset_url, current_df, columns, ignore_sheet=None, label_column=None, skip_header=None, skip_footer=None):
    # source_dataset_url = "http://www2.census.gov/library/data/tables/2008/demo/language-use/2009-2013-acs-lang-tables-state.xls"

    descriptor = load_descriptor();

    my_ignore_sheet = descriptor['ignore_sheet']
    if (ignore_sheet is not None):
        my_ignore_sheet = ignore_sheet

    #my_imposter = descriptor['imposter']
    #if (imposter):
    #    my_imposter = imposter

    my_columns = descriptor['columns']
    if (columns is not None):
        my_columns = columns

    my_label_column = descriptor['label_column']
    if (label_column is not None):
        my_label_column = label_column

    my_skip_header = descriptor['skip_header']
    if (skip_header is not None):
        my_skip_header = skip_header

    my_skip_footer = descriptor['skip_footer']
    if (skip_footer is not None):
        my_skip_footer = skip_footer



    response = None
    retries = 1

    asset_list = []

    for attempt in range(retries):
        try:
            # xl = pd.read_excel(source_dataset_url)
            xl = pd.ExcelFile(source_dataset_url)

            sheets = xl.sheet_names

            for sheet in sheets:
                if sheet != my_ignore_sheet:
                    #current_df.append(load_sheet(xl,sheet))
                    state_df = load_sheet(xl, sheet, my_columns, my_label_column, my_skip_header, my_skip_footer)
                    # print(state_df)
                    current_df = current_df.append(state_df, ignore_index = True )
                    # print(current_df)
        except Exception as ex:
            print(ex)

    # print(current_df)

    return current_df


def source_excel(source_dataset_url, sheet_name, imposter=False, columns=None, skip_header=None, skip_footer=None):

    data_set_name = os.environ['DATA_SET_NAME']
    data_dir = '/tmp'
    file_location = os.path.join(data_dir, data_set_name+'.csv')

    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    if imposter:
        class AppURLopener(urllib.request.FancyURLopener):
            version = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.69 Safari/537.36"
        urllib._urlopener = AppURLopener()
        urllib._urlopener.retrieve(source_dataset_url, file_location)

        xl = pd.ExcelFile(file_location)
        df = xl.parse(sheet_name)

    elif sheet_name is None:
        df = pd.read_excel(source_dataset_url)

    if columns is not None:
        length = len(columns)

        for i in range(length):
            df.columns.values[i] = columns[i]
        end = len(df.index) - skip_footer
        df = df.iloc[skip_header:end]

    result = {}
    result['data_frame'] = df
    result['file_location'] = file_location
    result['data_dir'] = data_dir
    result['data_set_name'] = data_set_name

    return result

def load_sheet(xl, sheet, columns, index_column, skip_header, skip_footer):

    name = re.sub('[^A-Za-z0-9]+', '-', sheet)

    print("Processing:" + name)

    df = xl.parse(sheet)

    length = len(columns)
    for i in range(length):
        df.columns.values[i] = columns[i]

    df[index_column] = name

    begin = skip_header # 5

    end = len(df.index) - skip_footer # 11

    response = df.iloc[5:end]
    # print(response)

    return response



def append_excel(current_df, source_dataset_url, ignore_sheet_name, imposter=False):

    # xl = pd.read_excel(source_dataset_url)
    if imposter:
        class AppURLopener(urllib.request.FancyURLopener):
            version = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.69 Safari/537.36"
        urllib._urlopener = AppURLopener()
        urllib._urlopener.retrieve(source_dataset_url, file_location)

        sheet = "Measles"
        xl = pd.ExcelFile(file_location)

    else:
        xl = pd.ExcelFile(source_dataset_url)

    sheets = xl.sheet_names

    for sheet in sheets:
        if sheet != ignore_sheet_name:
            #current_df.append(load_sheet(xl,sheet))
            state_df = load_sheet(xl,sheet)
            # print(state_df)
            current_df = current_df.append(state_df, ignore_index = True )
            # print(current_df)

    # print(current_df)

    return current_df


def convert_dataframe_to_csv(df):

    data_set_name = os.environ['DATA_SET_NAME']

    data_dir = '/tmp'
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    file_location = os.path.join(data_dir, data_set_name + '.csv')

    obj_name = file_location.split('/', 3).pop().replace(' ', '_').lower()
    file_location = os.path.join(data_dir, obj_name)
    new_s3_key = data_set_name + '/dataset/' + obj_name
    df.to_csv(file_location, index=False, sep=';')


def flatten_list(df, columns, target_column, delimiter):

    #TODO make column and alternative a combined list
    # then use a while loop
    print(df)

    try:
        out = []
        for n, row in df.iterrows():
            column_list = None
            idx = 0;
            while column_list is None:
                column_list = row[columns[idx]]
                if type(column_list) != type(str()):
                    column_list = None
                    idx = idx + 1

            if type(column_list) != type(str()):
                column_list = row[alternative]

            list = column_list.split(delimiter)
            for item in list:
                row[target_column] = item
                out += [row.copy()]
    except Exception as ex:
        print(column_list)
        print(ex)

    flattened_df = pd.DataFrame(out)

    print(flattened_df)

    return flattened_df

def transform_columns(df, source, target):
    selected_columns = df[source]
    new_df = selected_columns.copy()

    length = len(target)
    for i in range(length):
        new_df.columns.values[i] = target[i]

    print(new_df)

    return new_df