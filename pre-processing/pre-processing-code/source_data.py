import os
from multiprocessing.dummy import Pool
from urllib.error import URLError, HTTPError
from urllib.request import urlopen
import boto3
from extract_helper import *
import numpy as np


def data_to_s3(frmt=None):
    # throws error occured if there was a problem accessing data
    # otherwise downloads and uploads to s3

    # https://data.cms.gov/provider-data/sites/default/files/resources/598b68bde4da1561a570cb057a4176dd_1621267521/Medical-Equipment-Suppliers.csv
    # https://data.cms.gov/provider-data/sites/default/files/resources/8525e597d915d9ab7b3ce72268cd253d_1621267522/CBP-Suppliers-Products-Carried.csv

    # source_dataset_url = 'https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/ct36-nrcq'

    source_dataset_url = "https://data.cms.gov/provider-data/sites/default/files/resources/598b68bde4da1561a570cb057a4176dd_1621267521/Medical-Equipment-Suppliers.csv"


    # Old:
    # source_dataset_url = 'https://data.medicare.gov/api/views/ct36-nrcq/rows'

    try:
        df = source_unknown_filetype(source_dataset_url)
        # response = urlopen(source_dataset_url) # + frmt)

    except HTTPError as e:
        raise Exception('HTTPError: ', e.code, frmt)

    except URLError as e:
        raise Exception('URLError: ', e.reason, frmt)

    else:
        df = flatten_list(df,["supplieslist","specialitieslist"],"Product Category Name","|")

        df['PhoneNumber'] = df['telephonenumber'].astype(str).apply(
            lambda x: np.where((len(x) >= 10) & set(list(x)).issubset(list('.0123456789')),
                               '(' + x[:3] + ')' + x[3:6] + '-' + x[6:10],
                               'Phone number not in record'))
        df['zip_string'] = df['practicezip9code'].astype(str)
        df['zip_string9'] = df['zip_string'].where(
                                df['zip_string'].str.len() != 9,
                                "0" + df['zip_string']
                            )
        df['practicezip5code'] = df['zip_string9'].str[:5]

        df = transform_columns(df,
                               [
                                   #"Competitive Bid Service Area ID",
                                   #"Competitive Bid Service Area Name",
                                   "businessname", #OK
                                   "practicename", #OK
                                   "practiceaddress1", #OK
                                   "practiceaddress2", #OK
                                   "practicecity", #OK
                                   "practicestate", #OK
                                   "practicezip5code", #TODO make Zip 5
                                   "zip_string9", #OK
                                   "PhoneNumber", #telephonenumber", #OK
                                   #"telephonenumber", #toll free number not available, deleted
                                   "Product Category Name", #OK
                                   "is_contracted_for_cba", #OK
                                   ########## Debug below
                                   #"practicezip9code",
                                   #"zip_string",

                               ],
                               [
                                   #"Competitive Bid Service Area ID",
                                   #"Competitive Bid Service Area Name",
                                   "Company Name",
                                   "DBA Name",
                                   "Address",
                                   "Address 2",
                                   "City",
                                   "State",
                                   "Zip",
                                   "Zip Plus 4",
                                   "Phone",
                                   #"Toll-Free Telephone",
                                   "Product Category Name",
                                   "Competitive Bid",
                                   ########## Debug below
                                   #"practicezip9code",
                                   #"zip_string",
                               ]
                               )

        frmt = ".csv"
        data_set_name = os.environ['DATA_SET_NAME']

        filename = data_set_name + frmt
        file_location = '/tmp/' + filename


        df.to_csv(file_location, index=False, sep=',')


        #
        #with open(file_location, 'wb') as f:
        #    f.write(response.read())
        #    f.close()

         # variables/resources used to upload to s3
        s3_bucket = os.environ['S3_BUCKET']
        new_s3_key = data_set_name + '/dataset/'
        s3 = boto3.client('s3')

        s3.upload_file(file_location, s3_bucket, new_s3_key + filename)

        print('Uploaded: ' + filename)

        # deletes to preserve limited space in aws lamdba
        os.remove(file_location)

        # dicts to be used to add assets to the dataset revision
        return {'Bucket': s3_bucket, 'Key': new_s3_key + filename}


def source_dataset():

    # list of enpoints to be used to access data included with product
    data_endpoints = [
        '.json',
        '.csv'
    ]

    # multithreading speed up accessing data, making lambda run quicker
    #with (Pool(2)) as p:
    #    asset_list = p.map(data_to_s3, data_endpoints)

    asset_list = data_to_s3()

    # asset_list is returned to be used in lamdba_handler function
    return asset_list
