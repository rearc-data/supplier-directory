import os
import io
import urllib
from multiprocessing.dummy import Pool
from urllib.error import URLError, HTTPError
from urllib.request import urlopen
import boto3
import requests
from rearc_data_utils.pre_processing import extract_helper as eh
from rearc_data_utils.s3_helper import s3_md5_compare as s3md5
import numpy as np
import json
from boto3.s3.transfer import TransferConfig

import logging

version = '0.1.0 06-08-2021 16:22'

logger = logging.getLogger()

def upload_file(frmt_list):
    s3_uploads = []
    asset_list = []

    data_set_name = os.environ['DATA_SET_NAME']
    data_dir = '/tmp'
    s3_bucket = os.environ['S3_BUCKET']

    if (data_set_name is None):
        raise ('DATA_SET_NAME environment not set')
    if (s3_bucket is None):
        raise ('S3_BUCKET environment not set')

    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)

    for frmt in frmt_list:
        obj_name = data_set_name + frmt
            #file_location.split('/', 3).pop().replace(' ', '_').lower()
        file_location = os.path.join(data_dir, obj_name)
        new_s3_key = data_set_name + '/dataset/' + obj_name

        with open(file_location) as f:
            mystring = f.read()
            filedata = bytes(mystring, 'utf-8')

        has_changes = s3md5.md5_compare(s3, s3_bucket, new_s3_key, io.BytesIO(filedata))
        if has_changes:
            s3_resource.Object(s3_bucket, new_s3_key).put(Body=filedata)
            # sys.exit(0)
            print('Uploaded: ' + file_location)
        else:
            print('No changes in: ' + file_location)

        asset_source = {'Bucket': s3_bucket, 'Key': new_s3_key}
        s3_uploads.append({'has_changes': has_changes, 'asset_source': asset_source})

    count_updated_data = sum(upload['has_changes'] == True for upload in s3_uploads)
    if count_updated_data > 0:
        asset_list = list(map(lambda upload: upload['asset_source'], s3_uploads))
        if len(asset_list) == 0:
            raise Exception('Something went wrong when uploading files to s3')

    # asset_list is returned to be used in lamdba_handler function
    # if it is empty, lambda_handler will not republish
    return asset_list

def zip9_to_zip5 (zip9):
    if (isinstance(zip9, str)):
        z = zip9
    else:
        if (isinstance(zip9, int)):
            z = str(zip9)
        else:
            z = '000000000'

    if len(z) == 9:
        return z[:5]
    elif len(z) == 8:
        return '0' + z[:4]
    elif len(z) == 7:
        return '00' + z[:3]
    elif len(z) == 5:
        return z
    elif len(z) == 4:
        return '0' + z
    elif len(z) == 3:
        return '00' + z
    else:
        return z

def data_to_s3(frmt=None):
    # throws error occured if there was a problem accessing data
    # otherwise downloads and uploads to s3

    # https://data.cms.gov/provider-data/sites/default/files/resources/598b68bde4da1561a570cb057a4176dd_1621267521/Medical-Equipment-Suppliers.csv
    # https://data.cms.gov/provider-data/sites/default/files/resources/8525e597d915d9ab7b3ce72268cd253d_1621267522/CBP-Suppliers-Products-Carried.csv

    # source_dataset_url = 'https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/ct36-nrcq'

    response = requests.get('https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/ct36-nrcq')
    content = response.content
    fields = json.loads(content)
    download_url = fields['distribution'][0]['downloadURL'].replace('\\','')
    #source_dataset_url = "https://data.cms.gov/provider-data/sites/default/files/resources/598b68bde4da1561a570cb057a4176dd_1622563521/Medical-Equipment-Suppliers.csv"
    source_dataset_url = 'https://data.cms.gov' + download_url

    logger.info("URL used: " + source_dataset_url)

    # Old:
    # source_dataset_url = 'https://data.medicare.gov/api/views/ct36-nrcq/rows'

    try:
        df = eh.source_unknown_filetype(source_dataset_url)
        isempty = df.empty
        if isempty:
            logger.warning("Data Frame is empty: ")
        # response = urlopen(source_dataset_url) # + frmt)

    except HTTPError as e:
        raise Exception('HTTPError: ', e.code, frmt)

    except URLError as e:
        raise Exception('URLError: ', e.reason, frmt)

    # For debugging only:
    #except Exception as ex:
    #    raise ex

    else:
        df = eh.flatten_list(df,["supplieslist","specialitieslist","providertypelist"],"Product Category Name","|")

        df['PhoneNumber'] = df['telephonenumber'].astype(str).apply(
            lambda x: np.where((len(x) >= 10) & set(list(x)).issubset(list('.0123456789')),
                               '(' + x[:3] + ')' + x[3:6] + '-' + x[6:10],
                               'Phone number not in record'))
        df['zip_string9'] = df['practicezip9code']
        df['practicezip5code'] = df['practicezip9code'].apply(
            lambda x: zip9_to_zip5(x)
        )

        target =                                [
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
        source =                               [
                                   #"Competitive Bid Service Area ID",
                                   #"Competitive Bid Service Area Name",
                                   "businessname", #OK
                                   "practicename", #OK
                                   "practiceaddress1", #OK
                                   "practiceaddress2", #OK
                                   "practicecity", #OK
                                   "practicestate", #OK
                                   "practicezip5code", #OK
                                   "zip_string9", #OK
                                   "PhoneNumber", #telephonenumber", #OK
                                   #"telephonenumber", #toll free number not available, deleted
                                   "Product Category Name", #OK
                                   "is_contracted_for_cba", #OK
                                   ########## Debug below
                                   #"practicezip9code",
                                   #"zip_string",

                               ]

        df = eh.transform_columns(df, source, target)

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


        # asset_list = upload_file('.csv')

        ###
        ###s3_bucket = os.environ['S3_BUCKET']
        ###new_s3_key = data_set_name + '/dataset/'
        ###s3 = boto3.client('s3')

        ###s3.upload_file(file_location, s3_bucket, new_s3_key + filename)
        ### # #TODO delte this line # df_re_read = eh.source_unknown_filetype(file_location)

        #data = df.to_numpy().tolist()

        data3 = df.transpose()
        data4 = data3.to_dict("data")

        # data = df.to_dict("data")

        # os.remove(file_location)

        # print('Uploaded: ' + filename)


        filename = data_set_name + '.json'
        file_location = '/tmp/' + filename
        # df = df.set_index(target)
        # df.reset_index(drop=True, inplace=True)

        #data.to_json(file_location)
        with open(file_location, 'w') as fp:
            json.dump(data4, fp)

        #print(df)
        #df.to_json(file_location)

        asset_list = upload_file(['.csv','.json'])

        # s3.upload_file(file_location, s3_bucket, new_s3_key + filename)
        # print('Uploaded: ' + filename)

        # deletes to preserve limited space in aws lamdba
        os.remove(file_location)
        #TODO remove both files

        # TODO adjust asset_list with new code
        # dicts to be used to add assets to the dataset revision

        print(asset_list)

        return asset_list

        # return {'Bucket': s3_bucket, 'Key': new_s3_key + filename}


def source_dataset():
    print('REARC INFO: SOURCE_DATA.PY Version: ' + version)
    logger.info('REARC INFO: SOURCE_DATA.PY Version: ', version)

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
