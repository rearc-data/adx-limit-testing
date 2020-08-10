import os
import boto3
import json
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from multiprocessing.dummy import Pool


def data_to_s3(int_num):

    num = str(int_num)
    data_set_name = os.environ['DATA_SET_NAME']
    filename = data_set_name + '_' + num + '.json'
    file_location = '/tmp/' + filename

    with open(file_location, 'w', encoding='utf-8') as f:
        f.write(json.dumps({'test_file_num': num}))

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

    # multithreading speed up accessing data, making lambda run quicker
    with (Pool(40)) as p:
        asset_list = p.map(data_to_s3, list(range(0, 2000)))

    asset_lists = []

    for asset in asset_list:
        if len(asset_lists) == 0:
            asset_lists.append([asset])
        elif len(asset_lists[len(asset_lists) - 1]) == 100:
            asset_lists.append([asset])
        else:
            asset_lists[len(asset_lists) - 1].append(asset)

    # asset_list is returned to be used in lamdba_handler function
    return asset_lists
