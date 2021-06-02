import source_data
from datetime import date, datetime

if __name__ == '__main__':

    today = date.today().strftime('%Y-%m-%d')

    file1 = open('/Users/nfunke/iCoding/2021/rearc-data/credentials', 'r')
    lines = file1.readlines()

    REGION_NAME = lines[0].rstrip()
    AWS_SERVER_PUBLIC_KEY = lines[1].rstrip()
    AWS_SERVER_SECRET_KEY = lines[2].rstrip()
    RUN_LOCAL = True

    asset_list = source_data.source_dataset(
        #"world-bank-cpi.csv", "norbert-adx-test2", AWS_SERVER_SECRET_KEY
        #"world-bank-cpi.csv", "rearc-data-provider", AWS_SERVER_SECRET_KEY
        #source_dataset_url = "http://www.who.int/entity/immunization/monitoring_surveillance/data/incidence_series.xls"
    )
    print(type(asset_list))