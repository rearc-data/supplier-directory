import source_data
from datetime import date, datetime
import pandas as pd
import openpyxl

if __name__ == '__main__':

    today = date.today().strftime('%Y-%m-%d')

    file1 = open('/Users/nfunke/iCoding/2021/rearc-data/credentials', 'r')
    lines = file1.readlines()

    REGION_NAME = lines[0].rstrip()
    AWS_SERVER_PUBLIC_KEY = lines[1].rstrip()
    AWS_SERVER_SECRET_KEY = lines[2].rstrip()
    RUN_LOCAL = True


#    source_dataset_url = "https://pasteur.epa.gov/uploads/10.23719/1517796/SupplyChainEmissionFactorsforUSIndustriesCommodities.xlsx"
#    df = pd.DataFrame.from_dict(pd.read_excel(source_dataset_url, sheet_name=None, engine='openpyxl'), orient='index', columns=['sheet','data'])
#    df = pd.read_excel(source_dataset_url, sheet_name=None, engine='openpyxl')

    #for row in df.rows:
    #    print(row)

    #df.columns.values[0] = 'Sheet'
#    for keys in df.keys():
#        print (keys)
#        df[keys].to_csv('/Users/nfunke/Temp/' %keys)
#    isempty = df.empty

    asset_list = source_data.source_dataset(
        #"world-bank-cpi.csv", "norbert-adx-test2", AWS_SERVER_SECRET_KEY
        #"world-bank-cpi.csv", "rearc-data-provider", AWS_SERVER_SECRET_KEY
        #source_dataset_url = "http://www.who.int/entity/immunization/monitoring_surveillance/data/incidence_series.xls"
    )
    print(type(asset_list))