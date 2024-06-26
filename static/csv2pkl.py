
import pandas as pd
import os


def csv2pkl(pkl_name: str, csv_name: str = None, dtype_dict = 'str', na_filter_val = True, encode = 'utf-8'):

    if csv_name is None:
        csv_name = pkl_name

    if os.path.isfile(f'./csv/{csv_name}.csv'):
        print('Reading file ' + csv_name)
        pdf = pd.read_csv(f'./csv/{csv_name}.csv', dtype = dtype_dict, na_filter = na_filter_val, encoding = encode)
        pdf.to_pickle(f'../dqm/cfg/{pkl_name}.pkl')


csv2pkl('abd', na_filter_val = False)
csv2pkl('apdxc')
csv2pkl('countystate_lookup')
csv2pkl('fmg')
csv2pkl('prgncy')
csv2pkl('provider_classification_lookup', encode = 'cp1252')
csv2pkl('atypical_provider_table')
csv2pkl('prvtxnmy')
csv2pkl('sauths')
csv2pkl('schip')
csv2pkl('splans')
csv2pkl('st_fips')
csv2pkl('st_name')
csv2pkl('st_usps')
csv2pkl('st2_name')
csv2pkl('stabr')
csv2pkl('stc_cd', csv_name = 'type_of_service', dtype_dict = {'TypeOfService': int, 'Description': str})
csv2pkl('zcc', csv_name = 'zip_county_crosswalk', dtype_dict = {'ZipCode': str, 'Sequence': int, 'State': str, 'City': str, 'County': str, 'StateFIPS': str, 'CountyFIPS': str, 'Percent': int})
csv2pkl('zipstate_lookup')
