
import pandas as pd
import os


def csv2pkl(pkl_name: str, csv_name: str = None, dtype_dict = 'str'):

    if csv_name is None:
        csv_name = pkl_name

    if os.path.isfile(f'./csv/{csv_name}.csv'):
        print('Reading file ' + csv_name)
        pdf = pd.read_csv(f'./csv/{csv_name}.csv', dtype = dtype_dict)
        pdf.to_pickle(f'../dqm/cfg/{pkl_name}.pkl')


csv2pkl('abd')
csv2pkl('apdxc')
csv2pkl('countystate_lookup')
csv2pkl('fmg')
csv2pkl('missvar')
csv2pkl('prgncy')
csv2pkl('provider_classification_lookup')
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
csv2pkl('stc_cd', 'type_of_service', {'TypeOfService': int, 'Description': str})
csv2pkl('zcc', 'zip_county_crosswalk', {'ZipCode': str, 'Sequence': int, 'State': str, 'City': str, 'County': str, 'StateFIPS': str, 'CountyFIPS': str, 'Percent': int})
csv2pkl('zipstate_lookup')
