
import pandas as pd
import os


def csv2pkl(pkl_name: str, csv_name: str = None):

    if csv_name is None:
        csv_name = pkl_name

    if os.path.isfile(f'./csv/{csv_name}.csv'):
        print('Reading file ' + csv_name)
        pdf = pd.read_csv(f'./csv/{csv_name}.csv')
        pdf.to_pickle(f'../dqm/cfg/{pkl_name}.pkl')


csv2pkl('apdxc')
csv2pkl('countystate_lookup')
csv2pkl('fmg')
csv2pkl('missvar')
csv2pkl('prgncy')
csv2pkl('provider_classification_lookup')
csv2pkl('prvtxnmy')
csv2pkl('sauths')
csv2pkl('schip')
csv2pkl('splans')
csv2pkl('st_fips')
csv2pkl('st_name')
csv2pkl('st_usps')
csv2pkl('st2_name')
csv2pkl('stabr')
csv2pkl('stc_cd', 'type_of_service')
csv2pkl('zcc', 'zip_county_crosswalk')
csv2pkl('zipstate_lookup')
