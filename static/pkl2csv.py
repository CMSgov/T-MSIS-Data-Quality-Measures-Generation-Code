
import pandas as pd
import os


def pkl2csv(pkl_name: str, csv_name: str = None):

    if csv_name is None:
        csv_name = pkl_name

    if os.path.isfile(f'../dqm/cfg/{pkl_name}.pkl'):
        print('Reading file ' + pkl_name)
        pdf = pd.read_pickle(f'../dqm/cfg/{pkl_name}.pkl')
        pdf.to_csv(f'./csv/{csv_name}.csv', index=False)


pkl2csv('apdxc')
pkl2csv('countystate_lookup')
pkl2csv('fmg')
pkl2csv('missvar')
pkl2csv('prgncy')
pkl2csv('provider_classification_lookup')
pkl2csv('prvtxnmy')
pkl2csv('sauths')
pkl2csv('schip')
pkl2csv('splans')
pkl2csv('st_fips')
pkl2csv('st_name')
pkl2csv('st_usps')
pkl2csv('st2_name')
pkl2csv('stabr')
pkl2csv('stc_cd', 'type_of_service')
pkl2csv('zcc', 'zip_county_crosswalk')
pkl2csv('zipstate_lookup')
