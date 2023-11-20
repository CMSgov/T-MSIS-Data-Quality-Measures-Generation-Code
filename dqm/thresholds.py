import pandas as pd
import numpy as np
import glob

thresholds = glob.glob('./dqm/cfg/Thresholds*.xlsx')
print(thresholds)
assert len(thresholds) == 1, 'Either no thresholds file or multiple files found at ./dqm/cfg/Thresholds*.xlsx; expecting one'

df = pd.read_excel(thresholds[0], sheet_name='Measures', header=None, dtype=str, engine='openpyxl')
df.columns = ['_c' + str(col) for col in df.columns]

col_names = {'_c0': 'claim_file',
             '_c3': 'display_order',
             '_c4': 'measure',
             '_c5': 'measure_id_w_display_order',
             '_c6': 'measure_name',
             '_c7': 'Measure_Type',
             '_c9': 'claim_category',
             '_c13': 'Active_Ind',
             '_c27': 'decimal_places',
             '_c33': 'Display_Type'
                        }

df = df[col_names.keys()]
df = df.iloc[1:,:]
df = df.rename(columns=col_names)

df = df[df['Active_Ind'] == 'Y']
df = df[df['measure'].notnull()]

df['measure_id'] = df['measure'].str.replace('.', '_', regex=False).str.upper()
df['display_order'] = df['display_order'].str.replace('.', '_', regex=False)
df['claim_category'] = df['claim_category'].fillna('N/A')

# assign measure parts
def createMeasureCols(measure_id):
    import re

    try:
        measure_cat = re.search(
            r"[a-zA-Z]*", measure_id, re.IGNORECASE).group()
    except AttributeError:
        measure_cat = re.search(r"[a-zA-Z]*", measure_id, re.IGNORECASE)

    if (measure_cat.startswith('SUM')):
        measure_major = measure_id[len(measure_cat):].split('_', 1)[1]
        measure_minor = ''
    else:
        measure_major = measure_id[len(measure_cat):].split('_', 1)[0]
        measure_minor = measure_id[len(measure_cat):].split('_', 1)[1]

    return {'cat': measure_cat, 'major': measure_major, 'minor': measure_minor}

df[['measure_cat', 'measure_major', 'measure_minor']] = df.apply(
     lambda x: createMeasureCols(x['measure_id']), axis=1).apply(pd.Series)
df[['z_display_order', 'display_suffix']] = df['display_order'].str.split('_', expand=True)

df['z_display_order'] = df['z_display_order'].str.zfill(3)

df['z_display_order_suffix'] = df.apply(lambda x: x['z_display_order'] + '_' + x['display_suffix']
                                        if pd.notnull(x['display_suffix']) else x['z_display_order'], axis=1)

df['decimal_places_int'] = df['decimal_places'].mask(df['decimal_places'] == 'N/A')
df['decimal_places_int'] = df['decimal_places_int'].fillna(0)
df['decimal_places_int'] = df['decimal_places_int'].astype(np.int64)
df['decimal_places'] = df['decimal_places_int']

df.to_csv('./dqm/cfg/thresholds.csv', index=False)
df.to_pickle('./dqm/cfg/thresholds.pkl')
