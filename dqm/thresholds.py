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


#---------------------------
#load CHIPRA lookup file
#---------------------------

chipra_lookup = glob.glob('./dqm/cfg/CHIPRA_Lookup*.xlsx')
print(chipra_lookup)
assert len(chipra_lookup) == 1, 'Either no chipra_lookup file or multiple files found at ./dqm/cfg/CHIPRA_Lookup*.xlsx; expecting one'

df = pd.read_excel(chipra_lookup[0], sheet_name='chipra_table', header=None, dtype=str, engine='openpyxl')
df.columns = ['_c' + str(col) for col in df.columns]

col_names = {'_c0': 'state',
             '_c1': 'children_only',
             '_c2': 'pregnant_women_only',
             '_c3': 'both',
             '_c4': 'under_19'
            }

df = df[col_names.keys()]
df = df.iloc[1:,:]
df = df.rename(columns=col_names)

#only keep rows with non-missing state value
df = df[df['state'].notnull()]

df = df[~df["state"].str.startswith("Source", na=False)]

#df.to_csv('./dqm/cfg/chipra_lookup.csv', index=False)

#readin csv files as pandas dataframe
stabr_df = pd.read_csv('./static/csv/stabr.csv')
st_fips_df = pd.read_csv('./static/csv/st_fips.csv')

#merge the chipra lookup dataframe with state abbr and state fips
merged_df = (
     df
    .merge(
         stabr_df,
         how='left',
         left_on="state",
         right_on="STNAME" 
    )
    .merge(
         st_fips_df,
         how='left',
         on="STABBREV"
    )
    [['state', 'STABBREV', 'FIPS', 'children_only', 'pregnant_women_only', 'both', 'under_19']]
    .rename(columns={'STABBREV': 'state_cd'})
    .rename(columns={'FIPS': 'state_fips'})
)

#create a table for chipra lookup file
#table_view_name = f"chipra_table"
#spark.createDataFrame(merged_df).createOrReplaceTempView(table_view_name)

merged_df.to_csv('./dqm/cfg/chipra_lookup_fips.csv', index=False)