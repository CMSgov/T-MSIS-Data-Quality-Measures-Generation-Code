import numpy as np
import pandas as pd

# --------------------------------------------------------------------
#
#
# --------------------------------------------------------------------
df_missingVars = pd.read_csv('../cfg/State_DQ_Missingness_Measures_final_list.csv')
df_missingVars = df_missingVars[df_missingVars['Active Indicator'] == 'Y']
df_missingVars = df_missingVars.replace(np.nan, '', regex=True)

# --------------------------------------------------------------------
#
#
# --------------------------------------------------------------------
df_missingVars['measure_id'] = df_missingVars['Measure ID'].str.upper()
df_missingVars['claim_cat'] = df_missingVars['Claims_cat_type'].str.upper()
df_missingVars['sub_cap_exclusion'] = df_missingVars['Sub-Capitated Encounters Exclusion']

# --------------------------------------------------------------------
#
#
# --------------------------------------------------------------------
df_missingVars['Measure_ID_Short2'] = df_missingVars['Measure ID Short'].str.replace('.', '_', regex=False).str.upper()
df_missingVars['Non_claims_Table2'] = df_missingVars['Data_Element'].str.replace('tmsis_', '', regex=False).str.upper().str.split('.', expand=True)[0]
df_missingVars['File_Type2'] = df_missingVars['File Type']
df_missingVars['Claims_cat_type2'] = df_missingVars['Claims_cat_type']

#df_missingVars['Data_element_ln_hdr'] = df_missingVars.apply(lambda x: x['Data_Element'][6:9].upper() if x['File Type - Summary'] == 'Claims'  else '', axis=1)
df_missingVars['Data_element_ln_hdr'] = df_missingVars.apply(lambda x: x['Data_Element'][10:12].upper() if x['File Type - Summary'] == 'Claims' and x['Data_Element'][10:12].upper() =='DX' 
                                                                                            else x['Data_Element'][6:9].upper() if x['File Type - Summary'] == 'Claims' else '' , axis=1)
df_missingVars['Data_element_var'] = df_missingVars['Data_Element'].str.split('.', expand=True)[1]
df_missingVars['size_length'] = df_missingVars['Size'].str.extract('\(([^)]+)\)')

# --------------------------------------------------------------------
#
#
# --------------------------------------------------------------------
df_missingVars = df_missingVars.set_index("measure_id", drop = False)
# print(df_missingVars)
df_missingVars.to_pickle('./run_801_missvar.pkl')

# --------------------------------------------------------------------
#
#
# --------------------------------------------------------------------
df_Miss_Non_claims = df_missingVars[df_missingVars['File Type - Summary'] == 'Non-claims']
df_Miss_Non_claims['series'] = '803'
df_Miss_Non_claims['cb'] = 'non_claims_pct'
# print(df_Miss_Non_claims)
df_Miss_Non_claims.to_pickle('./run_801_miss_non_claims.pkl')

# --------------------------------------------------------------------
#
#
# --------------------------------------------------------------------
df_Miss_Claims = df_missingVars[df_missingVars['File Type - Summary'] == 'Claims']

def data_element_update(Data_element_var):
    if Data_element_var == 'orgnl_clm_num':
        return 'orgnl_clm_num_orig'
    elif Data_element_var == 'adjstmt_clm_num':
        return 'adjstmt_clm_num_orig'
    elif Data_element_var == 'adjdctn_dt_num':
        return 'adjdctn_dt_orig'
    elif Data_element_var == 'orgnl_line_num':
        return 'orgnl_line_num_orig'
    elif Data_element_var == 'adjstmt_line_num':
        return 'adjstmt_line_num_orig'
    elif Data_element_var == 'line_adjstmt_ind':
        return 'line_adjstmt_ind_orig'
    else:
        return Data_element_var

df_Miss_Claims['Data_element_var_updt'] = df_Miss_Claims.apply(lambda x: data_element_update(x['Data_element_var']), axis=1)
df_Miss_Claims['series'] = '802'
df_Miss_Claims['cb'] = 'claims_pct'
# print(df_Miss_Claims)
df_Miss_Claims.to_pickle('./run_801_miss_claims.pkl')


# --------------------------------------------------------------------
#
#   802 - Miss Claims Pct
#
# --------------------------------------------------------------------
df_802_miss_claims_pct = pd.DataFrame()
df_802_miss_claims_pct['series'] = df_Miss_Claims['series']
df_802_miss_claims_pct['cb'] = df_Miss_Claims['cb']
df_802_miss_claims_pct['measure_id'] = df_Miss_Claims['Measure_ID_Short2']

df_802_miss_claims_pct['claim_cat'] = df_Miss_Claims['Claims_cat_type2']
df_802_miss_claims_pct['miss_var_len'] = df_Miss_Claims['size_length']
df_802_miss_claims_pct['numer'] = df_Miss_Claims['Data_element_var_updt']
df_802_miss_claims_pct['level'] = df_Miss_Claims['Data_element_ln_hdr']
df_802_miss_claims_pct['file_type'] = df_Miss_Claims['File_Type2']
df_802_miss_claims_pct['sub_cap_exclusion'] = df_Miss_Claims['sub_cap_exclusion']

df_802_miss_claims_pct = df_802_miss_claims_pct.set_index("measure_id", drop = False)
print(df_802_miss_claims_pct)
df_802_miss_claims_pct.to_pickle('./run_802.pkl')


# --------------------------------------------------------------------
#
#   803 - Miss Non-Claims Pct
#
# --------------------------------------------------------------------
df_803_miss_non_claims_pct = pd.DataFrame()
df_803_miss_non_claims_pct['series'] = df_Miss_Non_claims['series']
df_803_miss_non_claims_pct['cb'] = df_Miss_Non_claims['cb']
df_803_miss_non_claims_pct['measure_id'] = df_Miss_Non_claims['Measure_ID_Short2']

df_803_miss_non_claims_pct['miss_var_len'] = df_Miss_Non_claims['size_length']
df_803_miss_non_claims_pct['numer'] = df_Miss_Non_claims['Data_element_var']
df_803_miss_non_claims_pct['claims_table2'] = df_Miss_Non_claims['Non_claims_Table2']
df_803_miss_non_claims_pct['file_type'] = df_Miss_Non_claims['File_Type2']

df_803_miss_non_claims_pct = df_803_miss_non_claims_pct.set_index("measure_id", drop = False)
print(df_803_miss_non_claims_pct)
df_803_miss_non_claims_pct.to_pickle('./run_803.pkl')


# --------------------------------------------------------------------
#
#   For the reverse lookup
#
# --------------------------------------------------------------------
df = df_802_miss_claims_pct[['measure_id','series','cb']]
df = df.append(df_803_miss_non_claims_pct[['measure_id','series','cb']])
df_803_miss_non_claims_pct.to_pickle('./run_801.pkl')

# CC0 1.0 Universal

# Statement of Purpose

# The laws of most jurisdictions throughout the world automatically confer
# exclusive Copyright and Related Rights (defined below) upon the creator and
# subsequent owner(s) (each and all, an "owner") of an original work of
# authorship and/or a database (each, a "Work").

# Certain owners wish to permanently relinquish those rights to a Work for the
# purpose of contributing to a commons of creative, cultural and scientific
# works ("Commons") that the public can reliably and without fear of later
# claims of infringement build upon, modify, incorporate in other works, reuse
# and redistribute as freely as possible in any form whatsoever and for any
# purposes, including without limitation commercial purposes. These owners may
# contribute to the Commons to promote the ideal of a free culture and the
# further production of creative, cultural and scientific works, or to gain
# reputation or greater distribution for their Work in part through the use and
# efforts of others.

# For these and/or other purposes and motivations, and without any expectation
# of additional consideration or compensation, the person associating CC0 with a
# Work (the "Affirmer"), to the extent that he or she is an owner of Copyright
# and Related Rights in the Work, voluntarily elects to apply CC0 to the Work
# and publicly distribute the Work under its terms, with knowledge of his or her
# Copyright and Related Rights in the Work and the meaning and intended legal
# effect of CC0 on those rights.

# 1. Copyright and Related Rights. A Work made available under CC0 may be
# protected by copyright and related or neighboring rights ("Copyright and
# Related Rights"). Copyright and Related Rights include, but are not limited
# to, the following:

#   i. the right to reproduce, adapt, distribute, perform, display, communicate,
#   and translate a Work;

#   ii. moral rights retained by the original author(s) and/or performer(s);

#   iii. publicity and privacy rights pertaining to a person's image or likeness
#   depicted in a Work;

#   iv. rights protecting against unfair competition in regards to a Work,
#   subject to the limitations in paragraph 4(a), below;

#   v. rights protecting the extraction, dissemination, use and reuse of data in
#   a Work;

#   vi. database rights (such as those arising under Directive 96/9/EC of the
#   European Parliament and of the Council of 11 March 1996 on the legal
#   protection of databases, and under any national implementation thereof,
#   including any amended or successor version of such directive); and

#   vii. other similar, equivalent or corresponding rights throughout the world
#   based on applicable law or treaty, and any national implementations thereof.

# 2. Waiver. To the greatest extent permitted by, but not in contravention of,
# applicable law, Affirmer hereby overtly, fully, permanently, irrevocably and
# unconditionally waives, abandons, and surrenders all of Affirmer's Copyright
# and Related Rights and associated claims and causes of action, whether now
# known or unknown (including existing as well as future claims and causes of
# action), in the Work (i) in all territories worldwide, (ii) for the maximum
# duration provided by applicable law or treaty (including future time
# extensions), (iii) in any current or future medium and for any number of
# copies, and (iv) for any purpose whatsoever, including without limitation
# commercial, advertising or promotional purposes (the "Waiver"). Affirmer makes
# the Waiver for the benefit of each member of the public at large and to the
# detriment of Affirmer's heirs and successors, fully intending that such Waiver
# shall not be subject to revocation, rescission, cancellation, termination, or
# any other legal or equitable action to disrupt the quiet enjoyment of the Work
# by the public as contemplated by Affirmer's express Statement of Purpose.

# 3. Public License Fallback. Should any part of the Waiver for any reason be
# judged legally invalid or ineffective under applicable law, then the Waiver
# shall be preserved to the maximum extent permitted taking into account
# Affirmer's express Statement of Purpose. In addition, to the extent the Waiver
# is so judged Affirmer hereby grants to each affected person a royalty-free,
# non transferable, non sublicensable, non exclusive, irrevocable and
# unconditional license to exercise Affirmer's Copyright and Related Rights in
# the Work (i) in all territories worldwide, (ii) for the maximum duration
# provided by applicable law or treaty (including future time extensions), (iii)
# in any current or future medium and for any number of copies, and (iv) for any
# purpose whatsoever, including without limitation commercial, advertising or
# promotional purposes (the "License"). The License shall be deemed effective as
# of the date CC0 was applied by Affirmer to the Work. Should any part of the
# License for any reason be judged legally invalid or ineffective under
# applicable law, such partial invalidity or ineffectiveness shall not
# invalidate the remainder of the License, and in such case Affirmer hereby
# affirms that he or she will not (i) exercise any of his or her remaining
# Copyright and Related Rights in the Work or (ii) assert any associated claims
# and causes of action with respect to the Work, in either case contrary to
# Affirmer's express Statement of Purpose.

# 4. Limitations and Disclaimers.

#   a. No trademark or patent rights held by Affirmer are waived, abandoned,
#   surrendered, licensed or otherwise affected by this document.

#   b. Affirmer offers the Work as-is and makes no representations or warranties
#   of any kind concerning the Work, express, implied, statutory or otherwise,
#   including without limitation warranties of title, merchantability, fitness
#   for a particular purpose, non infringement, or the absence of latent or
#   other defects, accuracy, or the present or absence of errors, whether or not
#   discoverable, all to the greatest extent permissible under applicable law.

#   c. Affirmer disclaims responsibility for clearing rights of other persons
#   that may apply to the Work or any use thereof, including without limitation
#   any person's Copyright and Related Rights in the Work. Further, Affirmer
#   disclaims responsibility for obtaining any necessary consents, permissions
#   or other rights required for any use of the Work.

#   d. Affirmer understands and acknowledges that Creative Commons is not a
#   party to this document and has no duty or obligation with respect to this
#   CC0 or use of the Work.

# For more information, please see
# <http://creativecommons.org/publicdomain/zero/1.0/>
