from typing import List
from pandas import DataFrame

def create_run_201_exp_claims_entry(
    series: str, cb: str,
    measure_id: str, claim_cat: str, 
    denom: str, numer: str,
    level: str, claim_type: str
) -> List[str]:
    return [ series, cb, measure_id, claim_cat, denom, numer, level, claim_type ]

run_201_exp_claims_pct_macros = [

    ['201', 'claims_pct', 'exp1_1', 'a', '1=1', 'tot_bill_amt=0 ',                                    'clh', 'ip'],
    ['201', 'claims_pct', 'exp1_2', 'a', '1=1', 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',       'clh', 'ip'],
    ['201', 'claims_pct', 'exp1_3', 'a', '1=1', 'tot_mdcd_pd_amt > 2000000',                          'clh', 'ip'],
    ['201', 'claims_pct', 'exp6_1', 'a', '1=1', 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',       'clh', 'lt'],
    ['201', 'claims_pct', 'exp6_2', 'a', '1=1', 'tot_bill_amt = 0',                                   'clh', 'lt'],
    ['201', 'claims_pct', 'exp6_3', 'a', '1=1', 'tot_mdcd_pd_amt > 20000',                            'clh', 'lt'],
    ['201', 'claims_pct', 'exp11_1', 'a', '1=1', 'bill_amt=0',                                        'cll', 'ot'],
    ['201', 'claims_pct', 'exp11_2', 'a', '1=1', 'mdcd_pd_amt=0 or mdcd_pd_amt is null',              'cll', 'ot'],
    ['201', 'claims_pct', 'exp11_3', 'a', '1=1', 'mdcd_pd_amt > 100000',                              'cll', 'ot'],
    ['201', 'claims_pct', 'exp11_4', 'a', "stc_cd in ('002', '061')", 'mdcd_pd_amt=0',                'cll', 'ot'],
    ['201', 'claims_pct', 'exp16_1', 'a', '1=1', 'tot_mdcd_pd_amt > 300000',                          'clh', 'rx'],
    ['201', 'claims_pct', 'exp16_2', 'a', '1=1', 'tot_bill_amt=0',                                    'clh', 'rx'],
    ['201', 'claims_pct', 'exp16_3', 'a', '1=1', 'tot_mdcd_pd_amt =0 or tot_mdcd_pd_amt is null',     'clh', 'rx'],
    ['201', 'claims_pct', 'exp2_1', 'b', '1=1', 'tot_mdcd_pd_amt> 2000000',                           'clh', 'ip'],
    ['201', 'claims_pct', 'exp2_2', 'b', '1=1', 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',       'clh', 'ip'],
    ['201', 'claims_pct', 'exp7_1', 'b', '1=1', 'tot_mdcd_pd_amt>20000',                              'clh', 'lt'],
    ['201', 'claims_pct', 'exp7_2', 'b', '1=1', 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',       'clh', 'lt'],
    ['201', 'claims_pct', 'exp27_1', 'b', '1=1', 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'ot'],

    #['201', 'claims_pct', 'exp22_9', 'd', '1=1', 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'ot'],

    ['201', 'claims_pct', 'exp3_1', 'f', '1=1', 'tot_bill_amt=0',                                     'clh', 'ip'],
    ['201', 'claims_pct', 'exp3_2', 'f', '1=1', 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',       'clh', 'ip'],
    ['201', 'claims_pct', 'exp3_3', 'f', '1=1', 'tot_mdcd_pd_amt > 2000000',                          'clh', 'ip'],
    ['201', 'claims_pct', 'exp8_1', 'f', '1=1', 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',       'clh', 'lt'],
    ['201', 'claims_pct', 'exp8_2', 'f', '1=1', 'tot_bill_amt = 0',                                   'clh', 'lt'],
    ['201', 'claims_pct', 'exp8_3', 'f', '1=1', 'tot_mdcd_pd_amt > 20000',                            'clh', 'lt'],
    ['201', 'claims_pct', 'exp13_1', 'f', '1=1', 'bill_amt=0',                                        'cll', 'ot'],
    ['201', 'claims_pct', 'exp13_2', 'f', '1=1', 'mdcd_pd_amt=0 or mdcd_pd_amt is null',              'cll', 'ot'],
    ['201', 'claims_pct', 'exp13_3', 'f', '1=1', 'mdcd_pd_amt > 100000',                              'cll', 'ot'],
    ['201', 'claims_pct', 'exp13_4', 'f', "stc_cd in ('002', '061')", 'mdcd_pd_amt=0 ',               'cll', 'ot'],
    ['201', 'claims_pct', 'exp13_6', 'f', 'pymt_lvl_ind = 2', 'bill_amt=0',                           'cll', 'ot'],
    ['201', 'claims_pct', 'exp13_7', 'f', 'pymt_lvl_ind = 2', 'mdcd_pd_amt=0 or mdcd_pd_amt is null', 'cll', 'ot'],
    ['201', 'claims_pct', 'exp18_1', 'f', '1=1', 'tot_mdcd_pd_amt > 300000',                          'clh', 'rx'],
    ['201', 'claims_pct', 'exp18_2', 'f', '1=1', 'tot_bill_amt=0',                                    'clh', 'rx'],
    ['201', 'claims_pct', 'exp18_3', 'f', '1=1', 'tot_mdcd_pd_amt =0 or tot_mdcd_pd_amt is null',     'clh', 'rx'],
    ['201', 'claims_pct', 'exp28_1', 'g', '1=1', 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'ot'],
    ['201', 'claims_pct', 'exp4_1', 'g', '1=1', 'tot_mdcd_pd_amt> 2000000',                           'clh', 'ip'],
    ['201', 'claims_pct', 'exp4_2', 'g', '1=1', 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',       'clh', 'ip'],
    ['201', 'claims_pct', 'exp9_1', 'g', '1=1', 'tot_mdcd_pd_amt>20000',                              'clh', 'lt'],
    ['201', 'claims_pct', 'exp9_2', 'g', '1=1', 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',       'clh', 'lt'],

    #['201', 'claims_pct', 'exp24_9', 'j', '1=1', 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'ot'],

    # new measures

    ['201', 'claims_pct', 'exp29_1', 'p', "src_lctn_cd not in ('22','23')", 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'ip'],
    ['201', 'claims_pct', 'exp33_1', 'p', "src_lctn_cd not in ('22','23')", 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'lt'],
    ['201', 'claims_pct', 'exp37_1', 'p', "src_lctn_cd not in ('22','23')", 'mdcd_pd_amt=0 or mdcd_pd_amt is null',              'cll', 'ot'],
    ['201', 'claims_pct', 'exp41_1', 'p', "src_lctn_cd not in ('22','23')", 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'rx'],

    ['201', 'claims_pct', 'exp30_1', 't', "src_lctn_cd not in ('22','23')", 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'ip'],
    ['201', 'claims_pct', 'exp34_1', 't', "src_lctn_cd not in ('22','23')", 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'lt'],
    ['201', 'claims_pct', 'exp38_1', 't', "src_lctn_cd not in ('22','23')", 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'ot'],

    ['201', 'claims_pct', 'exp31_1', 'r', "src_lctn_cd not in ('22','23')", 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'ip'],
    ['201', 'claims_pct', 'exp35_1', 'r', "src_lctn_cd not in ('22','23')", 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'lt'],
    ['201', 'claims_pct', 'exp39_1', 'r', "src_lctn_cd not in ('22','23')", 'mdcd_pd_amt=0 or mdcd_pd_amt is null        ',      'cll', 'ot'],
    ['201', 'claims_pct', 'exp42_1', 'r', "src_lctn_cd not in ('22','23')", 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'rx'],


    ['201', 'claims_pct', 'exp32_1', 'v', "src_lctn_cd not in ('22','23')", 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'ip'],
    ['201', 'claims_pct', 'exp36_1', 'v', "src_lctn_cd not in ('22','23')", 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'lt'],
    ['201', 'claims_pct', 'exp40_1', 'v', "src_lctn_cd not in ('22','23')", 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null',      'clh', 'ot'],

    ['201', 'claims_pct', 'exp44_1', 'au', '1=1', 'tot_mdcd_pd_amt <> 0 and tot_mdcd_pd_amt is not null', 'clh', 'ip'],
    ['201', 'claims_pct', 'exp44_2', 'au', '1=1', 'tot_mdcd_pd_amt <> 0 and tot_mdcd_pd_amt is not null', 'clh', 'lt'],
    ['201', 'claims_pct', 'exp44_3', 'au', '1=1', 'tot_mdcd_pd_amt <> 0 and tot_mdcd_pd_amt is not null', 'clh', 'ot'],
    ['201', 'claims_pct', 'exp44_4', 'au', '1=1', 'tot_mdcd_pd_amt <> 0 and tot_mdcd_pd_amt is not null', 'clh', 'rx'],
    ['201', 'claims_pct', 'exp45_1', 'at', '1=1', 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null', 'clh', 'ip'],
    ['201', 'claims_pct', 'exp45_2', 'at', '1=1', 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null', 'clh', 'lt'],
    ['201', 'claims_pct', 'exp45_3', 'at', '1=1', 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null', 'clh', 'ot'],

    # plan id measures
    ['201', 'claims_pct_planid', 'exp29p_1', 'p', "src_lctn_cd not in ('22','23')", 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null', 'clh', 'ip'],
    ['201', 'claims_pct_planid', 'exp33p_1', 'p', "src_lctn_cd not in ('22','23')", 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null', 'clh', 'lt'],
    ['201', 'claims_pct_planid', 'exp41p_1', 'p', "src_lctn_cd not in ('22','23')", 'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null', 'clh', 'rx'],
    #['201', 'claims_pct_planid', 'exp22p_9', 'd', '1=1',                            'tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null', 'clh', 'ot'],
    create_run_201_exp_claims_entry('201', 'claims_pct_planid', 'exp37p_2', 'p', "pymt_lvl_ind = 2 and src_lctn_cd not in ('22','23')", 'mdcd_pd_amt=0 or mdcd_pd_amt is null', 'cll', 'ot'),

    create_run_201_exp_claims_entry('201', 'claims_pct', 'exp11_163', 'a', 'pymt_lvl_ind = 2', 'bill_amt=0', 'cll', 'ot'),
    create_run_201_exp_claims_entry('201', 'claims_pct', 'exp11_164', 'a', 'pymt_lvl_ind = 2', 'mdcd_pd_amt=0 or mdcd_pd_amt is null', 'cll', 'ot'),
    create_run_201_exp_claims_entry('201', 'claims_pct', 'exp37_2', 'p', "pymt_lvl_ind = 2 and src_lctn_cd not in ('22','23')", 'mdcd_pd_amt=0 or mdcd_pd_amt is null', 'cll', 'ot'),
    create_run_201_exp_claims_entry('201', 'claims_pct', 'exp39_2', 'r', "pymt_lvl_ind = 2 and src_lctn_cd not in ('22','23')", 'mdcd_pd_amt=0 or mdcd_pd_amt is null', 'cll', 'ot'),
    
    #FTX
    create_run_201_exp_claims_entry('201', 'ftx_claims_pct', 'exp22_9', 'D', '1=1', 'pymt_or_rcpmt_amt=0 or pymt_or_rcpmt_amt is null', '', ''),
    create_run_201_exp_claims_entry('201', 'ftx_claims_pct_planid', 'exp22p_9', 'D', '1=1', 'pymt_or_rcpmt_amt=0 or pymt_or_rcpmt_amt is null', '', ''),
    create_run_201_exp_claims_entry('201', 'ftx_claims_pct', 'exp24_9', 'J', '1=1', 'pymt_or_rcpmt_amt=0 or pymt_or_rcpmt_amt is null', '', ''),

 
]

df = DataFrame(run_201_exp_claims_pct_macros, columns=['series', 'cb', 'measure_id', 'claim_cat', 'denom', 'numer', 'level', 'claim_type'])
df['measure_id'] = df['measure_id'].str.upper()
df['claim_cat'] = df['claim_cat'].str.upper()
# df = df.set_index("measure_id", drop = False)
print(df.head(100))
df.to_pickle('./run_201.pkl')

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
