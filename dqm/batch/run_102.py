from pandas import pandas as pd
from pandas import DataFrame

run_102 =[

    ['102', 'build_count_measure_tables', 'el112', 'el1.12', 'tmsis_var_dmgrphc_elgblty', "(ctznshp_ind='1')"],

    ['102', 'build_count_measure_tables', 'el301', 'el3.1', 'tmsis_elgblty_dtrmnt', 
    "elgblty_grp_cd in ('01','02','03','04','05','06','07','08','09','72','73','74','75','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34', '35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','59','60','61','62','63','64','65','66','67','68','69','70','71','76')"],

    ['102', 'build_count_measure_tables', 'el303', 'el3.3', 'tmsis_dsblty_info', "(dsblty_type_cd in ('01','02','03','04','05','06','07'))"],
    ['102', 'build_count_measure_tables', 'el501', 'el5.1', 'tmsis_var_dmgrphc_elgblty', "(chip_cd='2')"],
    ['102', 'build_count_measure_tables', 'el502', 'el5.2', 'tmsis_var_dmgrphc_elgblty', "(chip_cd='3')"],
    ['102', 'build_count_measure_tables', 'sumel03', 'sumel.3', 'tmsis_var_dmgrphc_elgblty', "(chip_cd in ('2','3'))"],
    ['102', 'build_count_measure_tables', 'sumel01', 'sumel.1', 'tmsis_prmry_dmgrphc_elgblty', "(1=1)"],
    ['102', 'build_count_measure_tables', 'el605', 'el6.5', 'tmsis_hh_sntrn_prtcptn_info', "(hh_sntrn_prtcptn_efctv_dt is not null)"],
    ['102', 'build_count_measure_tables', 'el606', 'el6.6', 'tmsis_hh_chrnc_cond', "(hh_chrnc_cd in ('A','B','C','D','E','F','G','H'))"],
    ['102', 'build_count_measure_tables', 'el607', 'el6.7', 'tmsis_hcbs_chrnc_cond_non_hh', "(ndc_uom_chrnc_non_hh_cd in ('001','002','003','004','005','006','007','008','009','010'))"],
    ['102', 'build_count_measure_tables', 'el608', 'el6.8', 'tmsis_lckin_info', "(lckin_efctv_dt is not null)"],
    ['102', 'build_count_measure_tables', 'el609', 'el6.9', 'tmsis_ltss_prtcptn_data', "(ltss_elgblty_efctv_dt is not null)"],
    ['102', 'build_count_measure_tables', 'el610', 'el6.10', 'tmsis_mfp_info', "(mfp_enrlmt_efctv_dt is not null)"],
    ['102', 'build_count_measure_tables', 'sumel02', 'sumel.2', 'tmsis_elgblty_dtrmnt', "(dual_elgbl_cd in ('01','02','03','04','05','06','08','09','10'))"],
    ['102', 'build_count_measure_tables', 'el612', 'el6.12', 'tmsis_elgblty_dtrmnt', "(dual_elgbl_cd = '01')"],
    ['102', 'build_count_measure_tables', 'el613', 'el6.13', 'tmsis_elgblty_dtrmnt', "(dual_elgbl_cd = '02')"],
    ['102', 'build_count_measure_tables', 'el614', 'el6.14', 'tmsis_elgblty_dtrmnt', "(dual_elgbl_cd = '03')"],
    ['102', 'build_count_measure_tables', 'el615', 'el6.15', 'tmsis_elgblty_dtrmnt', "(dual_elgbl_cd = '04')"],
    ['102', 'build_count_measure_tables', 'el616', 'el6.16', 'tmsis_elgblty_dtrmnt', "(dual_elgbl_cd = '05')"],
    ['102', 'build_count_measure_tables', 'el617', 'el6.17', 'tmsis_elgblty_dtrmnt', "(dual_elgbl_cd = '06')"],
    ['102', 'build_count_measure_tables', 'el618', 'el6.18', 'tmsis_elgblty_dtrmnt', "(dual_elgbl_cd = '08')"],
    ['102', 'build_count_measure_tables', 'el619', 'el6.19', 'tmsis_elgblty_dtrmnt', "(dual_elgbl_cd = '09')"],
    ['102', 'build_count_measure_tables', 'el620', 'el6.20', 'tmsis_elgblty_dtrmnt', "(dual_elgbl_cd = '10')"],
    ['102', 'build_count_measure_tables', 'el621', 'el6.21', 'tmsis_sect_1115a_demo_info', "(sect_1115a_demo_ind = '1')"],
    ['102', 'build_count_measure_tables', 'el1002', 'el10.2', 'tmsis_mc_prtcptn_data', "(mc_plan_type_cd in ('01','02','03','04','05','06','07','08','09','10', '11','12','13','14','15','16','17','18','60','70','80'))"],
    ['102', 'build_count_measure_tables', 'el313', 'el3.13', 'tmsis_elgblty_dtrmnt', "(elgblty_grp_cd in ('01','02','03','04','05','06','07','08','09','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26'))"],
    ['102', 'build_count_measure_tables', 'el623', 'el6.23', 'tmsis_elgblty_dtrmnt', "(rstrctd_bnfts_cd in ('1','4','5','7','A','B','D')) or (rstrctd_bnfts_cd is null)"],

    ['102', 'build_count_measure_tables', 'el327', 'el3.27', 'tmsis_elgblty_dtrmnt', "(elgblty_grp_cd in ('01', '05', '06', '07', '08', '09'))"],
    ['102', 'build_count_measure_tables', 'el328', 'el3.28', 'tmsis_elgblty_dtrmnt', "(elgblty_grp_cd in ('02', '03'))"],
    ['102', 'build_count_measure_tables', 'el329', 'el3.29', 'tmsis_elgblty_dtrmnt', "(elgblty_grp_cd in ('23', '24', '25', '26'))"],
    ['102', 'build_count_measure_tables', 'el330', 'el3.30', 'tmsis_elgblty_dtrmnt', "(elgblty_grp_cd in ('11', '12'))"],

    # ['102', 'el611', 'el611', 'el6.11', '', '']
]

df = DataFrame(run_102, columns=['series', 'cb', 'id', 'measure_id', 'input_dsn', 'condition'])

df['measure_id'] = df['measure_id'].str.replace('.', '_', regex=False).str.upper()

with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 2000):
    print (df)

df.to_pickle('./run_102.pkl')

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
