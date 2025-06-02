from pandas import DataFrame

run_202_exp_avg_macros = [
    ['202', 'avg', 'EXP1_4', 'ip', 'clh', '', 'tot_mdcd_pd_amt', 'tot_mdcd_pd_amt < 2000000', '', 'A'],
    ['202', 'avg', 'EXP11_5', 'ot', 'cll', '', 'mdcd_pd_amt', "%not_missing_1(hcpcs_srvc_cd,1) and 0 < mdcd_pd_amt and mdcd_pd_amt < 200000", '', 'A'],
    ['202', 'avg', 'EXP16_4', 'rx', 'clh', '', 'tot_mdcd_pd_amt', 'tot_mdcd_pd_amt < 300000', '', 'A'],
    ['202', 'avg', 'EXP11_29', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '003'", '', 'A'],
    ['202', 'avg', 'EXP12_9', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '013'", '', 'B'],
    ['202', 'avg', 'EXP12_10', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '014'", '', 'B'],
    ['202', 'avg', 'EXP12_11', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '015'", '', 'B'],
    ['202', 'avg', 'EXP12_12', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '016'", '', 'B'],
    ['202', 'avg', 'EXP12_13', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '017'", '', 'B'],
    ['202', 'avg', 'EXP12_14', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '018'", '', 'B'],
    ['202', 'avg', 'EXP12_15', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '019'", '', 'B'],
    ['202', 'avg', 'EXP12_16', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '002'", '', 'B'],
    ['202', 'avg', 'EXP12_17', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '020'", '', 'B'],
    ['202', 'avg', 'EXP12_18', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '021'", '', 'B'],
    ['202', 'avg', 'EXP12_19', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '022'", '', 'B'],
    ['202', 'avg', 'EXP12_20', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '023'", '', 'B'],
    ['202', 'avg', 'EXP12_21', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '024'", '', 'B'],
    ['202', 'avg', 'EXP12_22', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '025'", '', 'B'],
    ['202', 'avg', 'EXP12_23', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '026'", '', 'B'],
    ['202', 'avg', 'EXP12_24', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '027'", '', 'B'],
    ['202', 'avg', 'EXP12_25', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '028'", '', 'B'],
    ['202', 'avg', 'EXP12_26', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '029'", '', 'B'],
    ['202', 'avg', 'EXP12_27', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '003'", '', 'B'],
    ['202', 'avg', 'EXP12_28', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '030'", '', 'B'],
    ['202', 'avg', 'EXP12_29', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '031'", '', 'B'],
    ['202', 'avg', 'EXP12_30', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '032'", '', 'B'],
    ['202', 'avg', 'EXP12_31', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '035'", '', 'B'],
    ['202', 'avg', 'EXP12_32', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '036'", '', 'B'],
    ['202', 'avg', 'EXP12_33', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '037'", '', 'B'],
    ['202', 'avg', 'EXP12_34', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '038'", '', 'B'],
    ['202', 'avg', 'EXP12_35', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '039'", '', 'B'],
    ['202', 'avg', 'EXP12_36', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '004'", '', 'B'],
    ['202', 'avg', 'EXP12_37', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '040'", '', 'B'],
    ['202', 'avg', 'EXP12_38', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '041'", '', 'B'],
    ['202', 'avg', 'EXP12_39', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '042'", '', 'B'],
    ['202', 'avg', 'EXP12_40', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '043'", '', 'B'],
    ['202', 'avg', 'EXP12_41', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '049'", '', 'B'],
    ['202', 'avg', 'EXP12_42', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '005'", '', 'B'],
    ['202', 'avg', 'EXP12_43', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '050'", '', 'B'],
    ['202', 'avg', 'EXP12_44', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '051'", '', 'B'],
    ['202', 'avg', 'EXP12_45', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '052'", '', 'B'],
    ['202', 'avg', 'EXP12_46', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '053'", '', 'B'],
    ['202', 'avg', 'EXP12_47', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '054'", '', 'B'],
    ['202', 'avg', 'EXP12_48', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '055'", '', 'B'],
    ['202', 'avg', 'EXP12_49', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '056'", '', 'B'],
    ['202', 'avg', 'EXP12_50', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '057'", '', 'B'],
    ['202', 'avg', 'EXP12_51', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '006'", '', 'B'],
    ['202', 'avg', 'EXP12_52', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '061'", '', 'B'],
    ['202', 'avg', 'EXP12_53', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '062'", '', 'B'],
    ['202', 'avg', 'EXP12_54', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '063'", '', 'B'],
    ['202', 'avg', 'EXP12_55', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '064'", '', 'B'],
    ['202', 'avg', 'EXP12_56', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '065'", '', 'B'],
    ['202', 'avg', 'EXP12_57', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '066'", '', 'B'],
    ['202', 'avg', 'EXP12_58', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '067'", '', 'B'],
    ['202', 'avg', 'EXP12_59', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '068'", '', 'B'],
    ['202', 'avg', 'EXP12_60', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '069'", '', 'B'],
    ['202', 'avg', 'EXP12_61', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '007'", '', 'B'],
    ['202', 'avg', 'EXP12_62', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '070'", '', 'B'],
    ['202', 'avg', 'EXP12_63', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '071'", '', 'B'],
    ['202', 'avg', 'EXP12_64', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '072'", '', 'B'],
    ['202', 'avg', 'EXP12_65', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '073'", '', 'B'],
    ['202', 'avg', 'EXP12_66', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '074'", '', 'B'],
    ['202', 'avg', 'EXP12_67', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '075'", '', 'B'],
    ['202', 'avg', 'EXP12_68', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '076'", '', 'B'],
    ['202', 'avg', 'EXP12_69', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '077'", '', 'B'],
    ['202', 'avg', 'EXP12_70', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '078'", '', 'B'],
    ['202', 'avg', 'EXP12_71', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '079'", '', 'B'],
    ['202', 'avg', 'EXP12_72', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '008'", '', 'B'],
    ['202', 'avg', 'EXP12_73', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '080'", '', 'B'],
    ['202', 'avg', 'EXP12_74', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '081'", '', 'B'],
    ['202', 'avg', 'EXP12_75', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '082'", '', 'B'],
    ['202', 'avg', 'EXP12_76', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '083'", '', 'B'],
    ['202', 'avg', 'EXP12_77', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '085'", '', 'B'],
    ['202', 'avg', 'EXP12_78', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '087'", '', 'B'],
    ['202', 'avg', 'EXP12_79', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '088'", '', 'B'],
    ['202', 'avg', 'EXP12_80', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '089'", '', 'B'],
    ['202', 'avg', 'EXP27_2', 'ot', 'clh', '', 'tot_mdcd_pd_amt', 'tot_mdcd_pd_amt>0 and tot_mdcd_pd_amt < 200000', '', 'B'],
    ['202', 'avg', 'EXP22_1', 'ot', 'cll', '', 'mdcd_pd_amt', '1=1', '', 'D'],
   # ['202', 'avg', 'EXP22_3', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '119'", '', 'D'],
   # ['202', 'avg', 'EXP20_2', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '121'", '', 'D'],
   # ['202', 'avg', 'EXP22_5', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '120'", '', 'D'],
   # ['202', 'avg', 'EXP22_7', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '122'", '', 'D'],
    ['202', 'avg', 'EXP3_4', 'ip', 'clh', '', 'tot_mdcd_pd_amt', 'tot_mdcd_pd_amt < 2000000', '', 'F'],
    ['202', 'avg', 'EXP18_4', 'rx', 'clh', '', 'tot_mdcd_pd_amt', 'tot_mdcd_pd_amt < 300000', '', 'F'],
    ['202', 'avg_cll_to_clh', 'EXP10_10', 'lt', '', 'tot_mdcd_pd_amt', '', '', "stc_cd = '044'", 'H'],
    ['202', 'avg_cll_to_clh', 'EXP10_11', 'lt', '', 'tot_mdcd_pd_amt', '', '', "stc_cd = '045'", 'H'],
    ['202', 'avg_cll_to_clh', 'EXP10_12', 'lt', '', 'tot_mdcd_pd_amt', '', '', "stc_cd = '046'", 'H'],
    ['202', 'avg_cll_to_clh', 'EXP10_13', 'lt', '', 'tot_mdcd_pd_amt', '', '', "stc_cd = '047'", 'H'],
    ['202', 'avg_cll_to_clh', 'EXP10_14', 'lt', '', 'tot_mdcd_pd_amt', '', '', "stc_cd = '048'", 'H'],
    ['202', 'avg_cll_to_clh', 'EXP10_15', 'lt', '', 'tot_mdcd_pd_amt', '', '', "stc_cd = '050'", 'H'],
    ['202', 'avg_cll_to_clh', 'EXP10_16', 'lt', '', 'tot_mdcd_pd_amt', '', '', "stc_cd = '059'", 'H'],
    ['202', 'avg_cll_to_clh', 'EXP10_17', 'lt', '', 'tot_mdcd_pd_amt', '', '', "stc_cd = '009'", 'H'],
    ['202', 'avg', 'EXP24_1', 'ot', 'cll', '', 'mdcd_pd_amt', '1=1', "stc_cd = '009'", 'J'],
    #['202', 'avg', 'EXP24_3', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '119'", '', 'J'],
    #['202', 'avg', 'EXP21_2', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '121'", '', 'J'],
    # ['202', 'avg', 'EXP24_5', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '120'", '', 'J'],
    # ['202', 'avg', 'EXP24_7', 'ot', 'cll', '', 'mdcd_pd_amt', "stc_cd = '122'", '', 'J'],

    #FTX
    ['202', 'ftx_avg', 'EXP22_3', 'tmsis_indvdl_cptatn_pmpm', '', '', 'pymt_or_rcpmt_amt', "pyee_mcr_plan_type = '01' ", '', 'D'],
    ['202', 'ftx_avg', 'EXP22_5', 'tmsis_indvdl_cptatn_pmpm', '', '', 'pymt_or_rcpmt_amt', "pyee_mcr_plan_type in  ('02', '03') ", '', 'D'],
    ['202', 'ftx_avg', 'EXP22_7', 'tmsis_indvdl_cptatn_pmpm', '', '', 'pymt_or_rcpmt_amt', "pyee_mcr_plan_type in  ('05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19') ", '', 'D'],
    ['202', 'ftx_avg2', 'EXP20_2', '', '', '', 'pymt_or_rcpmt_amt', '', '', 'D'],
 
    ['202', 'ftx_avg', 'EXP24_3', 'tmsis_indvdl_cptatn_pmpm', '', '', 'pymt_or_rcpmt_amt', "pyee_mcr_plan_type = '01' ", '', 'J'],
    ['202', 'ftx_avg', 'EXP24_5', 'tmsis_indvdl_cptatn_pmpm', '', '', 'pymt_or_rcpmt_amt', "pyee_mcr_plan_type in  ('02', '03') ", '', 'J'],
    ['202', 'ftx_avg', 'EXP24_7', 'tmsis_indvdl_cptatn_pmpm', '', '', 'pymt_or_rcpmt_amt', "pyee_mcr_plan_type in  ('05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19') ", '', 'J'],
    ['202', 'ftx_avg2', 'EXP21_2', '', '', '', 'pymt_or_rcpmt_amt', '', '', 'J'],
 
  
  
]

df = DataFrame(run_202_exp_avg_macros, columns=['series', 'cb', 'measure_id', 'claim_type', 'level', 'clm_avgvar', 'avgvar', 'constraint', 'line_constraint', 'claim_cat'])
df['measure_id'] = df['measure_id'].str.upper()
df['claim_cat'] = df['claim_cat'].str.upper()
# df = df.set_index("measure_id", drop = False)
print(df.head())
df.to_pickle('./run_202.pkl')

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
