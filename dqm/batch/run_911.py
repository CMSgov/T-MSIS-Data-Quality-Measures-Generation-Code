from pandas import DataFrame

run_911_all_ever_elig = [

    # mcr30.1, ffs24.1, mcr30.6, ffs24.5;
    ['911', 'any_span', 'ffs24_1', 'C', 'CLH', 'IP'],
    ['911', 'any_span', 'ffs24_5', 'I', 'CLH', 'IP'],
    ['911', 'any_span', 'mcr30_1', 'O', 'CLH', 'IP'],
    ['911', 'any_span', 'mcr30_6', 'U', 'CLH', 'IP'],

    # mcr31.1, ffs25.1, mcr31.6, ffs25.5;
    ['911', 'span_on_date', 'ffs25_1', 'C', 'CLH', 'IP', 'admsn_dt'],
    ['911', 'span_on_date', 'ffs25_5', 'I', 'CLH', 'IP', 'admsn_dt'],
    ['911', 'span_on_date', 'mcr31_1', 'O', 'CLH', 'IP', 'admsn_dt'],
    ['911', 'span_on_date', 'mcr31_6', 'U', 'CLH', 'IP', 'admsn_dt'],

    # mcr30.2, ffs24.2, mcr30.7, ffs24.6;
    ['911', 'any_span', 'ffs24_2', 'C', 'CLH', 'LT'],
    ['911', 'any_span', 'ffs24_6', 'I', 'CLH', 'LT'],
    ['911', 'any_span', 'mcr30_2', 'O', 'CLH', 'LT'],
    ['911', 'any_span', 'mcr30_7', 'U', 'CLH', 'LT'],

    # mcr31.2, ffs25.2, mcr31.7, ffs25.6;
    ['911', 'span_on_date', 'ffs25_2', 'C', 'CLH', 'LT', 'srvc_bgnng_dt'],
    ['911', 'span_on_date', 'ffs25_6', 'I', 'CLH', 'LT', 'srvc_bgnng_dt'],
    ['911', 'span_on_date', 'mcr31_2', 'O', 'CLH', 'LT', 'srvc_bgnng_dt'],
    ['911', 'span_on_date', 'mcr31_7', 'U', 'CLH', 'LT', 'srvc_bgnng_dt'],

    # mcr31.3, mcr31.4, ffs25.3, mcr31.8, mcr31.9, ffs25.7;
    ['911', 'span_on_date', 'ffs25_3', 'C', 'CLH', 'OT', 'srvc_bgnng_dt'],
    ['911', 'span_on_date', 'ffs25_7', 'I', 'CLH', 'OT', 'srvc_bgnng_dt'],
    #['911', 'span_on_date', 'mcr31_3', 'Y', 'CLH', 'OT', 'srvc_bgnng_dt'],
    ['911', 'span_on_date', 'mcr31_4', 'O', 'CLH', 'OT', 'srvc_bgnng_dt'],
    #['911', 'span_on_date', 'mcr31_8', 'Z', 'CLH', 'OT', 'srvc_bgnng_dt'],
    ['911', 'span_on_date', 'mcr31_9', 'U', 'CLH', 'OT', 'srvc_bgnng_dt'],

    # mcr30.3, mcr30.4, ffs24.3, mcr30.8, mcr30.9, ffs24.7;
    ['911', 'any_span', 'ffs24_3', 'C', 'CLH', 'OT'],
    ['911', 'any_span', 'ffs24_7', 'I', 'CLH', 'OT'],
    #['911', 'any_span', 'mcr30_3', 'Y', 'CLH', 'OT'],
    ['911', 'any_span', 'mcr30_4', 'O', 'CLH', 'OT'],
    ['911', 'any_span', 'mcr30_9', 'U', 'CLH', 'OT'],
    #['911', 'any_span', 'mcr30_8', 'Z', 'CLH', 'OT'],

    # mcr30.5, ffs24.4, mcr30.10, ffs24.8;
    ['911', 'any_span', 'ffs24_4', 'C', 'CLH', 'RX'],
    ['911', 'any_span', 'ffs24_8', 'I', 'CLH', 'RX'],
    ['911', 'any_span', 'mcr30_5', 'O', 'CLH', 'RX'],
    ['911', 'any_span', 'mcr30_10', 'U', 'CLH', 'RX'],

    # mcr31.5, ffs25.4, mcr31.10, ffs25.8;
    ['911', 'span_on_date', 'ffs25_4', 'C', 'CLH', 'RX', 'rx_fill_dt'],
    ['911', 'span_on_date', 'ffs25_8', 'I', 'CLH', 'RX', 'rx_fill_dt'],
    ['911', 'span_on_date', 'mcr31_5', 'O', 'CLH', 'RX', 'rx_fill_dt'],
    ['911', 'span_on_date', 'mcr31_10', 'U', 'CLH', 'RX', 'rx_fill_dt'],

    #FTX measures:

    ['911', 'ftx_any_span', 'mcr30_3', 'E'],
    ['911', 'ftx_any_span', 'mcr30_8', 'K'],

    ['911', 'ftx_span_on_date', 'mcr31_11', 'E', '', 'tmsis_indvdl_cptatn_pmpm', 'cptatn_prd_strt_dt'],
    ['911', 'ftx_span_on_date', 'mcr31_12', 'E', '', 'tmsis_indvdl_hi_prm_pymt', 'prm_prd_strt_dt'],
    ['911', 'ftx_span_on_date', 'mcr31_13', 'E', '', 'tmsis_cst_shrng_ofst', 'cvrg_prd_strt_dt'],

    ['911', 'ftx_span_on_date', 'mcr31_14', 'K', '', 'tmsis_indvdl_cptatn_pmpm', 'cptatn_prd_strt_dt'],
    ['911', 'ftx_span_on_date', 'mcr31_15', 'K', '', 'tmsis_indvdl_hi_prm_pymt', 'prm_prd_strt_dt'],
    ['911', 'ftx_span_on_date', 'mcr31_16', 'K', '', 'tmsis_cst_shrng_ofst', 'cvrg_prd_strt_dt'],

]

df = DataFrame(run_911_all_ever_elig, columns=['series', 'cb', 'measure_id', 'claim_cat', 'level', 'claim_type', 'date_var'])
df['measure_id'] = df['measure_id'].str.upper()
# df = df.set_index("measure_id", drop = False)
print(df.head())
df.to_pickle('./run_911.pkl')

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
