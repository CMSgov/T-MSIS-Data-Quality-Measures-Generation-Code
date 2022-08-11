from pandas import DataFrame

run_910_all_freq = [

    # ffs27.1, ffs35.1, mcr33.1, mcr43.1;
    ['910', 'frq', 'ffs27_1', 'IP', 'CLH', 'C', 'adjstmt_ind', '1=1'],
    ['910', 'frq', 'ffs35_1', 'IP', 'CLH', 'I', 'adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr33_1', 'IP', 'CLH', 'O', 'adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr43_1', 'IP', 'CLH', 'U', 'adjstmt_ind', '1=1'],

    # ffs28.1, ffs36.1, mcr34.1, mcr44.1;
    ['910', 'frq', 'ffs28_1', 'LT', 'CLH', 'C', 'adjstmt_ind', '1=1'],
    ['910', 'frq', 'ffs36_1', 'LT', 'CLH', 'I', 'adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr34_1', 'LT', 'CLH', 'O', 'adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr44_1', 'LT', 'CLH', 'U', 'adjstmt_ind', '1=1'],

    # ffs29.1, ffs37.1, mcr35.1, mcr36.1, mcr45.1, mcr46.1;
    ['910', 'frq', 'ffs29_1', 'OT', 'CLH', 'C', 'adjstmt_ind', '1=1'],
    ['910', 'frq', 'ffs37_1', 'OT', 'CLH', 'I', 'adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr35_1', 'OT', 'CLH', 'Y', 'adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr36_1', 'OT', 'CLH', 'O', 'adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr45_1', 'OT', 'CLH', 'Z', 'adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr46_1', 'OT', 'CLH', 'U', 'adjstmt_ind', '1=1'],

    # ffs30.1, ffs38.1, mcr37.1, mcr48.1;
    ['910', 'frq', 'ffs30_1', 'RX', 'CLH', 'C', 'adjstmt_ind', '1=1'],
    ['910', 'frq', 'ffs38_1', 'RX', 'CLH', 'I', 'adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr37_1', 'RX', 'CLH', 'O', 'adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr48_1', 'RX', 'CLH', 'U', 'adjstmt_ind', '1=1'],

    # ffs31.1, ffs39.1, mcr38.1, mcr47.1;
    ['910', 'frq', 'ffs31_1', 'IP', 'CLL', 'C', 'line_adjstmt_ind', '1=1'],
    ['910', 'frq', 'ffs39_1', 'IP', 'CLL', 'I', 'line_adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr38_1', 'IP', 'CLL', 'O', 'line_adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr47_1', 'IP', 'CLL', 'U', 'line_adjstmt_ind', '1=1'],

    # ffs32.1, ffs40.1, mcr39.1, mcr49.1;
    ['910', 'frq', 'ffs32_1', 'LT', 'CLL', 'C', 'line_adjstmt_ind', '1=1'],
    ['910', 'frq', 'ffs40_1', 'LT', 'CLL', 'I', 'line_adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr39_1', 'LT', 'CLL', 'O', 'line_adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr49_1', 'LT', 'CLL', 'U', 'line_adjstmt_ind', '1=1'],

    # ffs33.1, ffs41.1, mcr40.1, mcr41.1, mcr50.1, mcr51.1;
    ['910', 'frq', 'ffs33_1', 'OT', 'CLL', 'C', 'line_adjstmt_ind', '1=1'],
    ['910', 'frq', 'ffs41_1', 'OT', 'CLL', 'I', 'line_adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr40_1', 'OT', 'CLL', 'Y', 'line_adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr41_1', 'OT', 'CLL', 'O', 'line_adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr50_1', 'OT', 'CLL', 'Z', 'line_adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr51_1', 'OT', 'CLL', 'U', 'line_adjstmt_ind', '1=1'],

    # ffs34.1, ffs42.1, mcr42.1, mcr52.1;
    ['910', 'frq', 'ffs34_1', 'RX', 'CLL', 'C', 'line_adjstmt_ind', '1=1'],
    ['910', 'frq', 'ffs42_1', 'RX', 'CLL', 'I', 'line_adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr42_1', 'RX', 'CLL', 'O', 'line_adjstmt_ind', '1=1'],
    ['910', 'frq', 'mcr52_1', 'RX', 'CLL', 'U', 'line_adjstmt_ind', '1=1'],
]

df = DataFrame(run_910_all_freq, columns=['series', 'cb', 'measure_id', 'claim_type', 'level', 'claim_cat', 'var', 'constraint'])
df['measure_id'] = df['measure_id'].str.upper()
# df = df.set_index("measure_id", drop = False)
print(df.head())
df.to_pickle('./run_910.pkl')

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
