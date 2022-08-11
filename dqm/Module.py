# --------------------------------------------------------------------
#
#   Container for Built-in Runner Modules
#
# --------------------------------------------------------------------

from dqm.submodules import Runner_101 as r101
from dqm.submodules import Runner_102 as r102
from dqm.submodules import Runner_103 as r103
from dqm.submodules import Runner_104 as r104
from dqm.submodules import Runner_105 as r105
from dqm.submodules import Runner_106 as r106
from dqm.submodules import Runner_107 as r107
from dqm.submodules import Runner_108 as r108
from dqm.submodules import Runner_109 as r109
from dqm.submodules import Runner_110 as r110

from dqm.submodules import Runner_201 as r201
from dqm.submodules import Runner_202 as r202
from dqm.submodules import Runner_204 as r204
from dqm.submodules import Runner_205 as r205
from dqm.submodules import Runner_206 as r206

from dqm.submodules import Runner_802 as r802
from dqm.submodules import Runner_803 as r803

from dqm.submodules import Runner_901 as r901
from dqm.submodules import Runner_902 as r902
from dqm.submodules import Runner_903 as r903
from dqm.submodules import Runner_904 as r904
from dqm.submodules import Runner_905 as r905
from dqm.submodules import Runner_906 as r906
from dqm.submodules import Runner_907 as r907
from dqm.submodules import Runner_909 as r909
from dqm.submodules import Runner_910 as r910
from dqm.submodules import Runner_911 as r911
from dqm.submodules import Runner_912 as r912
from dqm.submodules import Runner_913 as r913
from dqm.submodules import Runner_914 as r914
from dqm.submodules import Runner_915 as r915
from dqm.submodules import Runner_916 as r916
from dqm.submodules import Runner_917 as r917
from dqm.submodules import Runner_918 as r918
from dqm.submodules import Runner_919 as r919

from dqm.submodules import Runner_701 as r701
from dqm.submodules import Runner_702 as r702
from dqm.submodules import Runner_703 as r703
from dqm.submodules import Runner_704 as r704
from dqm.submodules import Runner_705 as r705
from dqm.submodules import Runner_706 as r706
from dqm.submodules import Runner_707 as r707
from dqm.submodules import Runner_708 as r708
from dqm.submodules import Runner_709 as r709
from dqm.submodules import Runner_710 as r710
from dqm.submodules import Runner_711 as r711
from dqm.submodules import Runner_712 as r712
from dqm.submodules import Runner_713 as r713
from dqm.submodules import Runner_714 as r714

from dqm.submodules import Runner_601 as r601
from dqm.submodules import Runner_602 as r602
from dqm.submodules import Runner_603 as r603

from dqm.submodules import Runner_501 as r501
from dqm.submodules import Runner_502 as r502
from dqm.submodules import Runner_503 as r503
from dqm.submodules import Runner_504 as r504


class Module():

    # --------------------------------------------------------------------
    #
    #   References to build-in runner modules
    #
    # --------------------------------------------------------------------
    def __init__(self):

        self.run101 = r101.Runner_101
        self.run102 = r102.Runner_102
        self.run103 = r103.Runner_103
        self.run104 = r104.Runner_104
        self.run105 = r105.Runner_105
        self.run106 = r106.Runner_106
        self.run107 = r107.Runner_107
        self.run108 = r108.Runner_108
        self.run109 = r109.Runner_109
        self.run110 = r110.Runner_110

        self.run201 = r201.Runner_201
        self.run202 = r202.Runner_202
        self.run204 = r204.Runner_204
        self.run205 = r205.Runner_205
        self.run206 = r206.Runner_206

        self.run802 = r802.Runner_802
        self.run803 = r803.Runner_803

        self.run901 = r901.Runner_901
        self.run902 = r902.Runner_902
        self.run903 = r903.Runner_903
        self.run904 = r904.Runner_904
        self.run905 = r905.Runner_905
        self.run906 = r906.Runner_906
        self.run907 = r907.Runner_907
        self.run909 = r909.Runner_909
        self.run910 = r910.Runner_910
        self.run911 = r911.Runner_911
        self.run912 = r912.Runner_912
        self.run913 = r913.Runner_913
        self.run914 = r914.Runner_914
        self.run915 = r915.Runner_915
        self.run916 = r916.Runner_916
        self.run917 = r917.Runner_917
        self.run918 = r918.Runner_918
        self.run919 = r919.Runner_919

        self.run701 = r701.Runner_701
        self.run702 = r702.Runner_702
        self.run703 = r703.Runner_703
        self.run704 = r704.Runner_704
        self.run705 = r705.Runner_705
        self.run706 = r706.Runner_706
        self.run707 = r707.Runner_707
        self.run708 = r708.Runner_708
        self.run709 = r709.Runner_709
        self.run710 = r710.Runner_710
        self.run711 = r711.Runner_711
        self.run712 = r712.Runner_712
        self.run713 = r713.Runner_713
        self.run714 = r714.Runner_714

        self.run601 = r601.Runner_601
        self.run602 = r602.Runner_602
        self.run603 = r603.Runner_603

        self.run501 = r501.Runner_501
        self.run502 = r502.Runner_502
        self.run503 = r503.Runner_503
        self.run504 = r504.Runner_504

        self.runners = {

            '101': self.run101,
            '102': self.run102,
            '103': self.run103,
            '104': self.run104,
            '105': self.run105,
            '106': self.run106,
            '107': self.run107,
            '108': self.run108,
            '109': self.run109,
            '110': self.run110,

            '201': self.run201,
            '202': self.run202,
            '204': self.run204,
            '205': self.run205,
            '206': self.run206,

            '802': self.run802,
            '803': self.run803,

            '901': self.run901,
            '902': self.run902,
            '903': self.run903,
            '904': self.run904,
            '905': self.run905,
            '906': self.run906,
            '907': self.run907,
            '909': self.run909,
            '910': self.run910,
            '911': self.run911,
            '912': self.run912,
            '913': self.run913,
            '914': self.run914,
            '915': self.run915,
            '916': self.run916,
            '917': self.run917,
            '918': self.run918,
            '919': self.run919,

            '701': self.run701,
            '702': self.run702,
            '703': self.run703,
            '704': self.run704,
            '705': self.run705,
            '706': self.run706,
            '707': self.run707,
            '708': self.run708,
            '709': self.run709,
            '710': self.run710,
            '711': self.run711,
            '712': self.run712,
            '713': self.run713,
            '714': self.run714,

            '601': self.run601,
            '602': self.run602,
            '603': self.run603,

            '501': self.run501,
            '502': self.run502,
            '503': self.run503,
            '504': self.run504
        }


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
