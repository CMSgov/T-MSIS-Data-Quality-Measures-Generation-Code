# --------------------------------------------------------------------
#
#   Dynamic logic and assertions for variables.
#
# --------------------------------------------------------------------
class DQClosure():

    def miss_misslogic(var):
        return f"""( not {var} rlike '[a-zA-Z1-9]' or
            {var} is null
            )"""

    def miss_misslogic_c6(var):
        return f"""( {var} ='6' or
            not {var} rlike '[a-zA-Z0-9]' or
            {var} is null
            )"""

    def miss_misslogic_c017(var):
        return f"""( {var} ='017' or
            not {var} rlike '[a-zA-Z1-9]' or
            {var} is null
            )"""

    def miss_misslogic_cU(var):
        return f"""( {var} = 'U' or
            not {var} rlike '[a-zA-Z1-9]' or
            {var} is null
            )"""

    def miss_misslogic_c9(var):
        return f"""( {var} ='9' or
            not {var} rlike '[a-zA-Z1-8]' or
            {var} is null
            )"""

    def miss_misslogic_c88_99(var):
        return f"""( {var} ='88' or
            {var} = '99' or
            not {var} rlike '[a-zA-Z1-9]' or
            {var} is null
            )"""

    def miss_misslogic_c88(var):
        return f"""( {var} ='88' or
            not {var} rlike '[a-zA-Z1-9]' or
            {var} is null
            )"""

    def miss_misslogic_ex000(var):
        return f"""( ({var} <> '000' and (not {var} rlike '[a-zA-Z1-9]'))
                or
            {var} is null
            )"""

    def not_missing_1(var, length):
        return f"""({var} is not NULL and
                            not {var} like repeat(8,{length}) and
                            not {var} like repeat(9,{length}) and
                            {var} rlike '[a-zA-Z1-9]')"""

    def is_missing_2(var, length):
        return f"""({var} is NULL or
                            abs({var}) like repeat(8,{length}) or
                            abs({var}) like repeat (9,{length}) or
                            not {var} rlike '[1-9]')"""

    def is_missing_3(var, length, dec):
        return f"""({var} is NULL or
                            abs({var}) like concat_ws('.',repeat(8,{length}),repeat(8,{dec})) or
                            abs({var}) like concat_ws('.',repeat(9,{length}),repeat(9,{dec})) or
                            not {var} rlike '[1-9]')"""

    def ssn_nmisslogic(var, length):
        return f"""case when {var} like repeat(8,{length}) then 0
                        when {var} like repeat(9,{length}) then 0
                        when {var} is null then 0
                        when {var} rlike '[1-9]' then 1
                   else 0 end"""

    def misslogic(var, length=None):
        if length is not None:
            return f"""case when {var} like repeat(8,{length}) then 1
                            when {var} like repeat(9,{length}) then 1
                            when {var} is null then 1
                            when {var} rlike '[A-Za-z1-9]' then 0
                   else 1 end"""
        else:
            return f"""(not {var} rlike '[a-zA-Z1-9]' or
                            {var} is null)"""

    def nmisslogic(var, length=None):
        if length is not None:
            return f"""case when {var} like repeat(8,{length}) then 0
                        when {var} like repeat(9,{length}) then 0
                        when {var} is null then 0
                        when {var} rlike '[A-Za-z1-9]' then 1
                   else 0 end"""
        else:
            return f"""({var} is not NULL and
                        {var} rlike '[a-zA-Z1-9]')"""

    def misslogicprv_id(var, length):
        return f"""case when {var} like repeat(8,{length}) then 1
                        when {var} like repeat(9,{length}) then 1
                        when {var} is null then 1
                        when {var} = '0' then 1
                        when {var} rlike '[A-Za-z1-9]' then 0
                   else 1 end"""

    def nmsng(var, length):
        return f"""({var} is not NULL and
                    not {var} like repeat(8,{length}) and
                    not {var} like repeat(9,{length}) and
                    {var} rlike '[a-zA-Z1-9]')"""

    # --------------------------------------------------------------------
    #
    #   V-table for macro calls
    #
    # --------------------------------------------------------------------
    passthrough = {
        '%miss_misslogic': miss_misslogic,
        '%miss_misslogic_c6': miss_misslogic_c6,
        '%miss_misslogic_c017': miss_misslogic_c017,
        '%miss_misslogic_cU': miss_misslogic_cU,
        '%miss_misslogic_c9': miss_misslogic_c9,
        '%miss_misslogic_c88_99': miss_misslogic_c88_99,
        '%miss_misslogic_c88': miss_misslogic_c88,
        '%miss_misslogic_ex000': miss_misslogic_ex000,
        '%not_missing_1': not_missing_1,
        '%is_missing_2': is_missing_2,
        '%is_missing_3': is_missing_3,
        '%ssn_nmisslogic': ssn_nmisslogic,
        '%misslogic': misslogic,
        '%nmisslogic': nmisslogic,
        '%misslogicprv_id': misslogicprv_id,
        '%nmsng': nmsng,
        'ltc_days': 'coalesce(lve_days_cnt,0) + coalesce(ICF_IID_DAYS_CNT,0) + coalesce(NRSNG_FAC_DAYS_CNT,0) + coalesce(MDCD_CVRD_IP_DAYS_CNT,0)',
        'ltc_days1': 'coalesce(lve_days_cnt,0) + coalesce(nrsng_fac_days_cnt,0)',
        'ltc_days2': 'coalesce(lve_days_cnt,0) + coalesce(mdcd_cvrd_ip_days_cnt,0)',
        'ltc_days3': 'coalesce(lve_days_cnt,0) + coalesce(icf_iid_days_cnt,0)',
        'm_start': '{m_start}',
        'm_end': '{m_end}'
    }

    # --------------------------------------------------------------------
    #
    #   Lexical Analysis for a Closure
    #
    # --------------------------------------------------------------------
    @staticmethod
    def parse(var):
        oplen = len(var)
        i = 0
        pos = [0]
        tokens = []
        while i >= 0:
            i = var.find('%', i, oplen)
            if i >= 0:
                pos.extend([i])
                i += 1
        pos.extend([oplen])
        i = 0
        for p in pos[:-1]:
            s = var[p:pos[i + 1]]
            tokens.extend([s])
            i += 1
        conditions = []
        for t in tokens:
            t1 = t[0:t.find('(')]
            t2 = t[t.find('('):len(t)]
            macro = [t1, t2]
            if len(macro[0]) > 1:
                if macro[0].strip() in DQClosure.passthrough.keys():
                    predicate = macro[1]
                    k_pos = predicate.find(')')
                    params = predicate[1:k_pos]
                    trail = predicate[k_pos+1:len(predicate)]
                    args = params.split(',')
                    if (len(args) == 1):
                        conditions.append(DQClosure.passthrough[macro[0].strip()](args[0]))
                    elif (len(args) == 2):
                        conditions.append(DQClosure.passthrough[macro[0].strip()](args[0], args[1]))
                    elif (len(args) == 3):
                        conditions.append(DQClosure.passthrough[macro[0].strip()](args[0], args[1], args[2]))
                    conditions.append(trail)
                    m = 2
                    while m < len(macro):
                        conditions.append(str(macro[m]).format(**DQClosure.passthrough))
                        m += 1
                else:
                    conditions.append(str(t).format(**DQClosure.passthrough))
            else:
                conditions.append(str(t).format(**DQClosure.passthrough))
        return '\n'.join(conditions)


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