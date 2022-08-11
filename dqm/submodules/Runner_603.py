# --------------------------------------------------------------------
#
#
#
# --------------------------------------------------------------------
from dqm.DQM_Metadata import DQM_Metadata
from dqm.DQMeasures import DQMeasures

class Runner_603:

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def run(spark, dqm: DQMeasures, measures: list = None) :
        dqm.logger.info('Module().run<series>.run() has been deprecated. Use dqm.run(spark) or dqm.run(spark, dqm.where(series="<series>")) instead.')
        if measures is None:
            return dqm.run(spark, dqm.reverse_measure_lookup[dqm.reverse_measure_lookup['series'] == '603']['measure_id'].tolist())
        else:
            return dqm.run(spark, measures)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def tpl_prsn_hi_cvrg(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_tpl_prsn_hi as
                select
                    submtg_state_cd
                    ,msis_ident_num
                    ,max(case when (cvrg_type_cd in {DQM_Metadata.tpl_tables.tpl_prsn_hi_sql.tpl_cvrg_typ } ) then 1 else 0 end) as tpl1_2
                from {dqm.taskprefix}_tmsis_tpl_mdcd_prsn_hi
                group by submtg_state_cd, msis_ident_num
             """

        dqm.logger.debug(z)

        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_tpl_prsn_hi_tab as
        z = f"""
                SELECT
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'603' as submodule
                    ,null as numer
                    ,null as denom
                    ,sum(tpl1_2) as mvalue

                from {dqm.taskprefix}_tpl_prsn_hi
                group by submtg_state_cd
            """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def tpl_prsn_hi_insrnc(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_tpl_prsn_hi as
                select
                    submtg_state_cd
                    ,msis_ident_num
                    ,max(case when (insrnc_plan_type_cd in {DQM_Metadata.tpl_tables.tpl_prsn_hi_sql.tpl_insrnc_typ} ) then 1 else 0 end) as tpl1_3
                from {dqm.taskprefix}_tmsis_tpl_mdcd_prsn_hi
                group by submtg_state_cd, msis_ident_num
             """

        dqm.logger.debug(z)

        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_tpl_prsn_hi_tab as
        z = f"""
                SELECT
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'603' as submodule
                    ,null as numer
                    ,null as denom
                    ,sum(tpl1_3) as mvalue

                from {dqm.taskprefix}_tpl_prsn_hi
                group by submtg_state_cd
            """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def ever_tpl_elig_prsn_mn_sql(spark, dqm: DQMeasures, measure_id, x) :

        # create or replace temporary view {dqm.taskprefix}_ever_tpl_elig as
        e = f"""
                select
                    a.submtg_state_cd
                    ,a.msis_ident_num
                    ,a.ever_tpl
                    ,coalesce(b.ever_eligible,0) as ever_eligible

                from {dqm.taskprefix}_ever_tpl a
                left join {dqm.taskprefix}_ever_elig b
                on a.msis_ident_num = b.msis_ident_num
                and a.submtg_state_cd = b.submtg_state_cd
            """

        # create or replace temporary view {dqm.taskprefix}_uniq_ever_tpl_elig as
        u = f"""
                select
                    submtg_state_cd
                    ,msis_ident_num
                    ,max(ever_tpl) as ever_tpl
                    ,max(ever_eligible) as ever_eligible
                from (
                    {e}
                )
                group by submtg_state_cd, msis_ident_num
            """

        # create or replace temporary view {dqm.taskprefix}_ever_tpl_elig_tab as
        z = f"""
                SELECT
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'603' as submodule
                    ,sum(ever_tpl) as denom
                    ,sum(ever_eligible) as numer
                    ,round((sum(ever_eligible) / sum(ever_tpl)),4) as mvalue
                from (
                    {u}
                )
                group by submtg_state_cd
            """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def tpl_prsn_mn_prsn(spark, dqm: DQMeasures, measure_id, x) :

        y = f"""
                select
                    submtg_state_cd
                    ,msis_ident_num
                    ,max(case when elgbl_prsn_mn_efctv_dt is not null then 1 else 0 end) as tpl1_1
                from  {dqm.taskprefix}_tmsis_tpl_mdcd_prsn_mn
                group by submtg_state_cd, msis_ident_num
             """

        # create or replace temporary view {dqm.taskprefix}_tpl_prsn_hi_tab as
        z = f"""
                SELECT
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'603' as submodule
                    ,null as numer
                    ,null as denom
                    ,sum(tpl1_1) as mvalue

                from (
                    {y}
                )
                group by submtg_state_cd
            """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def tpl_prsn_mn_cvrg(spark, dqm: DQMeasures, measure_id, x) :

        y = f"""
                select
                    submtg_state_cd
                    ,msis_ident_num
                    ,max(case when (tpl_insrnc_cvrg_ind = '1' or tpl_othr_cvrg_ind = '1')
                        then 1 else 0 end) as tpl1_4
                from  {dqm.taskprefix}_tmsis_tpl_mdcd_prsn_mn
                group by submtg_state_cd, msis_ident_num
             """

        # create or replace temporary view {dqm.taskprefix}_tpl_prsn_hi_tab as
        z = f"""
                SELECT
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'603' as submodule
                    ,null as numer
                    ,null as denom
                    ,sum(tpl1_4) as mvalue

                from (
                    {y}
                )
                group by submtg_state_cd
            """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    v_table = {
            "tpl_prsn_mn_prsn": tpl_prsn_mn_prsn,
            "tpl_prsn_mn_cvrg": tpl_prsn_mn_cvrg,
            "tpl_prsn_hi_cvrg": tpl_prsn_hi_cvrg,
            "tpl_prsn_hi_insrnc": tpl_prsn_hi_insrnc,
            "ever_tpl_elig_prsn_mn_sql": ever_tpl_elig_prsn_mn_sql
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