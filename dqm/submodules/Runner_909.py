# --------------------------------------------------------------------
#
#
#
# --------------------------------------------------------------------
from dqm.DQMeasures import DQMeasures
class Runner_909():

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def run(spark, dqm: DQMeasures, measures: list=None) :
        dqm.logger.info('Module().run<series>.run() has been deprecated. Use dqm.run(spark) or dqm.run(spark, dqm.where(series="<series>")) instead.')
        if measures is None:
            return dqm.run(spark, dqm.reverse_measure_lookup[dqm.reverse_measure_lookup['series'] == '909']['measure_id'].tolist())
        else:
            return dqm.run(spark, measures)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def mcr28_1(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ids AS

                SELECT DISTINCT state_plan_id_num
                FROM {dqm.taskprefix}_tmsis_mc_mn_data
                WHERE (state_plan_id_num IS NOT NULL)
                    AND NOT (
                        mc_plan_type_cd IN (
                            '02'
                            ,'03'
                            )
                        )
            """
        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_enrollees AS

                SELECT mc_plan_id
                    ,count(1) AS enrollment
                FROM (
                    SELECT DISTINCT mc_plan_id
                        ,msis_ident_num
                    FROM {dqm.taskprefix}_tmsis_mc_prtcptn_data
                    WHERE mc_plan_id IS NOT NULL
                        AND msis_ident_num IS NOT NULL
                    ) a
                GROUP BY mc_plan_id
            """
        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_cap AS

                SELECT plan_id_num
                    ,count(1) AS capitation
                FROM {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
                WHERE plan_id_num IS NOT NULL
                    AND mdcd_pd_amt > 0
                    AND clm_type_cd IN (
                        '2'
                        ,'B'
                        )
                    AND adjstmt_ind = '0'
                GROUP BY plan_id_num
            """
        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                CREATE
                    OR replace TEMPORARY VIEW {dqm.taskprefix}_full AS

                SELECT a.state_plan_id_num
                    ,coalesce(b.enrollment, 0) AS enrollment
                    ,coalesce(c.capitation, 0) AS capitation
                    ,CASE
                        WHEN coalesce(b.enrollment, 0) <> 0
                            THEN coalesce(c.capitation, 0) / coalesce(b.enrollment, 0)
                        ELSE NULL
                        END AS capitation_ratio
                FROM {dqm.taskprefix}_ids a
                LEFT JOIN {dqm.taskprefix}_enrollees b ON a.state_plan_id_num = b.mc_plan_id
                LEFT JOIN {dqm.taskprefix}_cap c ON a.state_plan_id_num = c.plan_id_num
            """
        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                SELECT '{dqm.state}' AS submtg_state_cd
                    ,'MCR28_1' AS measure_id
                    ,'909' AS submodule
                    ,coalesce(numer, 0) AS numer
                    ,coalesce(denom, 0) AS denom
                    ,CASE
                        WHEN coalesce(denom, 0) <> 0
                            THEN numer / denom
                        ELSE NULL
                        END AS mvalue
                    ,NULL AS valid_value
                FROM (
                    SELECT count(1) AS denom
                        ,sum(CASE
                                WHEN capitation_ratio < 0.9
                                    OR capitation_ratio > 1.1
                                    THEN 1
                                ELSE 0
                                END) AS numer
                    FROM {dqm.taskprefix}_full
                    ) a
            """
        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #  FTX measures
    #
    # --------------------------------------------------------------------
    def ftx_mcr28_1(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ftx_ids AS

                SELECT DISTINCT state_plan_id_num
                FROM {dqm.taskprefix}_tmsis_mc_mn_data
                WHERE (state_plan_id_num IS NOT NULL)
                    AND NOT (
                        mc_plan_type_cd IN (
                            '02'
                            ,'03'
                            )
                        )
            """
        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ftx_enrollees AS

                SELECT mc_plan_id
                    ,count(1) AS enrollment
                FROM (
                    SELECT DISTINCT mc_plan_id
                        ,msis_ident_num
                    FROM {dqm.taskprefix}_tmsis_mc_prtcptn_data
                    WHERE mc_plan_id IS NOT NULL
                        AND msis_ident_num IS NOT NULL
                    ) a
                GROUP BY mc_plan_id
            """
        dqm.logger.debug(z)
        spark.sql(z)
        
        # Dedup across FTX tables
        de_dup_vars=f"""submtg_state_cd
                        ,orgnl_clm_num
                        ,adjstmt_clm_num
                        ,pymt_or_rcpmt_dt
                        ,adjstmt_ind
                    """
                    
        prep_query = f"""
                        SELECT distinct 
                               {de_dup_vars}
                               ,pyee_id
                        FROM (  SELECT {de_dup_vars}
                                       ,pyee_id
                                FROM {dqm.taskprefix}_tmsis_indvdl_cptatn_pmpm
                                WHERE pyee_id IS NOT NULL and
                                        pyee_id_type = '02' and
                                        pymt_or_rcpmt_amt > 0 and                      
                                        adjstmt_ind = '0'

                                UNION ALL
                                
                                SELECT {de_dup_vars}
                                       ,pyee_id
                                FROM {dqm.taskprefix}_tmsis_indvdl_hi_prm_pymt
                                WHERE pyee_id IS NOT NULL and
                                        pyee_id_type = '02' and
                                        pymt_or_rcpmt_amt > 0 and                 
                                        adjstmt_ind = '0'

                                UNION ALL
                                
                                SELECT {de_dup_vars}
                                       ,pyee_id
                                FROM {dqm.taskprefix}_tmsis_cst_shrng_ofst
                                WHERE pyee_id IS NOT NULL and
                                        pyee_id_type = '02' and
                                        pymt_or_rcpmt_amt > 0 and                 
                                        adjstmt_ind = '0' and
                                        ofst_trans_type <> '3'
                        )
        """

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ftx_cap AS

                SELECT pyee_id
                    ,count(1) AS capitation
                FROM ({prep_query})
                GROUP BY pyee_id
            """
        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                CREATE
                    OR replace TEMPORARY VIEW {dqm.taskprefix}_ftx_full AS

                SELECT a.state_plan_id_num
                    ,coalesce(b.enrollment, 0) AS enrollment
                    ,coalesce(c.capitation, 0) AS capitation
                    ,CASE
                        WHEN coalesce(b.enrollment, 0) <> 0
                            THEN coalesce(c.capitation, 0) / coalesce(b.enrollment, 0)
                        ELSE NULL
                        END AS capitation_ratio
                FROM {dqm.taskprefix}_ftx_ids a
                LEFT JOIN {dqm.taskprefix}_ftx_enrollees b ON a.state_plan_id_num = b.mc_plan_id
                LEFT JOIN {dqm.taskprefix}_ftx_cap c ON a.state_plan_id_num = c.pyee_id
            """
        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                SELECT '{dqm.state}' AS submtg_state_cd
                    ,'MCR28_1' AS measure_id
                    ,'909' AS submodule
                    ,coalesce(numer, 0) AS numer
                    ,coalesce(denom, 0) AS denom
                    ,CASE
                        WHEN coalesce(denom, 0) <> 0
                            THEN numer / denom
                        ELSE NULL
                        END AS mvalue
                    ,NULL AS valid_value
                FROM (
                    SELECT count(1) AS denom
                        ,sum(CASE
                                WHEN capitation_ratio < 0.9
                                    OR capitation_ratio > 1.1
                                    THEN 1
                                ELSE 0
                                END) AS numer
                    FROM {dqm.taskprefix}_ftx_full
                    ) a
            """
        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    # --------------------------------------------------------------------
    def summcr27(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                SELECT '{dqm.state}' AS submtg_state_cd
                    ,'SUMMCR_27' AS measure_id
                    ,'909' AS submodule
                    ,NULL AS numer
                    ,NULL AS denom
                    ,coalesce(count(DISTINCT state_plan_id_num), 0) AS mvalue
                    ,NULL AS valid_value
                FROM {dqm.taskprefix}_tmsis_mc_mn_data
            """
        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    # MCR65.x measures capture % of MANAGED-CARE-PLAN-TYPE enrollees with 
    # no capitation payments for that plan type
    # --------------------------------------------------------------------
    def mcr65(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view mcr65_denom as
                select distinct 
                    msis_ident_num
                   ,mc_plan_id
                from
                    {dqm.taskprefix}_tmsis_mc_prtcptn_data
                where
                    mc_plan_type_cd in {x['plan_type']}
                    and msis_ident_num is not null
                    and mc_plan_id is not null
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view mcr65_numer as
                select
                    p.msis_ident_num
                   ,max(case when h.msis_ident_num is null then 1 else 0 end) as numer_flag
                from
                    mcr65_denom as p
                left join
                    {DQMeasures.getBaseTable(dqm, 'clh', 'ot')} as h
                    on p.mc_plan_id = h.plan_id_num
                    and p.msis_ident_num = h.msis_ident_num
                    and h.clm_type_cd in ('2','B')
                group by p.msis_ident_num
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'909' as submodule
                    ,coalesce(sum(numer_flag), 0) as numer
                    ,count(*) as denom
                    ,case when count(*) > 0 then (coalesce(sum(numer_flag), 0) / count(*)) else null end as mvalue
                from
                    mcr65_numer
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    # FTX
    # MCR65.1-8 and 10-12 measures capture % of MANAGED-CARE-PLAN-TYPE enrollees with 
    # no capitation payments for that plan type
    # --------------------------------------------------------------------
    def ftx_mcr65(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view mcr65_ftx_denom as
                select distinct 
                    msis_ident_num
                   ,mc_plan_id
                from
                    {dqm.taskprefix}_tmsis_mc_prtcptn_data
                where
                    mc_plan_type_cd in {x['plan_type']}
                    and msis_ident_num is not null
                    and mc_plan_id is not null
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # No need to dedup across FTX tables as no counts or payment amount is calculated
        z = f"""
                create or replace temporary view mcr65_ftx_numer as
                select
                    p.msis_ident_num
                   ,max(case when h.msis_ident_num is null then 1 else 0 end) as numer_flag
                from
                    mcr65_ftx_denom as p
                left join
                    (select pyee_id, msis_ident_num
                      from {dqm.taskprefix}_tmsis_indvdl_cptatn_pmpm
                      where pyee_id_type = '02' 

                      union all
                      
                      select pyee_id, msis_ident_num
                      from {dqm.taskprefix}_tmsis_indvdl_hi_prm_pymt
                      where pyee_id_type = '02'

                      union all
                      
                      select pyee_id, msis_ident_num
                      from {dqm.taskprefix}_tmsis_cst_shrng_ofst
                      where pyee_id_type = '02' and 
                            ofst_trans_type <> '3'
                    ) as h
                    on p.mc_plan_id = h.pyee_id
                    and p.msis_ident_num = h.msis_ident_num
                group by p.msis_ident_num
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'909' as submodule
                    ,coalesce(sum(numer_flag), 0) as numer
                    ,count(*) as denom
                    ,case when count(*) > 0 then (coalesce(sum(numer_flag), 0) / count(*)) else null end as mvalue
                from
                    mcr65_ftx_numer
             """

        dqm.logger.debug(z)

        return spark.sql(z)
    


    # --------------------------------------------------------------------
    # FTX
    # MCR65.9 measures capture % of MANAGED-CARE-PLAN-TYPE enrollees with 
    # no capitation payments for that plan type
    # --------------------------------------------------------------------
    def ftx_mcr65_9(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view mcr65_ftx_denom as
                select distinct 
                    msis_ident_num
                   ,mc_plan_id
                from
                    {dqm.taskprefix}_tmsis_mc_prtcptn_data
                where
                    mc_plan_type_cd in {x['plan_type']}
                    and msis_ident_num is not null
                    and mc_plan_id is not null
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # No need to dedup across FTX tables as no counts or payment amount is calculated
        z = f"""
                create or replace temporary view mcr65_ftx_numer as
                select
                    p.msis_ident_num
                   ,max(case when h.msis_ident_num is null then 1 else 0 end) as numer_flag
                from
                    mcr65_ftx_denom as p
                left join
                    (select pyee_id, msis_ident_num
                      from {dqm.taskprefix}_tmsis_indvdl_cptatn_pmpm
                      where pyee_id_type in ('02','05','06')

                      union all
                      
                      select pyee_id, msis_ident_num
                      from {dqm.taskprefix}_tmsis_indvdl_hi_prm_pymt
                      where pyee_id_type in ('02','05','06')

                      union all
                      
                      select pyee_id, msis_ident_num
                      from {dqm.taskprefix}_tmsis_cst_shrng_ofst
                      where pyee_id_type in ('02','05','06') and 
                            ofst_trans_type <> '3'
                    ) as h
                    on p.mc_plan_id = h.pyee_id
                    and p.msis_ident_num = h.msis_ident_num
                group by p.msis_ident_num
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'909' as submodule
                    ,coalesce(sum(numer_flag), 0) as numer
                    ,count(*) as denom
                    ,case when count(*) > 0 then (coalesce(sum(numer_flag), 0) / count(*)) else null end as mvalue
                from
                    mcr65_ftx_numer
             """

        dqm.logger.debug(z)

        return spark.sql(z)
    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    v_table = { 'mcr28_1': mcr28_1,
                'ftx_mcr28_1': ftx_mcr28_1,
                'summcr27': summcr27,
                'mcr65': mcr65,
                'ftx_mcr65': ftx_mcr65, 
                'ftx_mcr65_9': ftx_mcr65_9 }

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