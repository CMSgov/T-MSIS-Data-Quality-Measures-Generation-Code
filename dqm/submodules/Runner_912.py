# --------------------------------------------------------------------
#
#
#
# --------------------------------------------------------------------
from dqm.DQPrepETL import DQPrepETL
from dqm.DQM_Metadata import DQM_Metadata
from dqm.DQMeasures import DQMeasures

class Runner_912():

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def run(spark, dqm: DQMeasures, measures: list=None) :
        dqm.logger.info('Module().run<series>.run() has been deprecated. Use dqm.run(spark) or dqm.run(spark, dqm.where(series="<series>")) instead.')
        if measures is None:
            return dqm.run(spark, dqm.reverse_measure_lookup[dqm.reverse_measure_lookup['series'] == '912']['measure_id'].tolist())
        else:
            return dqm.run(spark, measures)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def extract_claims(spark, dqm: DQMeasures, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_clms_{x['claim_cat']} AS

                SELECT msis_ident_num
                    ,plan_id_num
                    ,blg_prvdr_num
                    ,blg_prvdr_npi_num
                    ,case when stc_cd in ('119') then 1 else 0 end as mco_flag
                    ,case when stc_cd in ('120') then 1 else 0 end as pccm_flag
                    ,case when stc_cd in ('122') then 1 else 0 end as php_flag

                FROM {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
                WHERE plan_id_num IS NOT NULL
                    AND {DQM_Metadata.create_base_clh_view().claim_cat[x['claim_cat']]}
                    AND childless_header_flag = 0
                    AND stc_cd in ('119','120','122')
             """
        dqm.logger.debug(z)
        spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def PCCM_9_18_13_18(spark, dqm: DQMeasures, measure_id, x) :

        # DQPrepETL.run_912_other_measures_PCCM(dqm)

        Runner_912.extract_claims(spark, dqm, x)

        z = f"""
                CREATE
                    OR replace TEMPORARY VIEW {dqm.taskprefix}_denom AS

                SELECT a.*
                    ,1 AS has_denom_line
                    ,CASE
                        WHEN (
                                (
                                    a.msis_ident_num = b.msis_ident_num
                                    AND a.plan_id_num = b.mc_plan_id
                                    AND b.evr_pccm_mc_plan_type = 0
                                    )
                                OR (
                                    a.plan_id_num IS NOT NULL
                                    AND
                                    b.msis_ident_num IS NULL
                                    AND b.mc_plan_id IS NULL
                                    )
                                )
                            THEN 1
                        ELSE 0
                        END AS has_numer_line
                FROM (select * from {dqm.taskprefix}_clms_{x['claim_cat']} where pccm_flag=1) AS a
                LEFT JOIN {dqm.taskprefix}_mc_data_912 AS b ON (
                        a.msis_ident_num = b.msis_ident_num
                        AND a.plan_id_num = b.mc_plan_id
                        )
             """
        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                SELECT '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'912' AS submodule
                    ,coalesce(numer, 0) AS numer
                    ,coalesce(denom, 0) AS denom
                    ,CASE
                        WHEN coalesce(denom, 0) <> 0
                            THEN numer / denom
                        ELSE NULL
                        END AS mvalue
                    ,NULL AS valid_value
                FROM (
                    SELECT sum(CASE
                                WHEN has_denom_line = 1
                                    THEN 1
                                ELSE 0
                                END) AS denom
                        ,sum(CASE
                                WHEN has_numer_line = 1
                                    THEN 1
                                ELSE 0
                                END) AS numer
                    FROM {dqm.taskprefix}_denom
                    ) a
             """
        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def PCCM_9_19_13_19(spark, dqm: DQMeasures, measure_id, x) :

        # DQPrepETL.run_912_other_measures_PCCM(dqm)

        Runner_912.extract_claims(spark, dqm, x)

        z = f"""
                CREATE
                    OR replace TEMPORARY VIEW {dqm.taskprefix}_denom AS

                SELECT *
                    ,1 AS has_denom_line
                    ,CASE
                        WHEN (
                                (plan_id_num IS NOT NULL)
                                AND (
                                    plan_id_num = blg_prvdr_num
                                    OR plan_id_num = blg_prvdr_npi_num
                                    )
                                )
                            THEN 1
                        ELSE 0
                        END AS has_numer_line
                FROM {dqm.taskprefix}_clms_{x['claim_cat']}
                where pccm_flag=1
             """
        dqm.logger.debug(z)
        spark.sql(z)

        z =f"""
                SELECT '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'912' AS submodule
                    ,coalesce(numer, 0) AS numer
                    ,coalesce(denom, 0) AS denom
                    ,CASE
                        WHEN coalesce(denom, 0) <> 0
                            THEN numer / denom
                        ELSE NULL
                        END AS mvalue
                    ,NULL AS valid_value
                FROM (
                    SELECT sum(CASE
                                WHEN has_denom_line = 1
                                    THEN 1
                                ELSE 0
                                END) AS denom
                        ,sum(CASE
                                WHEN has_numer_line = 1
                                    THEN 1
                                ELSE 0
                                END) AS numer
                    FROM {dqm.taskprefix}_denom
                    ) a
            """
        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def PHP_9_20_13_20(spark, dqm: DQMeasures, measure_id, x) :

        # DQPrepETL.run_912_other_measures_PCCM(dqm)

        Runner_912.extract_claims(spark, dqm, x)

        z = f"""
                CREATE
                    OR replace TEMPORARY VIEW {dqm.taskprefix}_php_denom AS

                SELECT a.*
                    ,1 AS has_denom_line
                    ,CASE
                        WHEN (
                                (
                                    a.msis_ident_num = b.msis_ident_num
                                    AND a.plan_id_num = b.mc_plan_id
                                    AND b.evr_php_mc_plan_type = 0
                                    )
                                OR (
                                    a.plan_id_num IS NOT NULL
                                    AND
                                    b.msis_ident_num IS NULL
                                    AND b.mc_plan_id IS NULL
                                    )
                                )
                            THEN 1
                        ELSE 0
                        END AS has_numer_line
                FROM (select * from {dqm.taskprefix}_clms_{x['claim_cat']} where php_flag=1) AS a
                LEFT JOIN {dqm.taskprefix}_mc_data_912 AS b ON (
                        a.msis_ident_num = b.msis_ident_num
                        AND a.plan_id_num = b.mc_plan_id
                        )
             """
        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                SELECT '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'912' AS submodule
                    ,coalesce(numer, 0) AS numer
                    ,coalesce(denom, 0) AS denom
                    ,CASE
                        WHEN coalesce(denom, 0) <> 0
                            THEN numer / denom
                        ELSE NULL
                        END AS mvalue
                    ,NULL AS valid_value
                FROM (
                    SELECT sum(CASE
                                WHEN has_denom_line = 1
                                    THEN 1
                                ELSE 0
                                END) AS denom
                        ,sum(CASE
                                WHEN has_numer_line = 1
                                    THEN 1
                                ELSE 0
                                END) AS numer
                    FROM {dqm.taskprefix}_php_denom
                    ) a
             """
        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def MCO_9_21_13_21(spark, dqm: DQMeasures, measure_id, x) :

        # DQPrepETL.run_912_other_measures_PCCM(dqm)

        Runner_912.extract_claims(spark, dqm, x)

        z = f"""
                CREATE
                    OR replace TEMPORARY VIEW {dqm.taskprefix}_mco_denom AS

                SELECT a.*
                    ,1 AS has_denom_line
                    ,CASE
                        WHEN (
                                (
                                    a.msis_ident_num = b.msis_ident_num
                                    AND a.plan_id_num = b.mc_plan_id
                                    AND b.evr_mco_mc_plan_type = 0
                                    )
                                OR (
                                    a.plan_id_num IS NOT NULL
                                    AND
                                    b.msis_ident_num IS NULL
                                    AND b.mc_plan_id IS NULL
                                    )
                                )
                            THEN 1
                        ELSE 0
                        END AS has_numer_line
                FROM (select * from {dqm.taskprefix}_clms_{x['claim_cat']} where mco_flag=1) AS a
                LEFT JOIN {dqm.taskprefix}_mc_data_912 AS b ON (
                        a.msis_ident_num = b.msis_ident_num
                        AND a.plan_id_num = b.mc_plan_id
                        )
             """
        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                SELECT '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'912' AS submodule
                    ,coalesce(numer, 0) AS numer
                    ,coalesce(denom, 0) AS denom
                    ,CASE
                        WHEN coalesce(denom, 0) <> 0
                            THEN numer / denom
                        ELSE NULL
                        END AS mvalue
                    ,NULL AS valid_value
                FROM (
                    SELECT sum(CASE
                                WHEN has_denom_line = 1
                                    THEN 1
                                ELSE 0
                                END) AS denom
                        ,sum(CASE
                                WHEN has_numer_line = 1
                                    THEN 1
                                ELSE 0
                                END) AS numer
                    FROM {dqm.taskprefix}_mco_denom
                    ) a
             """
        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    v_table = { 'PCCM_9_18_13_18' : PCCM_9_18_13_18,
                'PCCM_9_19_13_19' : PCCM_9_19_13_19,
                'PHP_9_20_13_20'  : PHP_9_20_13_20,
                'MCO_9_21_13_21'  : MCO_9_21_13_21 }

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