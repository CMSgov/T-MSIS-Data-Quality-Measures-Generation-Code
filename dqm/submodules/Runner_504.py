# --------------------------------------------------------------------
#
#
#
# --------------------------------------------------------------------
from dqm.DQClosure import DQClosure
from dqm.DQMeasures import DQMeasures


class Runner_504:

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def run(spark, dqm: DQMeasures, measures: list = None) :
        dqm.logger.info('Module().run<series>.run() has been deprecated. Use dqm.run(spark) or dqm.run(spark, dqm.where(series="<series>")) instead.')
        if measures is None:
            return dqm.run(spark, dqm.reverse_measure_lookup[dqm.reverse_measure_lookup['series'] == '504']['measure_id'].tolist())
        else:
            return dqm.run(spark, measures)

    # --------------------------------------------------------------------
    #
    #   % of Submitting State Provider IDs with
    #   FACILITY-GROUP-INDIVIDUAL-CODE = 01, 02 (facility or group)
    #   that do not have a Provider Classification Code
    #   that indicates a facility or group
    #
    # --------------------------------------------------------------------
    def prv6_1(spark, dqm: DQMeasures, measure_id, x):

        # Unique tmsis_prvdr_attr_mn.submtg_state_prvdr_id

        # WHERE
        #   tmsis_prvdr_attr_mn_Constraints
        # AND
        #   tmsis_prvdr_attr_mn.fac_grp_indvdl_cd = "01" or "02"

        z = f"""
                create or replace temporary view prv6_1_denom as
                select
                    submtg_state_cd,
                    count(distinct submtg_state_prvdr_id) as prv6_1_denom
                from
                    {dqm.taskprefix}_tmsis_prvdr_attr_mn
                where
                    fac_grp_indvdl_cd in ('01','02')
                group by
                    submtg_state_cd
            """

        dqm.logger.debug(z)
        spark.sql(z)

        prv6_1_numer_base = f"""
                select distinct
                    p.submtg_state_cd,
                    p.submtg_state_prvdr_id
                from
                    {dqm.taskprefix}_tmsis_prvdr_attr_mn as p
                inner join
                    {dqm.taskprefix}_tmsis_prvdr_txnmy_clsfctn as t
                    on p.submtg_state_prvdr_id = t.submtg_state_prvdr_id
                    and p.fac_grp_indvdl_cd in ('01','02')
            """.format()

        # (
        #  (
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_type_cd= PROV-CLASSIFICATION-TYPE in Provider Classification Lookup
        #   AND
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_cd= Code in Provider Classification Lookup
        #   AND
        #   Designation = "Individual" in Provider Classification Lookup
        #  )

        # OR
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_type_cd <> PROV-CLASSIFICATION-TYPE in Provider Classification Lookup
        # OR
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_cd <> Code in Provider Classification Lookup
        # OR
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_type_cd is NULL
        # OR
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_cd is NULL
        # )

        z = f"""
                create or replace temporary view prv6_1_numer as
                select
                    '{dqm.state}' as submtg_state_cd,
                    count(distinct submtg_state_prvdr_id) as prv6_1_numer
                from (
                    {prv6_1_numer_base}
                        left join
                            dqm_conv.provider_classification_lookup as l
                                on (t.prvdr_clsfctn_type_cd = l.prov_class_type
                                    and t.prvdr_clsfctn_cd = l.Code)
                        where
                            l.Designation = 'Individual' or l.Designation is null
                    )
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'504' as submodule
                    ,coalesce(prv6_1_numer, 0) as numer
                    ,prv6_1_denom as denom
                    ,case when prv6_1_denom > 0 then (coalesce(prv6_1_numer, 0) / prv6_1_denom) else null end as mvalue
                from
                    prv6_1_denom as d
                left join
                    prv6_1_numer as n
                        on d.submtg_state_cd = n.submtg_state_cd
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #   % of Submitting State Provider IDs with
    #   FACILITY-GROUP-INDIVIDUAL-CODE = 03 (individual)
    #   that do not have a Provider Classification Code
    #   that indicates an individual
    #
    # --------------------------------------------------------------------
    def prv6_2(spark, dqm: DQMeasures, measure_id, x):

        # Unique tmsis_prvdr_attr_mn.submtg_state_prvdr_id

        # WHERE
        #   tmsis_prvdr_attr_mn_Constraints
        # AND
        #   tmsis_prvdr_attr_mn.fac_grp_indvdl_cd= "03"

        z = f"""
                create or replace temporary view prv6_2_denom as
                select
                    submtg_state_cd,
                    count(distinct submtg_state_prvdr_id) as prv6_2_denom
                from
                    {dqm.taskprefix}_tmsis_prvdr_attr_mn
                where
                    fac_grp_indvdl_cd in ('03')
                group by
                    submtg_state_cd
            """

        dqm.logger.debug(z)
        spark.sql(z)

        prv6_2_numer_base = f"""
                select distinct
                    p.submtg_state_cd,
                    p.submtg_state_prvdr_id
                from
                    {dqm.taskprefix}_tmsis_prvdr_attr_mn as p
                inner join
                    {dqm.taskprefix}_tmsis_prvdr_txnmy_clsfctn as t
                    on p.submtg_state_prvdr_id = t.submtg_state_prvdr_id
                    and p.fac_grp_indvdl_cd in ('03')
            """.format()

        # (
        #  (
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_type_cd = PROV-CLASSIFICATION-TYPE in Provider Classification Lookup
        #   AND
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_cd= Code in Provider Classification Lookup
        #   AND
        #   Designation = "Non-Individual" in Provider Classification Lookup
        #  )

        # OR
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_type_cd <> PROV-CLASSIFICATION-TYPE in Provider Classification Lookup
        # OR
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_cd <> Code in Provider Classification Lookup
        # OR
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_type_cd is NULL
        # OR
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_cd is NULL
        # )

        z = f"""
                create or replace temporary view prv6_2_numer as
                select
                    '{dqm.state}' as submtg_state_cd,
                    count(distinct submtg_state_prvdr_id) as prv6_2_numer
                from (
                    {prv6_2_numer_base}
                        left join
                            dqm_conv.provider_classification_lookup as l
                                on (t.prvdr_clsfctn_type_cd = l.prov_class_type
                                    and t.prvdr_clsfctn_cd = l.Code)
                        where
                            l.Designation = 'Non-Individual' or l.Designation is null
                    )
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'504' as submodule
                    ,coalesce(prv6_2_numer, 0) as numer
                    ,prv6_2_denom as denom
                    ,case when prv6_2_denom > 0 then (coalesce(prv6_2_numer, 0) / prv6_2_denom) else null end as mvalue
                from
                    prv6_2_denom as d
                left join
                    prv6_2_numer as n
                        on d.submtg_state_cd = n.submtg_state_cd
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #   % of Submitting State Provider IDs with
    #   FACILITY-GROUP-INDIVIDUAL-CODE = 01, 02 (facility or group)
    #   that are missing Provider Classification Code
    #
    # --------------------------------------------------------------------
    def prv6_3(spark, dqm: DQMeasures, measure_id, x):

        # Unique tmsis_prvdr_attr_mn.submtg_state_prvdr_id
        # WHERE
        #   tmsis_prvdr_attr_mn_Constraints
        # AND
        #   tmsis_prvdr_attr_mn.fac_grp_indvdl_cd= "01" or "02"

        z = f"""
                create or replace temporary view prv6_3_denom as
                select
                    submtg_state_cd,
                    count(distinct submtg_state_prvdr_id) as prv6_3_denom
                from
                    {dqm.taskprefix}_tmsis_prvdr_attr_mn
                where
                    fac_grp_indvdl_cd in ('01','02')
                group by
                    submtg_state_cd
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # Denominator Definition
        # AND
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_cd does not contain (any digit 1-9)
        # OR
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_cd is NULL
        # WHERE
        #   tmsis_prvdr_txnmy_clsfctn _Constraints

        z = f"""
                create or replace temporary view prv6_3_numer as
                select
                    p.submtg_state_cd,
                    count(distinct p.submtg_state_prvdr_id) as prv6_3_numer
                from
                    {dqm.taskprefix}_tmsis_prvdr_attr_mn as p
                inner join
                    {dqm.taskprefix}_tmsis_prvdr_txnmy_clsfctn as t
                    on
                        p.submtg_state_prvdr_id = t.submtg_state_prvdr_id
                        and p.fac_grp_indvdl_cd in ('01','02')
                        and ((t.prvdr_clsfctn_cd not rlike '[1-9]') or (t.prvdr_clsfctn_cd is null))
                group by
                    p.submtg_state_cd
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'504' as submodule
                    ,coalesce(prv6_3_numer, 0) as numer
                    ,prv6_3_denom as denom
                    ,case when prv6_3_denom > 0 then (coalesce(prv6_3_numer, 0) / prv6_3_denom) else null end as mvalue
                from
                    prv6_3_denom as d
                left join
                    prv6_3_numer as n
                        on d.submtg_state_cd = n.submtg_state_cd
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #   % of Submitting State Provider IDs with
    #   FACILITY-GROUP-INDIVIDUAL-CODE = 03 (individual)
    #   that are missing Provider Classification Code
    #
    # --------------------------------------------------------------------
    def prv6_4(spark, dqm: DQMeasures, measure_id, x):

        # Unique tmsis_prvdr_attr_mn.submtg_state_prvdr_id
        # WHERE
        #   tmsis_prvdr_attr_mn_Constraints
        # AND
        #   tmsis_prvdr_attr_mn.fac_grp_indvdl_cd= "03"

        z = f"""
                create or replace temporary view prv6_4_denom_view as
                select
                    submtg_state_cd,
                    count(distinct submtg_state_prvdr_id) as prv6_4_denom
                from
                    {dqm.taskprefix}_tmsis_prvdr_attr_mn
                where
                    fac_grp_indvdl_cd = '03'
                group by
                    submtg_state_cd
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # Denominator Definition
        # AND
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_cd does not contain (any digit 1-9)
        # OR
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_cd is NULL
        # WHERE
        #   tmsis_prvdr_txnmy_clsfctn _Constraints

        z = f"""
                create or replace temporary view prv6_4_numer as
                select
                    p.submtg_state_cd,
                    count(distinct p.submtg_state_prvdr_id) as prv6_4_numer
                from
                    {dqm.taskprefix}_tmsis_prvdr_attr_mn as p
                inner join
                    {dqm.taskprefix}_tmsis_prvdr_txnmy_clsfctn as t
                    on
                        p.submtg_state_prvdr_id = t.submtg_state_prvdr_id
                        and p.fac_grp_indvdl_cd = '03'
                        and ((t.prvdr_clsfctn_cd not rlike '[1-9]') or (t.prvdr_clsfctn_cd is null))
                group by
                    p.submtg_state_cd
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'504' as submodule
                    ,coalesce(prv6_4_numer, 0) as numer
                    ,prv6_4_denom as denom
                    ,case when prv6_4_denom > 0 then (coalesce(prv6_4_numer, 0) / prv6_4_denom) else null end as mvalue
                from
                    prv6_4_denom_view as d
                left join
                    prv6_4_numer as n
                        on d.submtg_state_cd = n.submtg_state_cd
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #   % of Submitting State Provider IDs that
    #   are not atypical providers 
    #   that are missing NPI
    #
    # --------------------------------------------------------------------
    def prv2_11(spark, dqm: DQMeasures, measure_id, x):

        # Unique tmsis_prvdr_attr_mn.submtg_state_prvdr_id WHERE
        # 
        #  (
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_type_cd = PROV-CLASSIFICATION-TYPE in Atypical Provider Lookup
        #   AND
        #   tmsis_prvdr_txnmy_clsfctn.prvdr_clsfctn_cd= Code in Atypical Provider Lookup
        #   AND
        #   NPI required = "YES" in Atypical Provider Lookup
        #  )
        
        z = f"""
                create or replace temporary view {dqm.taskprefix}_prv2_11_denom_prep as
                select distinct
                    p.submtg_state_cd,
                    p.submtg_state_prvdr_id

                from
                    {dqm.taskprefix}_tmsis_prvdr_attr_mn as p
                inner join
                    {dqm.taskprefix}_tmsis_prvdr_txnmy_clsfctn as t
                    on p.submtg_state_prvdr_id = t.submtg_state_prvdr_id

                inner join
                     dqm_conv.atypical_provider_table as l
                     on (t.prvdr_clsfctn_type_cd = l.prov_class_type
                         and t.prvdr_clsfctn_cd = l.prov_class_cd)
                     where
                     p.submtg_state_prvdr_id is not null and 
                     l.NPI_req = 'YES'
            """

        dqm.logger.debug(z)
        spark.sql(z)


        # flag providers with NPI : (tmsis_prvdr_id.prvdr_id_type_cd ='2' and tmsis_prvdr_id.prvdr_id rlike '[a-zA-Z1-9]')

        z = f"""
                create or replace temporary view prv2_11_numer_prep as
                select
                    d.submtg_state_cd,
                    d.submtg_state_prvdr_id,
                    
                    max(case when n.prvdr_id_type_cd ='2' and 
                                  {DQClosure.parse('%nmsng(n.prvdr_id, 12)')}
                             then 1 else 0 end
                        ) as  prv_with_npi             

                from {dqm.taskprefix}_prv2_11_denom_prep d
                
                left join
                     {dqm.taskprefix}_tmsis_prvdr_id n

                on (d.submtg_state_prvdr_id = n.submtg_state_prvdr_id)
                       
                group by d.submtg_state_cd, d.submtg_state_prvdr_id    
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # Count providers with no NPI

        prv2_11_rollup = f"""
                select
                    submtg_state_cd
                    ,sum(1) as prv2_11_denom
                    ,sum(case when prv_with_npi=0 then 1 else 0 end) as prv2_11_numer
                 from
                    prv2_11_numer_prep
                 
                 group by submtg_state_cd

             """.format()
     
     
        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'504' as submodule
                    ,coalesce(prv2_11_numer, 0) as numer
                    ,prv2_11_denom as denom
                    ,case when prv2_11_denom > 0 then (coalesce(prv2_11_numer, 0) / prv2_11_denom) else null end as mvalue
                from ({prv2_11_rollup}) a
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    v_table = {
            "prv6_1": prv6_1,
            "prv6_2": prv6_2,
            "prv6_3": prv6_3,
            "prv6_4": prv6_4,
            "prv2_11": prv2_11
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
