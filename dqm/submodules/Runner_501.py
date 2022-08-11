# --------------------------------------------------------------------
#
#
#
# --------------------------------------------------------------------
from dqm.DQClosure import DQClosure
from dqm.DQMeasures import DQMeasures

class Runner_501:

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def run(spark, dqm: DQMeasures, measures: list = None) :
        dqm.logger.info('Module().run<series>.run() has been deprecated. Use dqm.run(spark) or dqm.run(spark, dqm.where(series="<series>")) instead.')
        if measures is None:
            return dqm.run(spark, dqm.reverse_measure_lookup[dqm.reverse_measure_lookup['series'] == '501']['measure_id'].tolist())
        else:
            return dqm.run(spark, measures)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def tot_match_loc(spark, dqm: DQMeasures, measure_id, x) :

        # merge the provider and claims and flag the records where location match
        z = f"""
                create or replace temporary view {dqm.taskprefix}_clm_prv_{x['claim_type']} as
                select   a.submtg_state_cd
                        ,a.blg_prvdr_num as prvdr_id
                        ,sum(case when (a.prvdr_lctn_id = b.prvdr_lctn_id) then 1 else 0 end) as tot_match_loc_cnt
                        ,count(a.prvdr_lctn_id) as tot_loc_cnt
                        ,avg(case when (a.prvdr_lctn_id = b.prvdr_lctn_id) then 1 else 0 end) as pct_match_loc
                from {dqm.taskprefix}_uniq_clms_prvdrs_file a
                left join {dqm.taskprefix}_prvdr_loc_prep b
                on  a.submtg_state_cd = b.submtg_state_cd and
                    a.blg_prvdr_num = b.prvdr_id and
                    a.prvdr_lctn_id = b.prvdr_lctn_id
                where a.sourcefile = '{x['claim_type']}'
                group by a.submtg_state_cd, a.blg_prvdr_num
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_clm_prv_tab_{x['claim_type']} as
        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'501' as submodule
                    sum(tot_match_loc_cnt) as numer,
                    sum(tot_loc_cnt)  as denom,
                    round((sum(tot_match_loc_cnt)/sum(tot_loc_cnt)), 2) as mvalue
                from {dqm.taskprefix}_clm_prv_{x['claim_type']}
                group by submtg_state_cd
            """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def pct_match_loc(spark, dqm: DQMeasures, measure_id, x) :

        # merge the provider and claims and flag the records where location match
        z = f"""
                create or replace temporary view {dqm.taskprefix}_clm_prv_{x['claim_type']} as
                select   a.submtg_state_cd
                        ,a.blg_prvdr_num as prvdr_id
                        ,sum(case when (a.prvdr_lctn_id = b.prvdr_lctn_id) then 1 else 0 end) as tot_match_loc_cnt
                        ,count(a.prvdr_lctn_id) as tot_loc_cnt
                        ,avg(case when (a.prvdr_lctn_id = b.prvdr_lctn_id) then 1 else 0 end) as pct_match_loc
                from {dqm.taskprefix}_uniq_clms_prvdrs_file a
                left join {dqm.taskprefix}_prvdr_prep b
                on  a.submtg_state_cd = b.submtg_state_cd and
                    a.blg_prvdr_num = b.prvdr_id and
                    a.prvdr_lctn_id = b.prvdr_lctn_id
                where a.sourcefile = '{x['claim_type']}'
                group by a.submtg_state_cd, a.blg_prvdr_num
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_clm_prv_tab_{x['claim_type']} as
        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'501' as submodule

                    sum(case when pct_match_loc = 1 then 1 else 0 end) as numer,
                    count(prvdr_id) as denom,
                    round((sum(case when pct_match_loc = 1 then 1 else 0 end)
                                /count(prvdr_id)), 2) as mvalue

                from {dqm.taskprefix}_clm_prv_{x['claim_type']}
                group by submtg_state_cd
            """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def prv_addtyp(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prv_addtyp_prep as
                select submtg_state_cd,
                    submtg_state_prvdr_id as prvdr_id,
                    case when ({x['numer']}) then 1 else 0 end as prv_
                from {dqm.taskprefix}_tmsis_prvdr_lctn_cntct
                where {DQClosure.parse('%nmsng(submtg_state_prvdr_id,30)')}
             """

        dqm.logger.debug(z)

        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prv_addtyp_rollup as
                select submtg_state_cd,
                    prvdr_id,
                    max(prv_) as prv_
                from {dqm.taskprefix}_prv_addtyp_prep
                group by submtg_state_cd, prvdr_id
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_prv_addtyp as
        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'501' as submodule
                    sum (prv_) as numer,
                    count(submtg_state_cd) as denom,
                    round((sum(prv_) / count(submtg_state_cd)), 2) as mvalue
                from {dqm.taskprefix}_prv_addtyp_rollup
                group by submtg_state_cd
            """

        dqm.logger.debug(z)
        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def prv_idtyp(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prv_idtyp_prep as
                select submtg_state_cd,
                    submtg_state_prvdr_id as prvdr_id
                    ,max(case when cast(coalesce(prvdr_id_type_cd,'9') as int) = {x['numer']} then 1 else 0 end) as prv2_{x['numer']}_1
                from {dqm.taskprefix}_tmsis_prvdr_id
                where submtg_state_prvdr_id is not null
                group by submtg_state_cd, submtg_state_prvdr_id
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_prv_idtyp as
        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'501' as submodule
                    ,sum(prv2_{x['numer']}_1) as numer
                    ,count(submtg_state_cd) as denom
                    ,round((sum(prv2_{x['numer']}_1) / count(submtg_state_cd)),2) as mvalue
                from {dqm.taskprefix}_prv_idtyp_prep
                group by submtg_state_cd
            """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def prvdr_mdcd_enrlmt(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prv_mdcd_prep as
                select  submtg_state_cd
                    ,submtg_state_prvdr_id as prvdr_id
                    ,max(case when ({x['numer']}) then 1 else 0 end) as prv3_
                from {dqm.taskprefix}_tmsis_prvdr_mdcd_enrlmt
                where submtg_state_prvdr_id is not null
                group by submtg_state_cd, submtg_state_prvdr_id
            """

        dqm.logger.debug(z)

        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_prv_mdcd as
        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'501' as submodule
                    ,count(submtg_state_cd) as denom
                    ,sum(prv3_) as numer
                    ,round((sum(prv3_) / count(submtg_state_cd)), 2) as mvalue
                from {dqm.taskprefix}_prv_mdcd_prep
                group by submtg_state_cd
             """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def prv2_10(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prv2_10_denom as
                select
                    a.submtg_state_cd,
                    a.submtg_state_prvdr_id,
                    max(case when a.fac_grp_indvdl_cd = '03' and a.ever_provider = 1 then 1 else 0 end) as prv2_10_denom0
                from (
                    select
                        *
                    from
                        {dqm.taskprefix}_ever_tmsis_prvdr_attr_mn
                    where
                        submtg_state_prvdr_id is not null
                ) a
                group by
                    a.submtg_state_cd,
                    a.submtg_state_prvdr_id
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prv2_10_numer as
                select a.submtg_state_cd,
                    a.submtg_state_prvdr_id,
                    a.prv2_10_denom0,
                    case when a.prv2_10_denom0=1 and b.numer_count >= 2 then 1 else 0 end as prv2_10_numer0
                from
                    {dqm.taskprefix}_prv2_10_denom a
                left join (
                    select
                        submtg_state_cd,
                        submtg_state_prvdr_id,
                        count(distinct case when prvdr_id_type_cd = '2' and ever_provider_id = 1
                        and {DQClosure.parse('%nmsng(prvdr_id, 12)')} then prvdr_id else null end) as numer_count
                    from
                        {dqm.taskprefix}_ever_tmsis_prvdr_id
                    where
                        submtg_state_prvdr_id is not null
                    group by
                        submtg_state_cd,
                        submtg_state_prvdr_id
                ) b
                    on a.submtg_state_cd = b.submtg_state_cd and
                    a.submtg_state_prvdr_id = b.submtg_state_prvdr_id
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_prv2_10_msr as
        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'501' as submodule
                    ,prv2_10_numer as numer
                    ,prv2_10_denom as denom
                    ,case when prv2_10_denom >0 then round(prv2_10_numer / prv2_10_denom,2)
                    else null end as mvalue
                from
                    (select
                        submtg_state_cd
                        ,sum(prv2_10_numer0) as prv2_10_numer
                        ,sum(prv2_10_denom0) as prv2_10_denom
                    from {dqm.taskprefix}_prv2_10_numer
                    group by submtg_state_cd
                ) a
             """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def prvdr_pct(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prvdr_pct as
                select
                    a.submtg_state_cd
                    %do i = 1 %to 11;
                        ,prv1_&i._numer
                        ,prv1_&i._denom
                        ,prv1_&i
                    %end;
                        ,prv1_13_numer
                        ,prv1_13_denom
                        ,prv1_13
                        ,prv1_15_numer
                        ,prv1_15_denom
                        ,prv1_15
                    %do i = 1 %to 10;
                        ,prv2_&i._numer
                        ,prv2_&i._denom
                        ,prv2_&i
                    %end;
                    %do i = 1 %to 6;
                        ,prv3_&i._numer
                        ,prv3_&i._denom
                        ,prv3_&i
                    %end;
                from {dqm.taskprefix}_clm_prv_tab a
                left join {dqm.taskprefix}_clm_prv_tab_ip b on a.submtg_state_cd = b.submtg_state_cd
                left join {dqm.taskprefix}_clm_prv_tab_lt c on a.submtg_state_cd = c.submtg_state_cd
                left join {dqm.taskprefix}_clm_prv_tab_ot d on a.submtg_state_cd = d.submtg_state_cd
                left join {dqm.taskprefix}_clm_prv_tab_rx e on a.submtg_state_cd = e.submtg_state_cd
                left join {dqm.taskprefix}_prv_addtyp f on a.submtg_state_cd = f.submtg_state_cd
                left join {dqm.taskprefix}_prv_idtyp g on a.submtg_state_cd = g.submtg_state_cd
                left join {dqm.taskprefix}_prv_mdcd h on a.submtg_state_cd = h.submtg_state_cd
                left join {dqm.taskprefix}_prvdr_npi_txnmy2 i on a.submtg_state_cd = i.submtg_state_cd
                left join {dqm.taskprefix}_prv2_10_msr j on a.submtg_state_cd = j.submtg_state_cd

            """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def prv1_1(spark, dqm: DQMeasures, measure_id, x) :

        # create or replace temporary view {dqm.taskprefix}_clm_prv_tab as
        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'501' as submodule

                    sum(case when pct_match_loc = 1 then 1 else 0 end) as numer,
                    count(prvdr_id) as denom,
                    round((sum(case when pct_match_loc = 1 then 1 else 0 end)
                            /count(prvdr_id)), 2) as mvalue

                from {dqm.taskprefix}_prv_clm
                group by submtg_state_cd
            """

        dqm.logger.debug(z)
        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def prv1_2(spark, dqm: DQMeasures, measure_id, x) :

        # create or replace temporary view {dqm.taskprefix}_clm_prv_tab as
        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'501' as submodule

                    sum(tot_match_loc_cnt) as numer,
                    sum(tot_loc_cnt) as denom,
                    round(sum(tot_match_loc_cnt)/sum(tot_loc_cnt), 2) as mvalue

                from {dqm.taskprefix}_prv_clm
                group by submtg_state_cd
            """

        dqm.logger.debug(z)
        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def prvdr_npi_txnmy(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prv_id_npi as
                select submtg_state_cd,
                    submtg_state_prvdr_id as prvdr_id
                    ,max(case when prvdr_id_type_cd = '2' then 1 else 0 end) as prv2_9_denom0
                from
                    {dqm.taskprefix}_tmsis_prvdr_id
                where
                    submtg_state_prvdr_id is not null
                group by
                    submtg_state_cd, submtg_state_prvdr_id
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prvdr_npi_txnmy as
                select a.submtg_state_cd,
                        a.prvdr_id,
                        a.prv2_9_denom0,
                        case when a.prv2_9_denom0=1 and
                            (b.prvdr_clsfctn_type_eq1 = 0 or b.prvdr_clsfctn_type_eq1 is null) then 1 else 0 end as prv2_9_numer0
                from
                    {dqm.taskprefix}_prv_id_npi a
                left join (
                    select
                        submtg_state_cd,
                        submtg_state_prvdr_id as prvdr_id,
                        max(case when prvdr_clsfctn_type_cd=1 then 1 else 0 end) as prvdr_clsfctn_type_eq1
                    from
                        {dqm.taskprefix}_tmsis_prvdr_txnmy_clsfctn
                    group by
                        submtg_state_cd,
                        submtg_state_prvdr_id
                ) b

                on a.submtg_state_cd = b.submtg_state_cd and
                    a.prvdr_id = b.prvdr_id
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_prvdr_npi_txnmy2 as
        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'501' as submodule
                    ,prv2_9_numer as numer
                    ,prv2_9_denom as denom
                    ,case when prv2_9_denom >0 then round(prv2_9_numer / prv2_9_denom,2)
                            else null end as mvalue
                from
                    (select
                        submtg_state_cd
                        ,sum(prv2_9_numer0) as prv2_9_numer
                        ,sum(prv2_9_denom0) as prv2_9_denom
                    from
                        {dqm.taskprefix}_prvdr_npi_txnmy
                    group by
                        submtg_state_cd
                    ) a
            """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    v_table = {
            "tot_match_loc": tot_match_loc,
            "pct_match_loc": pct_match_loc,
            "prv_addtyp": prv_addtyp,
            "prv_idtyp": prv_idtyp,
            "prvdr_npi_txnmy": prvdr_npi_txnmy,
            "prv2_10": prv2_10,
            "prvdr_mdcd_enrlmt": prvdr_mdcd_enrlmt,
            "prv1_1": prv1_1,
            "prv1_2": prv1_2
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