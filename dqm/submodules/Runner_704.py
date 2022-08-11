# --------------------------------------------------------------------
#
#
#
# --------------------------------------------------------------------
from dqm.DQM_Metadata import DQM_Metadata
from dqm.DQMeasures import DQMeasures

class Runner_704:

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def run(spark, dqm: DQMeasures, measures: list = None) :
        dqm.logger.info('Module().run<series>.run() has been deprecated. Use dqm.run(spark) or dqm.run(spark, dqm.where(series="<series>")) instead.')
        if measures is None:
            return dqm.run(spark, dqm.reverse_measure_lookup[dqm.reverse_measure_lookup['series'] == '704']['measure_id'].tolist())
        else:
            return dqm.run(spark, measures)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def all3_1(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ip_prep_w as
                select
                     tmsis_rptg_prd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,adjstmt_ind
                    ,orgnl_line_num
                    ,adjstmt_line_num
                    ,bnft_type_cd
                    /*standard measures calculated over all claim lines*/
                    /*denominator is at claim header level, but numer at claim line level*/

                    ,case when (bnft_type_cd in ('001','014','052')) then 1 else 0 end as all3_1
                from
                    {DQMeasures.getBaseTable(dqm, 'cll', 'ip')}
                where
                    {DQM_Metadata.create_base_clh_view().claim_cat['W']}
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ip_rollup_w as
                select
                     tmsis_rptg_prd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,adjstmt_ind
                    ,max(all3_1) as all3_1
                from
                    {dqm.taskprefix}_ip_prep_w
                group by
                    tmsis_rptg_prd,
                    orgnl_clm_num,
                    adjstmt_clm_num,
                    adjdctn_dt,adjstmt_ind
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'704' as submodule
                    ,sum(all3_1) as numer
                    ,count(*) as denom
                    ,round((sum(all3_1) / count(*)),2) as mvalue
                from
                    {dqm.taskprefix}_ip_rollup_w
                group by
                    '{dqm.state}'
             """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def all3_2(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ip_prep_w as
                select
                     tmsis_rptg_prd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,adjstmt_ind
                    ,orgnl_line_num
                    ,adjstmt_line_num
                    ,bnft_type_cd
                    /*standard measures calculated over all claim lines*/
                    /*denominator is at claim header level, but numer at claim line level*/

                    ,case when (hosp_type_cd in ('03','04','05','07','08')) then 1 else 0 end as all3_2

                from
                    {DQMeasures.getBaseTable(dqm, 'cll', 'ip')}
                where
                    {DQM_Metadata.create_base_clh_view().claim_cat['W']}
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ip_rollup_w as
                select
                     tmsis_rptg_prd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,adjstmt_ind
                    ,max(all3_2) as all3_2

                from
                    {dqm.taskprefix}_ip_prep_w
                group by
                    tmsis_rptg_prd,
                    orgnl_clm_num,
                    adjstmt_clm_num,
                    adjdctn_dt,adjstmt_ind
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'704' as submodule
                    ,sum(all3_2) as numer
                    ,count(*) as denom
                    ,round((sum(all3_2) / count(*)),2) as mvalue

                from
                    {dqm.taskprefix}_ip_rollup_w
                group by
                    '{dqm.state}'
             """
        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def merge_clm_prov_sql(spark, dqm: DQMeasures, measure_id, x) :

        if (x['claim_type'] == 'rx'):
            clm_prvdr = 'dspnsng_pd_prvdr_num'
        else:
            clm_prvdr = 'srvcng_prvdr_num'

        z = f"""
                create or replace temporary view {dqm.taskprefix}_{x['claim_type']}_prvdr_w as
                select distinct
                    submtg_state_cd
                    ,blg_prvdr_num as clm_prvdr
                from {DQMeasures.getBaseTable(dqm, 'clh', x['claim_type'])}
                where {DQM_Metadata.create_base_clh_view.claim_cat['W']}

                union

                select distinct
                    submtg_state_cd
                    ,{clm_prvdr} as clm_prvdr
                from {DQMeasures.getBaseTable(dqm, 'cll', x['claim_type'])}
                where {DQM_Metadata.create_base_clh_view.claim_cat['W']}
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_{x['claim_type']}_prvdr as
                select
                    a.submtg_state_cd
                    ,a.clm_prvdr
                    ,sum(case when b.prvdr_id is null then 1 else 0 end) as flag_not_in_prov
                from {dqm.taskprefix}_{x['claim_type']}_prvdr_w a
                left join {dqm.taskprefix}_prvdr_prep b
                on  a.submtg_state_cd = b.submtg_state_cd and
                    a.clm_prvdr = b.prvdr_id
                where a.clm_prvdr is not null
                group by a.submtg_state_cd, a.clm_prvdr
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                select
                    '{dqm.state}' as submtg_state_cd,
                    '{measure_id}' as measure_id,
                    '704' as submodule,
                    sum(flag_not_in_prov) as numer,
                    count(submtg_state_cd) as denom,
                    round((sum(flag_not_in_prov)/count(submtg_state_cd)),2) as mvalue
                from {dqm.taskprefix}_{x['claim_type']}_prvdr
                group by '{dqm.state}'
            """

        dqm.logger.debug(z)
        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def get_dups_clh(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_dup_clh_recs_{x['claim_type']} as
                select
                    tmsis_run_id
                    ,tmsis_rptg_prd
                    ,submtg_state_cd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,adjstmt_ind
                    ,case when (count(submtg_state_cd) > 1) then 1 else 0 end as flag_dup

                from {dqm.taskprefix}_dup_clh_{x['claim_type']}
                where {DQM_Metadata.create_base_clh_view.claim_cat['W']}
                group by tmsis_run_id, tmsis_rptg_prd, submtg_state_cd, orgnl_clm_num,
                        adjstmt_clm_num, adjdctn_dt, adjstmt_ind
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'704' as submodule
                    ,sum(flag_dup) as numer
                    ,count(submtg_state_cd) as denom
                    ,round((sum(flag_dup)/count(submtg_state_cd)),3) as mvalue
                from {dqm.taskprefix}_dup_clh_recs_{x['claim_type']}
                group by '{dqm.state}'
            """

        dqm.logger.debug(z)
        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def get_dups_cll(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_dup_cll_recs_{x['claim_type']} as
                select
                    tmsis_run_id
                    ,tmsis_rptg_prd
                    ,submtg_state_cd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,orgnl_line_num
                    ,adjstmt_line_num
                    ,line_adjstmt_ind
                    ,case when (count(submtg_state_cd) > 1) then 1 else 0 end as flag_dup

                from {dqm.taskprefix}_dup_cll_{x['claim_type']}
                /*Note - removed claim_cat_w because it just means all claims. restructuring to get that variable
                        onto this file is more effort than it is worth. No filter needed since it is all claims. */
                group by tmsis_run_id, tmsis_rptg_prd, submtg_state_cd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt,
                        orgnl_line_num, adjstmt_line_num, line_adjstmt_ind
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'704' as submodule
                    ,sum(flag_dup) as numer
                    ,count(submtg_state_cd) as denom
                    ,round((sum(flag_dup)/count(submtg_state_cd)),3) as mvalue
                from {dqm.taskprefix}_dup_cll_recs_{x['claim_type']}
                group by '{dqm.state}'
            """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    v_table = {
        "all3_1": all3_1,
        "all3_2": all3_2,
        'merge_clm_prov_sql': merge_clm_prov_sql,
        'get_dups_clh': get_dups_clh,
        'get_dups_cll': get_dups_cll
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