# --------------------------------------------------------------------
#
#
#
# --------------------------------------------------------------------
from dqm.DQClosure import DQClosure
from dqm.DQM_Metadata import DQM_Metadata
from dqm.DQMeasures import DQMeasures

class Runner_707:

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def run(spark, dqm: DQMeasures, measures: list = None) :
        dqm.logger.info('Module().run<series>.run() has been deprecated. Use dqm.run(spark) or dqm.run(spark, dqm.where(series="<series>")) instead.')
        if measures is None:
            return dqm.run(spark, dqm.reverse_measure_lookup[dqm.reverse_measure_lookup['series'] == '707']['measure_id'].tolist())
        else:
            return dqm.run(spark, measures)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def ot_clm_aj(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ot_prep_line as
                select

                    /*unique keys and other identifiers*/
                    submtg_state_cd
                    ,tmsis_rptg_prd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,adjstmt_ind
                    ,orgnl_line_num
                    ,adjstmt_line_num
                    ,line_adjstmt_ind
                    ,bill_type_cd
                    ,prcdr_cd
                    ,srvc_plc_cd
                    ,rev_cd
                    ,hcpcs_srvc_cd
                    ,wvr_id
                    ,{DQClosure.parse('case when %nmisslogic(srvc_plc_cd) then 1 else 0 end  as nmsng_pos')}
                    ,{DQClosure.parse('case when (%nmisslogic(srvc_plc_cd) and  %misslogic(prcdr_cd)) then 1 else 0 end as nmsng_pos_msng_prcdr_cd')}
                    ,{DQClosure.parse('case when (%nmisslogic(bill_type_cd) and %nmisslogic(srvc_plc_cd)) then 1 else 0 end as nmsng_pos_bill_type')}
                    ,{DQClosure.parse('case when (%misslogic(bill_type_cd)  and %misslogic(srvc_plc_cd)) then 1 else 0 end as msng_bill_pos_type')}
                    ,{DQClosure.parse('case when %nmisslogic(bill_type_cd) then 1 else 0 end  as nmsng_bill')}
                    ,{DQClosure.parse('case when (%nmisslogic(bill_type_cd) and %misslogic(rev_cd)) then 1 else 0 end as nmsng_bill_msng_rev')}
                    ,{DQClosure.parse('case when %nmisslogic(rev_cd) then 1 else 0 end  as nmsng_rev')}
                    ,{DQClosure.parse('case when (%nmisslogic(rev_cd) and %misslogic(bill_type_cd)) then 1 else 0 end as nmsng_rev_msng_bill_type')}
                    ,{DQClosure.parse('case when (%misslogic(prcdr_cd) and %misslogic(rev_cd)) then 1 else 0 end as msng_prcdr_rev')}
                    ,{DQClosure.parse('case when hcpcs_srvc_cd ="4"                           then 1 else 0 end as hcbs_eq4')}
                    ,{DQClosure.parse('case when hcpcs_srvc_cd ="4" and (%misslogic(wvr_id) ) then 1 else 0 end as hcbs_eq4_msng_wvr_id')}

                from {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
                where {DQM_Metadata.create_base_clh_view.claim_cat['AJ']}
                and childless_header_flag = 0
             """

        dqm.logger.debug(z)
        spark.sql(z)

        # /*rolling up to unique claim header level*/
        # /*therefore, taking max value of indicator across claim lines*/
        z = f"""
                create or replace temporary view {dqm.taskprefix}_ot_rollup_line as
                select
                    submtg_state_cd
                    ,tmsis_rptg_prd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,orgnl_line_num
                    ,adjstmt_line_num
                    ,line_adjstmt_ind
                    ,max(nmsng_pos) as ALL15_1_denom0
                    ,max(nmsng_pos_msng_prcdr_cd) as ALL15_1_numer0
                    ,max(nmsng_pos_bill_type) as ALL15_2_numer0
                    ,max(msng_bill_pos_type) as ALL15_3_numer0
                    ,max(nmsng_bill) as ALL15_4_denom0
                    ,max(nmsng_bill_msng_rev) as ALL15_4_numer0
                    ,max(nmsng_rev) as ALL15_5_denom0
                    ,max(nmsng_rev_msng_bill_type) as ALL15_5_numer0
                    ,max(msng_prcdr_rev) as ALL15_6_numer0

                from {dqm.taskprefix}_ot_prep_line
                group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt,
                        orgnl_line_num ,adjstmt_line_num, line_adjstmt_ind
             """

        dqm.logger.debug(z)
        spark.sql(z)

        # /*now summing to get values for state and month*/
        # TODO: refactor
        z = f"""
                create or replace temporary view {dqm.taskprefix}_ot_clm_aj as
                select
                    a.*
                    ,{measure_id}_denom as denom
                    ,{measure_id}_numer as numer

                    ,case when ALL15_1_denom >0 then round((ALL15_1_numer / ALL15_1_denom),3) else null end as ALL15_1
                    ,case when ALL15_2_denom >0 then round((ALL15_2_numer / ALL15_2_denom),3) else null end as ALL15_2
                    ,case when ALL15_3_denom >0 then round((ALL15_3_numer / ALL15_3_denom),3) else null end as ALL15_3
                    ,case when ALL15_4_denom >0 then round((ALL15_4_numer / ALL15_4_denom),3) else null end as ALL15_4
                    ,case when ALL15_5_denom >0 then round((ALL15_5_numer / ALL15_5_denom),3) else null end as ALL15_5
                    ,case when ALL15_6_denom >0 then round((ALL15_6_numer / ALL15_6_denom),3) else null end as ALL15_6
                from
                    (select
                        submtg_state_cd
                        ,sum(ALL15_1_denom0) as ALL15_1_denom
                        ,sum(ALL15_1_numer0) as ALL15_1_numer
                        ,count(submtg_state_cd) as ALL15_2_denom
                        ,sum(ALL15_2_numer0) as ALL15_2_numer
                        ,count(submtg_state_cd) as ALL15_3_denom
                        ,sum(ALL15_3_numer0) as ALL15_3_numer
                        ,sum(ALL15_4_denom0) as ALL15_4_denom
                        ,sum(ALL15_4_numer0) as ALL15_4_numer
                        ,sum(ALL15_5_denom0) as ALL15_5_denom
                        ,sum(ALL15_5_numer0) as ALL15_5_numer
                        ,count(submtg_state_cd) as ALL15_6_denom
                        ,sum(ALL15_6_numer0) as ALL15_6_numer
                    from {dqm.taskprefix}_ot_rollup_line
                    group by submtg_state_cd
                    ) a
             """

        dqm.logger.debug(z)

        spark.sql(z)

        z = f"""
                SELECT
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'707' as submodule
                    ,numer
                    ,denom
                    ,{measure_id} as mvalue
                FROM {dqm.taskprefix}_ot_clm_aj
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def ot_hcpcs_clm_aj(spark, dqm: DQMeasures, measure_id, x) :

        # create or replace temporary view {dqm.taskprefix}_ot_hcpcs_clm_aj as
        z = f"""
                
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'707' as submodule
                    ,sum(hcbs_eq4)             as denom
                    ,sum(hcbs_eq4_msng_wvr_id) as numer
                    ,case when sum(hcbs_eq4) >0 then round((sum(hcbs_eq4_msng_wvr_id) /sum(hcbs_eq4)),3)
                        else null end as mvalue
            from
                /*unique keys and other identifiers*/
                (select
                        submtg_state_cd
                        ,tmsis_rptg_prd
                        ,orgnl_clm_num
                        ,adjstmt_clm_num
                        ,adjdctn_dt
                        ,adjstmt_ind
                        ,max(hcbs_eq4) as hcbs_eq4
                        ,max(hcbs_eq4_msng_wvr_id) as hcbs_eq4_msng_wvr_id

                    from {dqm.taskprefix}_ot_prep_line
                group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num,
                            adjstmt_clm_num, adjdctn_dt, adjstmt_ind
                        ) a

                group by submtg_state_cd
             """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def clm_blg_prov_evr_enrl_sql(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_{x['claim_type']}_evr_enrl_blg_prvdr_aj as
                select distinct
                    submtg_state_cd
                    ,blg_prvdr_num as clm_prvdr
                    ,{x['clm_dt']} as clm_dt
                from {DQMeasures.getBaseTable(dqm, 'clh', x['claim_type'])}
                where {DQM_Metadata.create_base_clh_view.claim_cat['AJ']} and
                    blg_prvdr_num is not null
             """

        dqm.logger.debug(z)

        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_{x['claim_type']}_evr_enrl_blg_prvdr as
                select
                    a.submtg_state_cd
                    ,a.clm_prvdr
                    /**at least one claim id/date that didn't link to provider file **/
                    ,max(case when b.prvdr_id is null then 1 else 0 end) as flag_not_in_prov

                from {dqm.taskprefix}_{x['claim_type']}_evr_enrl_blg_prvdr_aj a
                left join {dqm.taskprefix}_prvdr_ever_enrld b

                on  a.submtg_state_cd = b.submtg_state_cd and
                    a.clm_prvdr = b.prvdr_id and
                    (((prvdr_mdcd_efctv_dt  <= clm_dt and prvdr_mdcd_efctv_dt is not null)
                    and (prvdr_mdcd_end_dt  >= clm_dt or prvdr_mdcd_end_dt  is null)))

                group by a.submtg_state_cd, a.clm_prvdr
             """

        dqm.logger.debug(z)

        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_utl_{x['claim_type']}_evr_enrl_blg_prov as
        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'707' as submodule
                    ,sum(flag_not_in_prov) as numer
                    ,count(submtg_state_cd) as denom
                    ,round((sum(flag_not_in_prov)/count(submtg_state_cd)),2) as mvalue
                from {dqm.taskprefix}_{x['claim_type']}_evr_enrl_blg_prvdr
                group by submtg_state_cd
             """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def clm_srvc_prov_evr_enrl_sql(spark, dqm: DQMeasures, measure_id, x) :

        if x['claim_type'] in ['ip','lt','ot']:
            clm_prvdr = 'srvcng_prvdr_num'
            clm_dt = 'srvc_bgnng_dt'
            base_table = DQMeasures.getBaseTable(dqm, 'cll', x['claim_type'])

        elif x['claim_type'] == 'rx':
            clm_prvdr = 'dspnsng_pd_prvdr_num'
            clm_dt = 'rx_fill_dt'
            base_table = DQMeasures.getBaseTable(dqm, 'clh', x['claim_type'])

        z = f"""
                create or replace temporary view {dqm.taskprefix}_{x['claim_type']}_evr_enrl_srvc_prvdr_aj as
                select distinct
                    submtg_state_cd
                    ,{clm_prvdr} as clm_prvdr
                    ,{clm_dt} as clm_dt
                from {base_table}
                where {DQM_Metadata.create_base_clh_view.claim_cat['AJ']} and
                      {clm_prvdr} is not null
             """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_{x['claim_type']}_evr_enrl_srvc_prvdr as
                select
                    a.submtg_state_cd
                    ,a.clm_prvdr
                    /**at least one claim id/date that didn't link to provider file **/
                    ,max(case when b.prvdr_id is null then 1 else 0 end) as flag_not_in_prov

                from {dqm.taskprefix}_{x['claim_type']}_evr_enrl_srvc_prvdr_aj a
                left join {dqm.taskprefix}_prvdr_ever_enrld b

                on  a.submtg_state_cd = b.submtg_state_cd and
                    a.clm_prvdr = b.prvdr_id and
                    (((prvdr_mdcd_efctv_dt  <= clm_dt and prvdr_mdcd_efctv_dt is not null)
                    and (prvdr_mdcd_end_dt  >= clm_dt or prvdr_mdcd_end_dt  is null)))

                group by a.submtg_state_cd, a.clm_prvdr
             """

        dqm.logger.debug(z)
        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_utl_{x['claim_type']}_evr_enrl_srvc_prov as
        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'707' as submodule
                    ,sum(flag_not_in_prov) as numer
                    ,count(submtg_state_cd) as denom
                    ,round((sum(flag_not_in_prov)/count(submtg_state_cd)),2) as mvalue
                from {dqm.taskprefix}_{x['claim_type']}_evr_enrl_srvc_prvdr
                group by submtg_state_cd
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    v_table = {
        "ot_clm_aj": ot_clm_aj,
        "ot_hcpcs_clm_aj": ot_hcpcs_clm_aj,
        "clm_blg_prov_evr_enrl_sql": clm_blg_prov_evr_enrl_sql,
        "clm_srvc_prov_evr_enrl_sql": clm_srvc_prov_evr_enrl_sql
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