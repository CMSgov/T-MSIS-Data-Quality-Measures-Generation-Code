# --------------------------------------------------------------------
#
#
#
# --------------------------------------------------------------------
from dqm.DQM_Metadata import DQM_Metadata
from dqm.DQMeasures import DQMeasures

class Runner_705:

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def run(spark, dqm: DQMeasures, measures: list = None) :
        dqm.logger.info('Module().run<series>.run() has been deprecated. Use dqm.run(spark) or dqm.run(spark, dqm.where(series="<series>")) instead.')
        if measures is None:
            return dqm.run(spark, dqm.reverse_measure_lookup[dqm.reverse_measure_lookup['series'] == '705']['measure_id'].tolist())
        else:
            return dqm.run(spark, measures)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def utl_ip_ab_ac_clm(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ip_prep_clm_{x['clmcat']} as
                select
                     submtg_state_cd
                    ,tmsis_rptg_prd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,adjstmt_ind
                    ,max(case when (xovr_ind = '1') then 1 else 0 end) as xover_clm
                from
                    {DQMeasures.getBaseTable(dqm, 'clh', 'ip')}
                where
                    {DQM_Metadata.create_base_clh_view().claim_cat[x['clmcat']]}
                group by
                    submtg_state_cd,
                    tmsis_rptg_prd,
                    orgnl_clm_num,
                    adjstmt_clm_num,
                    adjdctn_dt,
                    adjstmt_ind
             """

        dqm.logger.debug(z)
        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_ip_clm_{x['clmcat']} as
        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'705' as submodule

                    ,sum(xover_clm) as numer
                    ,count(submtg_state_cd) as denom
                    ,round((sum(xover_clm) / count(submtg_state_cd)),2) as mvalue
                from
                    {dqm.taskprefix}_ip_prep_clm_{x['clmcat']}
                group by
                    submtg_state_cd

             """

        dqm.logger.debug(z)
        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def all13_5(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ip_hdr_clm_ab as
                select
                     a.submtg_state_cd
                    ,msis_ident_num  /**keep msis id to link to el files*/
                    ,tmsis_rptg_prd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,adjstmt_ind
                    ,admsn_dt
                    ,case
                        when array_contains(code_cm,dgns_1_cd) then 1
                        when array_contains(code_cm,dgns_2_cd) then 1
                        when array_contains(code_cm,dgns_3_cd) then 1
                        when array_contains(code_cm,dgns_4_cd) then 1
                        when array_contains(code_cm,dgns_5_cd) then 1
                        when array_contains(code_cm,dgns_6_cd) then 1
                        when array_contains(code_cm,dgns_7_cd) then 1
                        when array_contains(code_cm,dgns_8_cd) then 1
                        when array_contains(code_cm,dgns_9_cd) then 1
                        when array_contains(code_cm,dgns_10_cd) then 1
                        when array_contains(code_cm,dgns_11_cd) then 1
                        when array_contains(code_cm,dgns_12_cd) then 1
                        else 0 end as prgncy_dx
                    ,case
                        when array_contains(code_pcs,prcdr_1_cd) then 1
                        when array_contains(code_pcs,prcdr_2_cd) then 1
                        when array_contains(code_pcs,prcdr_3_cd) then 1
                        when array_contains(code_pcs,prcdr_4_cd) then 1
                        when array_contains(code_pcs,prcdr_5_cd) then 1
                        when array_contains(code_pcs,prcdr_6_cd) then 1
                        else 0 end as prgncy_pcs
                from {DQMeasures.getBaseTable(dqm, 'clh', 'ip')} a
                left join {dqm.taskprefix}_prgncy_codes b
                on a.submtg_state_cd = b.submtg_state_cd
                where {DQM_Metadata.create_base_clh_view().claim_cat['AB']}
             """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ip_line_to_hdr_rollup_ab as
                select
                     submtg_state_cd
                    ,tmsis_rptg_prd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,adjstmt_ind
                    ,max(case when rev_cd in ('450', '451', '452', '453', '454', '455', '456', '457', '458', '459',
                                            '0450', '0451', '0452', '0453', '0454', '0455', '0456', '0457', '0458', '0459',
                                            '981','720','721','722','723','724','729',
                                            '0981','0720','0721','0722','0723','0724','0729'
                                            )
                            then 1 else 0 end) as rev_cd_excl
                from
                    {DQMeasures.getBaseTable(dqm, 'cll', 'ip')}
                where
                    {DQM_Metadata.create_base_clh_view().claim_cat['AB']}
                group by
                    submtg_state_cd,
                    tmsis_rptg_prd,
                    orgnl_clm_num,
                    adjstmt_clm_num,
                    adjdctn_dt,
                    adjstmt_ind
             """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_all_13_5 AS

                SELECT submtg_state_cd, SUM(all13_5_numer_1) AS all13_5_numer, SUM(all13_5_denom_1) AS all13_5_denom

                FROM (

                    SELECT
                         submtg_state_cd
                        ,msis_ident_num
                        ,max(all13_5_denom_orig) as all13_5_denom_1
                        ,max(case when all13_5_denom_orig=1 then all13_5_numer_orig else 0 end) as all13_5_numer_1

                    FROM (

                        SELECT
                              q1.submtg_state_cd
                            , q1.msis_ident_num
                            , CASE WHEN q3.rstrctd_bnfts_cd IN ('2') THEN 1 ELSE 0 END AS all13_5_denom_orig
                            , CASE WHEN all13_5_numer_excl = 1 THEN 0 ELSE 1 END AS all13_5_numer_orig

                        FROM (
                            /* ip_hdr_clm_bene_ab */
                            SELECT
                                 a.submtg_state_cd
                                ,a.msis_ident_num
                                ,a.tmsis_rptg_prd
                                ,a.orgnl_clm_num
                                ,a.adjstmt_clm_num
                                ,a.adjdctn_dt
                                ,a.adjstmt_ind
                                ,a.admsn_dt
                                ,max(CASE WHEN b.rev_cd_excl=1 OR a.prgncy_dx=1 OR a.prgncy_pcs=1 THEN 1 ELSE 0 END) as all13_5_numer_excl
                            FROM
                                {dqm.taskprefix}_ip_hdr_clm_ab a
                            LEFT JOIN
                                {dqm.taskprefix}_ip_line_to_hdr_rollup_ab b
                                    ON
                                        a.submtg_state_cd = b.submtg_state_cd AND
                                        a.tmsis_rptg_prd = b.tmsis_rptg_prd AND
                                        a.orgnl_clm_num = b.orgnl_clm_num AND
                                        a.adjstmt_clm_num = b.adjstmt_clm_num AND
                                        a.adjdctn_dt = b.adjdctn_dt AND
                                        a.adjstmt_ind = b.adjstmt_ind

                            GROUP BY
                                a.submtg_state_cd,
                                a.msis_ident_num,
                                a.tmsis_rptg_prd,
                                a.orgnl_clm_num,
                                a.adjstmt_clm_num,
                                a.adjdctn_dt,
                                a.adjstmt_ind,
                                a.admsn_dt
                        ) q1 /* ip_hdr_clm_bene_ab */

                        INNER JOIN (
                            /* q1 ij q2 = ip_hdr_ab_ever_elig */
                            SELECT
                                submtg_state_cd
                                , msis_ident_num
                            FROM
                                {dqm.taskprefix}_ever_elig
                            WHERE
                                ever_eligible = 1
                            GROUP BY
                                submtg_state_cd,
                                msis_ident_num
                        ) q2 /* LJ + IJ = ip_hdr_ab_ever_elig */
                                ON
                                    q1.submtg_state_cd = q2.submtg_state_cd AND
                                    q1.msis_ident_num = q2.msis_ident_num

                        INNER JOIN
                            /* q2 ij q3 = ip_hdr_ab_ever_elig2 */

                            {dqm.taskprefix}_ever_elig_dtrmnt q3 /* lj, ij, ij = ip_hdr_ab_ever_elig2 */
                                ON
                                    q1.submtg_state_cd = q3.submtg_state_cd AND
                                    q1.msis_ident_num = q3.msis_ident_num AND
                                    (q1.admsn_dt>=q3.elgblty_dtrmnt_efctv_dt and q1.admsn_dt is not null ) AND
                                    (q1.admsn_dt<=q3.elgblty_dtrmnt_end_dt or q3.elgblty_dtrmnt_end_dt is NULL)
                    ) q4

                GROUP BY
                    q4.submtg_state_cd,
                    q4.msis_ident_num
                ) q5
                GROUP BY submtg_state_cd

             """

        spark.sql(z)
        dqm.logger.debug(z)

        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd,
                    '{measure_id}' AS measure_id,
                    '705' as submodule,
                    all13_5_numer AS numer,
                    all13_5_denom AS denom,
                    case when all13_5_denom > 0 then round((all13_5_numer /all13_5_denom),2) else null end as mvalue
                from
                    {dqm.taskprefix}_all_13_5
             """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def utl_lt_ab_ac_clm(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_lt_prep_clm_{x['clmcat']} as
                select

                    /*unique keys and other identifiers*/
                    submtg_state_cd
                    ,tmsis_rptg_prd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,adjstmt_ind
                    ,max(case when (xovr_ind = '1') then 1 else 0 end) as xover_clm

                from
                    {DQMeasures.getBaseTable(dqm, 'clh', 'lt')}
                where
                    {DQM_Metadata.create_base_clh_view().claim_cat[x['clmcat']]}
                group by
                    submtg_state_cd,
                    tmsis_rptg_prd,
                    orgnl_clm_num,
                    adjstmt_clm_num,
                    adjdctn_dt,
                    adjstmt_ind
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_lt_clm_{x['clmcat']} as
        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'705' as submodule
                    ,sum(xover_clm) as numer
                    ,count(submtg_state_cd) as denom
                    ,round((sum(xover_clm) / count(submtg_state_cd)),2) as mvalue

                from
                    {dqm.taskprefix}_lt_prep_clm_{x['clmcat']}
                group by
                    submtg_state_cd
             """

        dqm.logger.debug(z)
        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def utl_ot_ab_ac_clm(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ot_prep_clm_{x['clmcat']} as
                select

                        /*unique keys and other identifiers*/
                        submtg_state_cd
                        ,tmsis_rptg_prd
                        ,orgnl_clm_num
                        ,adjstmt_clm_num
                        ,adjdctn_dt
                        ,orgnl_line_num
                        ,adjstmt_line_num
                        ,line_adjstmt_ind
                        ,max(case when (xovr_ind = '1') then 1 else 0 end) as xover_clm

                    from {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
                    where {DQM_Metadata.create_base_clh_view().claim_cat[x['clmcat']]}
                    and childless_header_flag = 0
                    group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt,
                            orgnl_line_num ,adjstmt_line_num, line_adjstmt_ind
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_ot_clm_{x['clmcat']} as
        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'705' as submodule
                    ,sum(xover_clm) as numer
                    ,count(submtg_state_cd) as denom
                    ,round((sum(xover_clm) / count(submtg_state_cd)),2) as mvalue

                from {dqm.taskprefix}_ot_prep_clm_{x['clmcat']}
                group by submtg_state_cd
            """

        dqm.logger.debug(z)
        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def utl_link_ot_el_ab(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ot_hdr_clm_ab as
                select
                    /*+ BROADCAST(b) */

                    /*unique keys and other identifiers*/
                    a.submtg_state_cd
                    ,msis_ident_num  /**keep msis id to link to el files*/
                    ,tmsis_rptg_prd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,adjstmt_ind
                    ,srvc_bgnng_dt
                    ,pgm_type_cd
                    ,wvr_id
                    ,case when (pgm_type_cd not in ('02') or pgm_type_cd is null) then 1 else 0 end as all13_1_orig
                    ,case when array_contains(code_cm,dgns_1_cd)
                            or array_contains(code_cm,dgns_2_cd)
                        then 1 else 0 end as prgncy_dx

                from {DQMeasures.getBaseTable(dqm, 'clh', 'ot')} a
                left join {dqm.taskprefix}_prgncy_codes b
                on a.submtg_state_cd = b.submtg_state_cd
                where {DQM_Metadata.create_base_clh_view().claim_cat['AB']}
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ot_line_to_hdr_rollup_ab as
                select
                    /*+ BROADCAST(b) */

                    /*unique keys and other identifiers*/
                    a.submtg_state_cd
                    ,tmsis_rptg_prd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,adjstmt_ind
                    ,max(case when rev_cd in ('450', '451', '452', '453', '454', '455', '456', '457', '458', '459',
                                            '0450', '0451', '0452', '0453', '0454', '0455', '0456', '0457', '0458', '0459',
                                            '981','720','721','722','723','724','729',
                                            '0981','0720','0721','0722','0723','0724','0729' )  or
                                    srvc_plc_cd in ('23')  then 1 else 0 end) as rev_cd_excl/**ER claims - NOT be included in Numerator */
                    ,max(case when array_contains(code_prc,prcdr_cd) or array_contains(code_prc,prcdr_cd) then 1 else 0 end) as prgncy_pcs
                from {DQMeasures.getBaseTable(dqm, 'cll', 'ot')} a
                left join {dqm.taskprefix}_prgncy_codes b
                on a.submtg_state_cd = b.submtg_state_cd
                where {DQM_Metadata.create_base_clh_view().claim_cat['AB']}
                and childless_header_flag = 0
                group by a.submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                CREATE OR replace temporary VIEW {dqm.taskprefix}_all_13_1 AS
                SELECT /* agg to st */
                     submtg_state_cd
                    ,sum(all13_1_numer_1) as all13_1_numer
                    ,sum(all13_1_denom_1) as all13_1_denom
                    ,sum(all13_6_numer_1) as all13_6_numer
                    ,sum(all13_6_denom_1) as all13_6_denom

                    ,case when (sum(all13_1_denom_1) > 0) then round((sum(all13_1_numer_1) / sum(all13_1_denom_1)),2) else null end as all13_1_mvalue
                    ,case when (sum(all13_6_denom_1) > 0) then round((sum(all13_6_numer_1) / sum(all13_6_denom_1)),2) else null end as all13_6_mvalue

                FROM ( /* agg to st, mid */
                    SELECT
                         submtg_state_cd
                        ,msis_ident_num
                        ,max(all13_1_denom_orig) as all13_1_denom_1
                        ,max(all13_6_denom_orig) as all13_6_denom_1
                        ,max(case when all13_1_denom_orig=1 then all13_1_numer_orig else 0 end) as all13_1_numer_1
                        ,max(case when all13_6_denom_orig=1 then all13_6_numer_orig else 0 end) as all13_6_numer_1
                    FROM ( /* select from q1, q2, q3 */
                        SELECT
                             q1.submtg_state_cd
                            ,q1.msis_ident_num
                            ,case when q3.rstrctd_bnfts_cd in ('6') then 1 else 0 end as all13_1_denom_orig
                            ,case when q3.rstrctd_bnfts_cd in ('2') then 1 else 0 end as all13_6_denom_orig
                            ,case when all13_6_numer_excl =1 then 0 else 1 end as all13_6_numer_orig
                            , all13_1_numer_orig
                        FROM ( /* ot_hdr_clm_bene_ab */

                            SELECT
                                 a.submtg_state_cd
                                ,a.msis_ident_num
                                ,a.tmsis_rptg_prd
                                ,a.orgnl_clm_num
                                ,a.adjstmt_clm_num
                                ,a.adjdctn_dt
                                ,a.adjstmt_ind
                                ,a.srvc_bgnng_dt
                                ,max(a.all13_1_orig) as all13_1_numer_orig
                                ,max(case when b.rev_cd_excl=1 or a.prgncy_dx=1 or b.prgncy_pcs=1 then 1 else 0 end ) as all13_6_numer_excl
                            FROM
                                {dqm.taskprefix}_ot_hdr_clm_ab a
                            LEFT JOIN
                                {dqm.taskprefix}_ot_line_to_hdr_rollup_ab b
                            ON
                                a.submtg_state_cd=b.submtg_state_cd AND
                                a.tmsis_rptg_prd=b.tmsis_rptg_prd AND
                                a.orgnl_clm_num=b.orgnl_clm_num AND
                                a.adjstmt_clm_num=b.adjstmt_clm_num AND
                                a.adjdctn_dt=b.adjdctn_dt AND
                                a.adjstmt_ind=b.adjstmt_ind

                        GROUP BY a.submtg_state_cd, a.msis_ident_num, a.tmsis_rptg_prd, a.orgnl_clm_num, a.adjstmt_clm_num,
                                    a.adjdctn_dt, a.adjstmt_ind, a.srvc_bgnng_dt ) q1

                                INNER JOIN (
                                    /* q1 ij q2 = ot_hdr_ab_ever_elig */
                                    SELECT
                                        submtg_state_cd
                                        , msis_ident_num
                                        FROM {dqm.taskprefix}_ever_elig
                                        WHERE ever_eligible = 1
                                        GROUP BY submtg_state_cd, msis_ident_num) q2 /* LJ + IJ = ot_hdr_ab_ever_elig */
                                ON
                                    q1.submtg_state_cd = q2.submtg_state_cd AND
                                    q1.msis_ident_num = q2.msis_ident_num

                                INNER JOIN
                                /* q2 ij q3 = ot_hdr_ab_ever_elig2 */

                                {dqm.taskprefix}_ever_elig_dtrmnt q3 /* lj, ij, ij = ot_hdr_ab_ever_elig2 */
                                    ON
                                q1.submtg_state_cd = q3.submtg_state_cd AND
                                q1.msis_ident_num = q3.msis_ident_num AND
                                (q1.srvc_bgnng_dt>=q3.elgblty_dtrmnt_efctv_dt and q1.srvc_bgnng_dt is not null ) AND
                                (q1.srvc_bgnng_dt<=q3.elgblty_dtrmnt_end_dt or q3.elgblty_dtrmnt_end_dt is NULL)

                ) q4

                GROUP BY q4.submtg_state_cd, q4.msis_ident_num) q5
                GROUP BY submtg_state_cd

            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                SELECT
                     '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'705' as submodule
                    ,sum({measure_id}_numer) as numer
                    ,sum({measure_id}_denom) as denom
                    ,case when (sum({measure_id}_denom) > 0) then round((sum({measure_id}_numer) / sum({measure_id}_denom)),2) else null end as mvalue
                FROM
                    {dqm.taskprefix}_all_13_1
            """

        dqm.logger.debug(z)
        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def utl_ot_hdr_wvr_lnk_pct_ab(spark, dqm: DQMeasures, measure_id, x) :

        # /**merge wvr to OT hdr */

        z = f"""
                create or replace temporary view {dqm.taskprefix}_wvr_1915_ot_hdr_ab as
                select  a.submtg_state_cd
                    ,a.msis_ident_num
                    ,case when b.msis_ident_num is null then 1 else 0 end as no_clm_rec
                    ,case when a.wvr_id=b.wvr_id then 1 else 0 end as  ALL2_9_same_wvr_id
                    ,case when b.pgm_type_cd in ('07') then 1 else 0 end as  ALL2_10_pgmtyp_07

                from {dqm.taskprefix}_utl_1915_wvr a
                    left join {dqm.taskprefix}_ot_hdr_clm_ab  b

                on a.submtg_state_cd=b.submtg_state_cd and
                    a.msis_ident_num=b.msis_ident_num
            """

        dqm.logger.debug(z)
        spark.sql(z)


        # /*now rolling upto one record per msis id*/

        z = f"""
                create or replace temporary view {dqm.taskprefix}_wvr_1915_ot_hdr_ab2 as
                select  submtg_state_cd
                        ,msis_ident_num
                        ,case when evr_no_clm_rec =1 or  ALL2_9_evr_same_wvr_id=0 then 1 else 0 end as all2_9_numer_1
                        ,case when evr_no_clm_rec =1 or  ALL2_10_evr_pgmtyp_07=0 then 1 else 0 end as all2_10_numer_1
                from
                    (select
                        submtg_state_cd
                        ,msis_ident_num
                        ,max(no_clm_rec) as  evr_no_clm_rec
                        ,max(ALL2_9_same_wvr_id) as  ALL2_9_evr_same_wvr_id
                        ,max( ALL2_10_pgmtyp_07) as  ALL2_10_evr_pgmtyp_07

                from {dqm.taskprefix}_wvr_1915_ot_hdr_ab
                group by submtg_state_cd,msis_ident_num) a
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_utl_ot_hdr_wvr_lnk_pct_ab as
                select
                     all2_9_numer
                    ,all2_9_denom
                    ,all2_10_numer
                    ,all2_10_denom
                from
                    (select
                        submtg_state_cd
                        ,sum(all2_9_numer_1)  as all2_9_numer
                        ,sum(all2_10_numer_1) as all2_10_numer
                        ,sum(1)               as all2_9_denom
                        ,sum(1)               as all2_10_denom
                    from {dqm.taskprefix}_wvr_1915_ot_hdr_ab2
                    group by submtg_state_cd) a
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                SELECT
                     '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'705' as submodule
                    ,sum({measure_id}_numer) as numer
                    ,sum({measure_id}_denom) as denom
                    ,case when (sum({measure_id}_denom) > 0) then round((sum({measure_id}_numer) / sum({measure_id}_denom)),2) else null end as mvalue
                FROM
                    {dqm.taskprefix}_utl_ot_hdr_wvr_lnk_pct_ab
            """

        dqm.logger.debug(z)
        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def utl_ot_line_wvr_lnk_pct_ab(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_wvr_1915_ot_line_ab as
                select  a.submtg_state_cd
                    ,a.msis_ident_num
                    ,case when b.msis_ident_num is null then 1 else 0 end as no_clm_rec
                    ,case when b.hcpcs_srvc_cd_eq_4=1   then 1 else 0 end as  ALL2_11_hcpcs_srvc_cd_eq_4

                from {dqm.taskprefix}_utl_1915_wvr a
                    left join {dqm.taskprefix}_ot_prep_clm2_ab b

                on a.submtg_state_cd=b.submtg_state_cd and
                    a.msis_ident_num=b.msis_ident_num
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # /*now rolling upto one record per msis id*/

        z = f"""
                create or replace temporary view {dqm.taskprefix}_wvr_1915_ot_line_ab2 as
                select 	submtg_state_cd
                        ,msis_ident_num
                        ,case when evr_no_clm_rec=1 or ALL2_11_ever_hcpcs_srvccd4=0 then 1 else 0 end as all2_11_numer_1
                from
                    (select
                        submtg_state_cd
                        ,msis_ident_num
                        ,max(no_clm_rec) as  evr_no_clm_rec
                        ,max(ALL2_11_hcpcs_srvc_cd_eq_4) as  ALL2_11_ever_hcpcs_srvccd4
                    from {dqm.taskprefix}_wvr_1915_ot_line_ab
                    group by submtg_state_cd,msis_ident_num) a
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_utl_ot_line_wvr_lnk_pct_ab as
        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'705' as submodule
                    ,all2_11_numer as numer
                    ,all2_11_denom as denom
                    ,case when all2_11_denom >0 then round((all2_11_numer /all2_11_denom),2)
                        else null end as mvalue
                from
                    (select
                        submtg_state_cd
                        ,sum(all2_11_numer_1) as all2_11_numer
                        ,sum(1)               as all2_11_denom
                    from {dqm.taskprefix}_wvr_1915_ot_line_ab2
                    group by submtg_state_cd) a
            """

        dqm.logger.debug(z)
        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def utl_link_rx_el_ab_sql(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                create or replace temporary view {dqm.taskprefix}_rx_hdr_clm_ab as
                select
                    submtg_state_cd
                    ,msis_ident_num  /**keep msis id to link to el files*/
                    ,tmsis_rptg_prd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,adjstmt_ind
                    ,rx_fill_dt
                    ,max(case when (pgm_type_cd not in ('02') or pgm_type_cd is null) then 1 else 0 end) as all13_2_numer_orig

                from {DQMeasures.getBaseTable(dqm, 'clh', 'rx')}
                where {DQM_Metadata.create_base_clh_view().claim_cat['AB']}
                group by submtg_state_cd, msis_ident_num, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind, rx_fill_dt
            """

        dqm.logger.debug(z)
        spark.sql(z)


        # /*merge to ever eligible **/
        z = f"""
                create or replace temporary view {dqm.taskprefix}_rx_hdr_ab_ever_elig as
                select a.*
                from {dqm.taskprefix}_rx_hdr_clm_ab a
                inner join
                    (select submtg_state_cd, msis_ident_num
                    from {dqm.taskprefix}_ever_elig
                    where ever_eligible=1
                    group by submtg_state_cd, msis_ident_num) b
                on    a.submtg_state_cd=b.submtg_state_cd and
                    a.msis_ident_num=b.msis_ident_num
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # /**merge to el determinant file and apply restrictions */

        z = f"""
                create or replace temporary view {dqm.taskprefix}_rx_hdr_ab_ever_elig2 as
                select a.*
                    ,case when b.rstrctd_bnfts_cd in ('6') then 1 else 0 end as all13_2_denom_orig
                from {dqm.taskprefix}_rx_hdr_ab_ever_elig a
                inner join {dqm.taskprefix}_ever_elig_dtrmnt b
                    on a.submtg_state_cd=b.submtg_state_cd and
                    a.msis_ident_num=b.msis_ident_num and
                    (a.rx_fill_dt >=b.elgblty_dtrmnt_efctv_dt and a.rx_fill_dt is not null ) and
                    (a.rx_fill_dt <=b.elgblty_dtrmnt_end_dt or b.elgblty_dtrmnt_end_dt is null)
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # /*now rolling upto one record per msis id*/

        z = f"""
                create or replace temporary view {dqm.taskprefix}_rx_el_lnk_ab as
                select
                    submtg_state_cd
                    ,msis_ident_num
                    ,max(all13_2_denom_orig) as all13_2_denom_1
                    ,max(case when all13_2_denom_orig=1 then all13_2_numer_orig else 0 end) as all13_2_numer_l
                from {dqm.taskprefix}_rx_hdr_ab_ever_elig2
                group by submtg_state_cd,msis_ident_num
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # create or replace temporary view {dqm.taskprefix}_utl_rx_el_lnk_pct_ab as
        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'705' as submodule
                    ,all13_2_numer as numer
                    ,all13_2_denom as denom
                    ,case when all13_2_denom >0 then round((all13_2_numer / all13_2_denom),2)
                        else null end as mvalue
                from
                    (select
                        submtg_state_cd
                        ,sum(all13_2_numer_l) as all13_2_numer
                        ,sum(all13_2_denom_1) as all13_2_denom
                    from {dqm.taskprefix}_rx_el_lnk_ab
                    group by submtg_state_cd) a
            """

        dqm.logger.debug(z)
        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    v_table = {
        "utl_ip_ab_ac_clm": utl_ip_ab_ac_clm,
        "all13_5": all13_5,
        "utl_lt_ab_ac_clm": utl_lt_ab_ac_clm,
        "utl_ot_ab_ac_clm": utl_ot_ab_ac_clm,
        "utl_link_ot_el_ab": utl_link_ot_el_ab,
        "utl_ot_hdr_wvr_lnk_pct_ab": utl_ot_hdr_wvr_lnk_pct_ab,
        "utl_ot_line_wvr_lnk_pct_ab": utl_ot_line_wvr_lnk_pct_ab,
        "utl_link_rx_el_ab_sql": utl_link_rx_el_ab_sql,
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