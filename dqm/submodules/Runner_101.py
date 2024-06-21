# --------------------------------------------------------------------
#
#
#
# --------------------------------------------------------------------
from dqm.DQPrepETL import DQPrepETL
from pyspark.sql.session import SparkSession
from dqm.DQClosure import DQClosure
from dqm.DQMeasures import DQMeasures

class Runner_101:

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def run(spark, dqm: DQMeasures, measures: list = None) :
        dqm.logger.info('Module().run<series>.run() has been deprecated. Use dqm.run(spark) or dqm.run(spark, dqm.where(series="<series>")) instead.')
        if measures is None:
            return dqm.run(spark, dqm.reverse_measure_lookup[dqm.reverse_measure_lookup['series'] == '101']['measure_id'].tolist())
        else:
            return dqm.run(spark, measures)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def el16(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
            select
                 '{dqm.state}' AS submtg_state_cd
                ,'{measure_id}' AS measure_id
                ,'101' AS submodule
                ,sum(numer) AS numer
                ,sum(denom) AS denom
                ,case when sum(denom) > 0 then
                    round((sum(numer) / sum(denom)), 3)
                    else null end as mvalue
            from (
                select
                    msis_id as msis_ident_num,
                    ({DQClosure.parse('%misslogic(msis_id, 20)')}) as numer,
                    1 as denom
                from
                    {x['tbl']}
                where
                    {dqm.run_id_filter()}
                ) a
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def nonclaimspct(spark, dqm: DQMeasures, measure_id, x) :

        # Denominator Definition
        # AND
        #   (
        #       (tmsis_elgbl_cntct.elgbl_state_cd is not NULL
        #           AND
        #        tmsis_elgbl_cntct.elgbl_state_cd <> tmsis_elgbl_cntct.submtg_state_cd
        #   )
        #   OR
        #   (
        #       IN Zip_County_Crosswalk
        #           WHERE
        #               StateFIPS = tmsis_elgbl_cntct.submtg_state_cd

        #       (
        #           (
        #               tmsis_elgbl_cntct.elgbl_cnty_cd is not NULL
        #                   AND
        #               tmsis_elgbl_cntct.elgbl_cnty_cd <> any CountyFIPS
        #           )
        #           OR
        #           (
        #               tmsis_elgbl_cntct.elgbl_zip_cd is not NULL
        #                   AND
        #               tmsis_elgbl_cntct.elgbl_zip_cd <> any ZipCode
        #           )
        #       )
        #   )
        # )

        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'101' AS submodule
                    ,sum(numer) as numer
                    ,count(1) as denom
                    ,round(sum(numer) / count(1), {x['round']}) as mvalue
                from (
                    select
                        msis_ident_num
                        ,max({DQClosure.parse(x['numer'])}) as numer
                    from
                        {dqm.taskprefix}{x['tbl']}
                    where
                        {DQClosure.parse(x['denom'])}
                    group by
                        msis_ident_num
                ) a
             """.format(m_start=dqm.m_start, m_end=dqm.m_end)

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def nonclaimspct2tbl(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'101' AS submodule
                    ,sum(numer) as numer
                    ,count(1) as denom
                    ,round(sum(numer) / count(1), {x['round']}) as mvalue
                from (
                    select
                        a.msis_ident_num
                        ,max({DQClosure.parse(x['numer'])}) as numer
                    from
                        {dqm.taskprefix}{x['denomtbl']} a
                    left join
                        {dqm.taskprefix}{x['numertbl']} b
                            on a.msis_ident_num = b.msis_ident_num
                    where
                        {DQClosure.parse(x['denom'])}
                    group by
                        a.msis_ident_num
                ) c
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def nonclaimspct2tblwvr(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'101' AS submodule
                    ,sum(numer) as numer
                    ,count(1) as denom
                    ,round(sum(numer) / count(1), {x['round']}) as mvalue
                from (
                    select
                        ({DQClosure.parse(x['numer'])}) as numer
                    from
                        {dqm.taskprefix}{x['denomtbl']} a
                    left join
                        {dqm.taskprefix}{x['numertbl']} b
                            on a.msis_ident_num = b.msis_ident_num
                    where
                        {DQClosure.parse(x['denom'])}
                ) a
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def nonclaimspctwvr(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'101' AS submodule
                    ,sum(numer) as numer
                    ,count(1) as denom
                    ,round(sum(numer) / count(1), {x['round']}) as mvalue
                from (
                    select
                        ({DQClosure.parse(x['numer'])}) as numer
                    from
                        {dqm.taskprefix}{x['denomtbl']} a
                    where
                        {DQClosure.parse(x['denom'])}
                ) a
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def nonclaimspct_notany(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'101' AS submodule
                    ,sum(case when numer_max = 0 then 1 else 0 end) as numer
                    ,count(1) as denom
                    ,round(sum(case when numer_max = 0 then 1 else 0 end) / count(1), {x['round']}) as mvalue
                from (
                    select
                        msis_ident_num
                        ,max({DQClosure.parse(x['numer'])}) as numer_max
                    from
                        {dqm.taskprefix}{x['tbl']}
                    where
                        {DQClosure.parse(x['denom'])}
                    group by
                        msis_ident_num
                ) a
             """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def el319t(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'101' AS submodule
                    , sum(denom_val) as denom
                    , sum(numer_val) as numer
                    , case when sum(denom_val) > 0 then
                        round(sum(numer_val) / sum(denom_val), 3)
                        else null end as mvalue
                from (
                    select msis_ident_num
                        , max(case when submtg_state_cd in ({dqm.el_grp_72}) then 1 else 0 end) as denom_val
                        , max(case when submtg_state_cd in ({dqm.el_grp_72})
                                and elgblty_grp_cd='72' then 1 else 0 end) as numer_val
                    from
                        {dqm.taskprefix}_tmsis_elgblty_dtrmnt
                    where
                        msis_ident_num is not null
                    group by
                        msis_ident_num
            ) c
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def el333t(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'101' AS submodule
                    , sum(denom_val) as denom
                    , sum(numer_val) as numer
                    , case when sum(denom_val) > 0 then
                        round(sum(numer_val) / sum(denom_val), 3)
                        else null end as mvalue
                from (
                    select msis_ident_num
                        , max(case when submtg_state_cd in ({dqm.el_grp_73_74_75}) then 1 else 0 end) as denom_val
                        , max(case when submtg_state_cd in ({dqm.el_grp_73_74_75}) and elgblty_grp_cd in ('73', '74', '75') then 1 else 0 end) as numer_val
                    from
                        {dqm.taskprefix}_tmsis_elgblty_dtrmnt
                    where
                        msis_ident_num is not null
                    group by
                        msis_ident_num
                ) c
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def el322t(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'101' AS submodule
                    , sum(denom_val) as denom
                    , sum(numer_val) as numer
                    , case when sum(denom_val) > 0 then
                        round(sum(numer_val) / sum(denom_val), 3)
                        else null end as mvalue
                from (
                    select msis_ident_num
                        , max(case when submtg_state_cd not in ({dqm.el_grp_72}) then 1 else 0 end) as denom_val
                        , max(case when submtg_state_cd not in ({dqm.el_grp_72}) and elgblty_grp_cd='72' then 1 else 0 end) as numer_val
                    from
                        {dqm.taskprefix}_tmsis_elgblty_dtrmnt
                    where
                        msis_ident_num is not null
                    group by
                        msis_ident_num
                ) c
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def el334t(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'101' AS submodule
                    , sum(denom_val) as denom
                    , sum(numer_val) as numer
                    , case when sum(denom_val) > 0 then
                        round(sum(numer_val) / sum(denom_val), 3)
                        else null end as mvalue
                from (
                    select msis_ident_num
                        , max(case when submtg_state_cd not in ({dqm.el_grp_73_74_75}) then 1 else 0 end) as denom_val
                        , max(case when submtg_state_cd not in ({dqm.el_grp_73_74_75}) and elgblty_grp_cd in ('73', '74', '75') then 1 else 0 end) as numer_val
                    from
                        {dqm.taskprefix}_tmsis_elgblty_dtrmnt
                    where
                        msis_ident_num is not null
                    group by
                        msis_ident_num
                ) c
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def el335t(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'101' AS submodule
                    , sum(denom_val) as denom
                    , sum(numer_val) as numer
                    , case when sum(denom_val) > 0 then
                        round(sum(numer_val) / sum(denom_val), 3)
                        else null end as mvalue
                from (
                    select msis_ident_num
                        , max(case when submtg_state_cd in ({dqm.medicaid_el335}) then 1 else 0 end) as denom_val
                        , max(case when submtg_state_cd in ({dqm.medicaid_el335}) and elgblty_grp_cd in ('11') then 1 else 0 end) as numer_val
                    from
                        {dqm.taskprefix}_tmsis_elgblty_dtrmnt
                    where
                        msis_ident_num is not null
                    group by
                        msis_ident_num
                ) c
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def el336t(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'101' AS submodule
                    , sum(denom_val) as denom
                    , sum(numer_val) as numer
                    , case when sum(denom_val) > 0 then
                        round(sum(numer_val) / sum(denom_val), 3)
                        else null end as mvalue
                from (
                    select msis_ident_num
                        , max(case when submtg_state_cd in ({dqm.medicaid_el336}) then 1 else 0 end) as denom_val
                        , max(case when submtg_state_cd in ({dqm.medicaid_el336}) and elgblty_grp_cd in ('12') then 1 else 0 end) as numer_val
                    from
                        {dqm.taskprefix}_tmsis_elgblty_dtrmnt
                    where
                        msis_ident_num is not null
                    group by
                        msis_ident_num
                ) c
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def el626t(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'101' AS submodule
                    , sum(denom_val) as denom
                    , sum(numer_val) as numer
                    , case when sum(denom_val) > 0 then
                        round(sum(numer_val) / sum(denom_val), 3)
                        else null end as mvalue
                from (
                    select msis_ident_num
                        , max(case when rstrctd_bnfts_cd in ('3', 'G') then 1 else 0 end) as denom_val
                        , max(case when rstrctd_bnfts_cd in ('3', 'G') and
                            (dual_elgbl_cd not in ('01', '03', '05', '06') or dual_elgbl_cd is null)
                            then 1 else 0 end) as numer_val
                    from
                        {dqm.taskprefix}_tmsis_elgblty_dtrmnt
                    where
                        msis_ident_num is not null
                    group by
                        msis_ident_num
                ) a
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def el627t(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,'101' AS submodule
                    , sum(denom_val) as denom
                    , sum(numer_val) as numer
                    , case when sum(denom_val) > 0 then
                        round(sum(numer_val) / sum(denom_val), 3)
                        else null end as mvalue
                from (
                    select msis_ident_num
                        , max(case when dual_elgbl_cd in ('01', '03', '05', '06') then 1 else 0 end) as denom_val
                        , max(case when dual_elgbl_cd in ('01', '03', '05', '06') and (rstrctd_bnfts_cd not in ('3', 'G') or rstrctd_bnfts_cd is null)
                            then 1 else 0 end) as numer_val
                    from
                        {dqm.taskprefix}_tmsis_elgblty_dtrmnt
                    where
                        msis_ident_num is not null
                    group by
                        msis_ident_num
                ) a
             """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def el122t(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
            select
                '{dqm.state}' AS submtg_state_cd
                ,'{measure_id}' AS measure_id
                ,'101' AS submodule
                ,sum(case when has01=0 or alwaysnull=1 then 1 else 0 end) as numer
                ,count(1) as denom
                ,round(sum(case when has01=0 or alwaysnull=1 then 1 else 0 end) / count(1),2) as mvalue
            from (
                select
                    msis_ident_num
                    , max(case when elgbl_adr_type_cd = '01' then 1 else 0 end) as has01
                    , min(case when elgbl_adr_type_cd is null then 1 else 0 end) as alwaysnull
                    , max(case when msis_ident_num is not null then 1 else 0 end) as denom
                from
                    {dqm.taskprefix}_tmsis_elgbl_cntct
                group by
                    msis_ident_num
            ) a
             """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def el640t(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
            create or replace temporary view pivot1 as
            select msis_ident_num
                        ,max(enrol_01) as Enrollment_Status_01   
                        ,max(enrol_02) as Enrollment_Status_02
                        ,max(enrol_03) as Enrollment_Status_03
                        ,max(enrol_04) as Enrollment_Status_04
                        ,max(enrol_05) as Enrollment_Status_05
                        ,max(enrol_06) as Enrollment_Status_06
                        ,max(enrol_07) as Enrollment_Status_07
                        ,max(enrol_08) as Enrollment_Status_08
                        ,max(enrol_09) as Enrollment_Status_09
                        ,max(enrol_10) as Enrollment_Status_10
                        ,max(enrol_11) as Enrollment_Status_11
                        ,max(enrol_12) as Enrollment_Status_12
            from (select msis_ident_num, enrlmt_efctv_dt, enrlmt_end_dt
            ,case when enrlmt_efctv_dt <= DATEADD(month, -11, '{dqm.m_start}') and (enrlmt_end_dt >= DATEADD(month, -11, '{dqm.m_end}') or enrlmt_end_dt is NULL) then 1 else 0 end as enrol_01
            ,case when enrlmt_efctv_dt <= DATEADD(month, -10, '{dqm.m_start}') and (enrlmt_end_dt >= DATEADD(month, -10, '{dqm.m_end}') or enrlmt_end_dt is NULL) then 1 else 0 end as enrol_02
            ,case when enrlmt_efctv_dt <= DATEADD(month, -9, '{dqm.m_start}') and (enrlmt_end_dt >= DATEADD(month, -9, '{dqm.m_end}') or enrlmt_end_dt is NULL) then 1 else 0 end as enrol_03
            ,case when enrlmt_efctv_dt <= DATEADD(month, -8, '{dqm.m_start}') and (enrlmt_end_dt >= DATEADD(month, -8, '{dqm.m_end}') or enrlmt_end_dt is NULL) then 1 else 0 end as enrol_04
            ,case when enrlmt_efctv_dt <= DATEADD(month, -7, '{dqm.m_start}') and (enrlmt_end_dt >= DATEADD(month, -7, '{dqm.m_end}') or enrlmt_end_dt is NULL) then 1 else 0 end as enrol_05 
            ,case when enrlmt_efctv_dt <= DATEADD(month, -6, '{dqm.m_start}') and (enrlmt_end_dt >= DATEADD(month, -6, '{dqm.m_end}') or enrlmt_end_dt is NULL) then 1 else 0 end as enrol_06 
            ,case when enrlmt_efctv_dt <= DATEADD(month, -5, '{dqm.m_start}') and (enrlmt_end_dt >= DATEADD(month, -5, '{dqm.m_end}') or enrlmt_end_dt is NULL) then 1 else 0 end as enrol_07 
            ,case when enrlmt_efctv_dt <= DATEADD(month, -4, '{dqm.m_start}') and (enrlmt_end_dt >= DATEADD(month, -4, '{dqm.m_end}') or enrlmt_end_dt is NULL) then 1 else 0 end as enrol_08 
            ,case when enrlmt_efctv_dt <= DATEADD(month, -3, '{dqm.m_start}') and (enrlmt_end_dt >= DATEADD(month, -3, '{dqm.m_end}') or enrlmt_end_dt is NULL) then 1 else 0 end as enrol_09 
            ,case when enrlmt_efctv_dt <= DATEADD(month, -2, '{dqm.m_start}') and (enrlmt_end_dt >= DATEADD(month, -2, '{dqm.m_end}') or enrlmt_end_dt is NULL) then 1 else 0 end as enrol_10 
            ,case when enrlmt_efctv_dt <= DATEADD(month, -1, '{dqm.m_start}') and (enrlmt_end_dt >= DATEADD(month, -1, '{dqm.m_end}') or enrlmt_end_dt is NULL) then 1 else 0 end as enrol_11 
            ,case when enrlmt_efctv_dt <= DATEADD(month, 0, '{dqm.m_start}') and (enrlmt_end_dt >=  DATEADD(month, 0, '{dqm.m_end}') or enrlmt_end_dt is NULL) then 1 else 0 end as enrol_12  
            from {dqm.taskprefix}_ever_elig
            where enrlmt_efctv_dt <= '{dqm.m_end}' and (enrlmt_end_dt >= DATEADD(month, -12, '{dqm.m_end}') or enrlmt_end_dt is null) and enrlmt_type_cd in (1, 2))
            group by msis_ident_num
            """
        spark.sql(z)    
        z = f"""
            create or replace temporary view allenrol as
            select msis_ident_num, ROW_NUMBER() OVER(PARTITION BY msis_ident_num ORDER BY Enrollment_Month ASC) AS month, status
            from (select msis_ident_num, Enrollment_Status_01, Enrollment_Status_02, Enrollment_Status_03, Enrollment_Status_04, Enrollment_Status_05, Enrollment_Status_06, Enrollment_Status_07, Enrollment_Status_08,
                                            Enrollment_Status_09, Enrollment_Status_10, Enrollment_Status_11, Enrollment_Status_12  from pivot1) as p1
            unpivot (status for Enrollment_Month in (Enrollment_Status_01, Enrollment_Status_02, Enrollment_Status_03, Enrollment_Status_04, Enrollment_Status_05, Enrollment_Status_06, Enrollment_Status_07, Enrollment_Status_08,
                                            Enrollment_Status_09, Enrollment_Status_10, Enrollment_Status_11, Enrollment_Status_12)) as unpivot1
            order by msis_ident_num, month
            """
        spark.sql(z)        
        z = f"""
            create or replace temporary view gaps1 as
            select * from allenrol where status = 0
            order by msis_ident_num, month    
            """
        spark.sql(z) 
        z = f"""
            create or replace temporary view pre_gap as 
            select a.msis_ident_num, a.month, sum(b.status) as enrol_pregap from gaps1 a
            left join allenrol b on a.msis_ident_num = b.msis_ident_num where a.month > b.month
            group by a.msis_ident_num, a.month order by a.msis_ident_num, a.month
            """
        spark.sql(z)
        z = f"""
            create or replace temporary view post_gap as
            select a.msis_ident_num, a.month, sum(b.status) as enrol_postgap from gaps1 a
            left join allenrol b on a.msis_ident_num = b.msis_ident_num where a.month < b.month
            group by a.msis_ident_num, a.month order by a.msis_ident_num, a.month
            """
        spark.sql(z)    
        z = f"""
            create or replace temporary view gaps_first as
            select msis_ident_num, 1 as enrollment_gap from (
            select *, ROW_NUMBER() OVER(PARTITION BY msis_ident_num ORDER BY month ASC) AS gap_order from (
            select coalesce(a.msis_ident_num,b.msis_ident_num) as msis_ident_num, coalesce(a.month,b.month) as month, 
                   coalesce(enrol_pregap,0) as enrol_pregap, coalesce(enrol_postgap,0) as enrol_postgap 
            from post_gap a
            full join pre_gap b on a.msis_ident_num = b.msis_ident_num and a.month = b.month order by msis_ident_num, month) where enrol_pregap > 0 and enrol_postgap >0)
            where gap_order = 1
            """
        spark.sql(z)
        z = f"""
            select
                    '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'101' as submodule
                    ,coalesce(sum(enrollment_gap), 0) as numer
                    ,count(*) as denom
                    ,round(case when count(*) > 0 then (coalesce(sum(enrollment_gap), 0) / count(*)) else null end,{x['round']}) as mvalue
                from
                    pivot1 as p
                left join
                    gaps_first as h on p.msis_ident_num = h.msis_ident_num
            """        
        spark.sql(z)

        dqm.logger.debug(z)

        return spark.sql(z)        
    def el641t(spark, dqm: DQMeasures, measure_id, x) :

        z = f"""
        create or replace temporary view spells1 as
            select * ,max(enrlmt_end_dt) over (partition by msis_ident_num
                                    order by msis_ident_num, enrlmt_efctv_dt, enrlmt_end_dt
                                    rows unbounded preceding) as max_end_date_thus_far
            from (select distinct msis_ident_num, enrlmt_efctv_dt, enrlmt_end_dt
                    from {dqm.taskprefix}_ever_elig
                    where enrlmt_efctv_dt <= '{dqm.m_end}' and (enrlmt_end_dt > DATEADD(month, -12, '{dqm.m_end}') or enrlmt_end_dt is null) and enrlmt_type_cd in (1, 2))

            order by msis_ident_num,enrlmt_efctv_dt,enrlmt_end_dt
        """
        spark.sql(z)

        z = f"""
            create or replace temporary view spells2 as
            select a.*
                    ,case when tot_rec =1 then 1
                        when enrlmt_efctv_dt <= dateadd(day,1,prev_enrlmt_end_dt) then 0 else 1 end as enrlmt_span_st
            from (
                    select *
                        ,lag(max_end_date_thus_far) over (partition by msis_ident_num
                                                    order by msis_ident_num, enrlmt_efctv_dt, enrlmt_end_dt
                                                    ) as prev_enrlmt_end_dt
                        ,count(*) over (partition by msis_ident_num) as tot_rec
                    from spells1
                    order by msis_ident_num,enrlmt_efctv_dt,enrlmt_end_dt ) a

            order by msis_ident_num,enrlmt_efctv_dt, enrlmt_end_dt
            """
        spark.sql(z)

        z = f"""
            select  
            '{dqm.state}' as submtg_state_cd
            ,'{measure_id}' as measure_id
            ,'101' as submodule
            ,sum(case when tot_enrlmt_span > 3 then 1 else 0 end) as numer
            ,count(*) as denom
            ,case when count(*) > 0 then round(sum(case when tot_enrlmt_span > 3 then 1 else 0 end) / count(*),3) else null end as mvalue
            from (
                select msis_ident_num, sum(enrlmt_span_st) as tot_enrlmt_span from spells2
                group by msis_ident_num
                order by msis_ident_num)
            """

        spark.sql(z)        

        dqm.logger.debug(z)

        return spark.sql(z)  

                    
    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    v_table = {
        "el16": el16,
        "nonclaimspct": nonclaimspct,
        "nonclaimspct2tbl": nonclaimspct2tbl,
        "nonclaimspct2tblwvr": nonclaimspct2tblwvr,
        "nonclaimspctwvr": nonclaimspctwvr,
        "nonclaimspct_notany": nonclaimspct_notany,
        "el319t": el319t,
        "el333t": el333t,
        "el322t": el322t,
        "el334t": el334t,
        "el626t": el626t,
        "el627t": el627t,
        "el122t": el122t,
        "el335t": el335t,
        "el336t": el336t,
        "el640t": el640t,
        "el641t": el641t
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