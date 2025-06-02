# --------------------------------------------------------------------
#
#
#
# --------------------------------------------------------------------
from dqm.DQClosure import DQClosure
from dqm.DQMeasures import DQMeasures

class Runner_107:

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def run(spark, dqm: DQMeasures, measures: list = None) :
        dqm.logger.info('Module().run<series>.run() has been deprecated. Use dqm.run(spark) or dqm.run(spark, dqm.where(series="<series>")) instead.')
        if measures is None:
            return dqm.run(spark, dqm.reverse_measure_lookup[dqm.reverse_measure_lookup['series'] == '107']['measure_id'].tolist())
        else:
            return dqm.run(spark, measures)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def el_802(spark, dqm: DQMeasures, measure_id, x) :

        # Bring in unique plan IDs for MC & CLAIMS;

        dqm.logger.info("107_EL Getting plan ids")

        # Bring in MC PRTCPTN plan ids;
        z = f"""
                create or replace temporary view {dqm.taskprefix}_mc_plans as
                select distinct
                    mc_plan_id as plan_id
                from
                    {dqm.taskprefix}_tmsis_mc_prtcptn_data
            """
        dqm.logger.debug(z)
        spark.sql(z)


        # --------------------------------------------------------------------
        #
        #def do_clms(spark, dqm: DQMeasures, measure_id, x) :
        # --------------------------------------------------------------------
        
        def clm(ft, level):

            dqm.logger.info("107_EL Getting plan ids from claims")
            z = f"""
                    create or replace temporary view {dqm.taskprefix}_{ft} as
                    select distinct
                        plan_id_num as plan_id
                    from
                        {DQMeasures.getBaseTable(dqm, level, ft)}
                    where
                        clm_type_cd in ('3','C')
                        and adjstmt_ind = '0'
                    order by
                        plan_id
                """
            dqm.logger.debug(z)
            spark.sql(z)

        clm('ip', 'clh')
        clm('lt', 'clh')
        clm('rx', 'clh')
        clm('ot', 'cll')


        # --------------------------------------------------------------------
        #
        # FTX file
        # --------------------------------------------------------------------
        
        dqm.logger.info("107_EL Getting plan ids from FTX")
         # Bring in FTX plan ids;
        z = f"""
                create or replace temporary view {dqm.taskprefix}_ftx_plans as
                
                    select distinct
                        pyee_id as plan_id
                    from
                        {dqm.taskprefix}_tmsis_indvdl_cptatn_pmpm

                    WHERE pyee_id_type = '02' and
                        pyee_id is not null

                union 

                    select distinct
                        pyee_id as plan_id
                    from
                        {dqm.taskprefix}_tmsis_indvdl_hi_prm_pymt

                    WHERE pyee_id_type = '02' and
                        pyee_id is not null

                union 
                
                    select distinct
                        pyee_id as plan_id
                    from
                        {dqm.taskprefix}_tmsis_cst_shrng_ofst

                    WHERE ofst_trans_type  in ('1','2') and
                          pyee_id_type = '02' and
                         pyee_id is not null


            """
        dqm.logger.debug(z)
        spark.sql(z)



        # --------------------------------------------------------------------
        #
        #def plan_ids(spark, dqm: DQMeasures, measure_id, x) :
        # --------------------------------------------------------------------

        # note: this distinct is needed if multiple plan ids satisfy misslogicprv_id
        z = f"""
                create or replace temporary view {dqm.taskprefix}_plan_ids as
                select distinct
                    case when ({DQClosure.parse('%misslogicprv_id(plan_id,12) = 1')}) then null else plan_id end as plan_id
                from(
                    select distinct plan_id from {dqm.taskprefix}_ip ip
                    union
                    select distinct plan_id from {dqm.taskprefix}_lt lt
                    union
                    select distinct plan_id from {dqm.taskprefix}_ot ot
                    union
                    select distinct plan_id from {dqm.taskprefix}_rx rx
                    union
                    select distinct plan_id from {dqm.taskprefix}_mc_plans mc
                    union
                    select distinct plan_id from {dqm.taskprefix}_ftx_plans
                ) a
                order by plan_id
            """
        dqm.logger.debug(z)
        spark.sql(z)

        # create table plan_ids as
        # select * from connection to tmsis_passthrough
        # (select * from {dqm.taskprefix}_plan_ids);


        # --------------------------------------------------------------------
        #
        #def mc_data(spark, dqm: DQMeasures, measure_id, x) :
        # --------------------------------------------------------------------

        # SGO updated the logic for selecting plan_type_mc based on V1.2 update
        z = f"""
            create or replace temporary view {dqm.taskprefix}_mc_data_1 as
            select
                plan_id,
                plan_type_mc,
                row_number() over (partition by plan_id order by plan_id asc, plan_type_mc_cnt desc, plan_type_mc asc) as row
            from (
                select
                    state_plan_id_num as plan_id,
                    mc_plan_type_cd as plan_type_mc,
                    count(1) as plan_type_mc_cnt
                from (
                    SELECT
                        case when ({DQClosure.parse('(%misslogicprv_id(state_plan_id_num,12)=1)')})
                            then NULL else state_plan_id_num end as state_plan_id_num,
                        mc_plan_type_cd
                    FROM
                        {dqm.taskprefix}_tmsis_mc_mn_data
                    ) a
                group by
                    state_plan_id_num,
                    mc_plan_type_cd
                ) b
            """
        dqm.logger.debug(z)
        spark.sql(z)

        # create table mc_data_1 as
        # select * from connection to tmsis_passthrough
        # (select * from {dqm.taskprefix}_mc_data_1);

        z = f"""
                create or replace temporary view {dqm.taskprefix}_mc_data as
                select
                    *
                from (
                    select
                        a.*,
                        case when b.count_plan_types > 1 then 1 else 0 end as MultiplePlanTypes_mc
                    from
                        {dqm.taskprefix}_mc_data_1 a
                    left join
                        (select
                            plan_id,
                            count(*) as count_plan_types
                         from
                            {dqm.taskprefix}_mc_data_1
                        group by
                            plan_id) b
                        on coalesce(a.plan_id,'0') = coalesce(b.plan_id,'0')
                    ) c
                where row = 1
            """
        dqm.logger.debug(z)
        spark.sql(z)

        # create table mc_data as
        # select * from connection to tmsis_passthrough
        # (select * from {dqm.taskprefix}_mc_data);


        # --------------------------------------------------------------------
        #
        #def plan_link(spark, dqm: DQMeasures, measure_id, x) :
        # --------------------------------------------------------------------

        # Set Managed Care Plan Type from MC file if matching Plan ID
        z = f"""
                create or replace temporary view {dqm.taskprefix}_plan_link as
                select
                    a.plan_id
                    ,b.plan_type_mc
                    ,b.MultiplePlanTypes_mc
                    ,case when b.plan_type_mc is not null then 'YES' else 'NO' end as linked
                from
                    {dqm.taskprefix}_plan_ids a
                left join
                    {dqm.taskprefix}_mc_data b
                    on coalesce(a.plan_id,'0') = coalesce(b.plan_id,'0')
            """
        dqm.logger.debug(z)
        spark.sql(z)

        # --------------------------------------------------------------------
        #
        #def enrollment(spark, dqm: DQMeasures, measure_id, x) :
        # --------------------------------------------------------------------

        # (4) Count # of enrollees for the unique plan ID;

        z = f"""
                create or replace temporary view {dqm.taskprefix}_enroll_data_1 as
                select
                    plan_id,
                    plan_type_el,
                    row_number() over (partition by plan_id order by plan_id asc, plan_type_el_cnt desc, plan_type_el asc) as row
                from (
                    select
                        mc_plan_id as plan_id,
                        mc_plan_type_cd as plan_type_el,
                        count(*) as plan_type_el_cnt
                    from ( 
                        SELECT
                            case when {DQClosure.parse('(%misslogicprv_id(mc_plan_id,12)=1)')} then NULL else mc_plan_id end as mc_plan_id,
                            mc_plan_type_cd
                        FROM
                            {dqm.taskprefix}_tmsis_mc_prtcptn_data
                        ) a
                    group by
                        mc_plan_id,
                        mc_plan_type_cd
                ) b
            """
        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_enroll_data_2 as
                select *
                from (
                    select
                        a.*,
                        case when b.count_plan_types > 1 then 1 else 0 end as MultiplePlanTypes_el
                    from
                        {dqm.taskprefix}_enroll_data_1 a
                    left join (
                        select
                            plan_id,
                            count(1) as count_plan_types /* to flag multiple code id's */
                        from
                            {dqm.taskprefix}_enroll_data_1
                        group by
                            plan_id
                        ) b
                    on coalesce(a.plan_id,'0') = coalesce(b.plan_id,'0')
                ) c
                where row = 1
            """
        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_enroll_data as
                select
                    a.*,
                    c.enrollment
                from
                    {dqm.taskprefix}_enroll_data_2 a
                left join (
                    select
                        plan_id,
                        count(distinct msis_ident_num) as enrollment
                    from (
                        SELECT
                            case when {DQClosure.parse('(%misslogicprv_id(mc_plan_id,12)=1)')} then NULL else mc_plan_id end as plan_id,
                            msis_ident_num
                        FROM
                            {dqm.taskprefix}_tmsis_mc_prtcptn_data
                        where
                            mc_plan_enrlmt_efctv_dt is not NULL
                        ) b
                    group by
                        plan_id
                    ) c
                on coalesce(a.plan_id,'0') = coalesce(c.plan_id,'0')
            """
        dqm.logger.debug(z)
        spark.sql(z)

        # create table enroll_data as
        # select * from connection to tmsis_passthrough
        # (select * from {dqm.taskprefix}_enroll_data);

        z = f"""
                create or replace temporary view {dqm.taskprefix}_enrollment as
                select
                    a.plan_id
                    ,b.plan_type_el
                    ,b.MultiplePlanTypes_el
                    ,a.plan_type_mc
                    ,a.MultiplePlanTypes_mc
                    ,a.linked
                    ,b.enrollment
                from
                    {dqm.taskprefix}_plan_link a
                left join
                    {dqm.taskprefix}_enroll_data b
                    on coalesce(a.plan_id,'0') = coalesce(b.plan_id,'0')
            """
        dqm.logger.debug(z)
        spark.sql(z)

        dqm.logger.info("107_EL Getting cap claims")


        # --------------------------------------------------------------------
        #
        #def capitation(spark, dqm: DQMeasures, measure_id, x) :
        # --------------------------------------------------------------------

        # def cap_tables(dqm, table, stc_cd):
        #     z = f"""
        #             create or replace temporary view {dqm.taskprefix}_cap_{table} as
        #             select
        #                 case when {DQClosure.parse('(%misslogicprv_id(plan_id_num,12) = 1)')}
        #                     then null else plan_id_num end as plan_id
        #                 ,count(1) as cap_{table}
        #             from
        #                 {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
        #             where
        #                 stc_cd = '{stc_cd}' and
        #                 mdcd_pd_amt > 0 and
        #                 clm_type_cd in ('2','B') and
        #                 adjstmt_ind = '0'
        #             group by
        #                 plan_id
        #         """
        #     dqm.logger.debug(z)
        #     spark.sql(z)

            # create table cap_&table. as
            # select * from connection to tmsis_passthrough
            # (select * from {dqm.taskprefix}_cap_&table.);

        #cap_tables(dqm, 'hmo', 119)  # (5) Count # of capitation HMO/HIO/PACE payments for plan ID;
        #cap_tables(dqm, 'php', 122)  # (6) Count # of capitation PHP payments for plan ID;
        #cap_tables(dqm, 'pccm', 120) # (7) Count # of capitation PCCM payments for plan ID;
        #cap_tables(dqm, 'phi', 121)  # (8) Count # of capitation PHI payments for plan ID;

        def ftx_cap_tables(dqm, cap_cat, ftx_tbl1, cap_cond1, ftx_tbl2, cap_cond2):

            ftx_de_dup_vars=f"""submtg_state_cd
                        ,orgnl_clm_num
                        ,adjstmt_clm_num
                        ,pymt_or_rcpmt_dt
                        ,adjstmt_ind
                    """
            
            p=f"""
               
                select {ftx_de_dup_vars}
                        ,case when ({DQClosure.parse('%misslogicprv_id(pyee_id,12) = 1')})
                       
                            then null else pyee_id end as plan_id
           
                from  {dqm.taskprefix}_{ftx_tbl1}

                WHERE {cap_cond1} 
                            pymt_or_rcpmt_amt  > 0 and
                            adjstmt_ind = '0'

                group by {ftx_de_dup_vars}
                                ,plan_id
                """
            if f"{ftx_tbl2}" == "":
                p += f""""""
            else:
                p +=f"""
                    union 

                    select {ftx_de_dup_vars}
                            ,case when ({DQClosure.parse('%misslogicprv_id(pyee_id,12) = 1')})
                            then null else pyee_id end as plan_id
            
                    from  {dqm.taskprefix}_{ftx_tbl2}

                    WHERE {cap_cond2} 
                          pymt_or_rcpmt_amt  > 0 and
                          adjstmt_ind = '0'

                    group by {ftx_de_dup_vars}
                                    ,plan_id
                    """
                            
            print(p)

            z=f"""create or replace temporary view {dqm.taskprefix}_cap_{cap_cat} as
                  select plan_id
                        ,count(1) as cap_{cap_cat}
            
                  from (select {ftx_de_dup_vars}
                               ,plan_id
                        from ({p}) a
                        group by {ftx_de_dup_vars}
                                 ,plan_id
                        ) b
                  group by plan_id
                  """
            
            dqm.logger.debug(z)
            
            spark.sql(z)

        ftx_cap_tables(dqm, 'hmo', 'tmsis_indvdl_cptatn_pmpm', "pyee_id_type = '02' and pyee_mcr_plan_type in ('01', '04','17') and",\
                        '', '')

        ftx_cap_tables(dqm, 'php','tmsis_indvdl_cptatn_pmpm', "pyee_id_type = '02' and pyee_mcr_plan_type in ('05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19') and", \
                        '', '')

        ftx_cap_tables(dqm, 'pccm','tmsis_indvdl_cptatn_pmpm', "pyee_id_type = '02' and pyee_mcr_plan_type in ('02', '03')  and",\
                       '', '')
       
        ftx_cap_tables(dqm, 'phi','tmsis_indvdl_hi_prm_pymt', "pyee_id_type = '02' and ",\
                       'tmsis_cst_shrng_ofst', "pyee_id_type ='02' and ofst_trans_type ='2' and" )
       
                    
        ftx_cap_tables(dqm,'oth', 'tmsis_indvdl_cptatn_pmpm', \
                            "(pyee_id_type = '02' and  \
                              (pyee_mcr_plan_type is null or \
                               pyee_mcr_plan_type not in ('01','02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19')) )  and" ,\
                            
                            'tmsis_cst_shrng_ofst',  \
                            "(pyee_id_type = '02' and  ofst_trans_type in ('1') and \
                             (pyee_mcr_plan_type is null or \
                              pyee_mcr_plan_type not in ('01','02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19')) )  and" \
                           )
        
        


        # def ftx_cap_tot_tables(dqm, cap_cat):

        #     if {cap_cat} == 'hmo' or {cap_cat} == 'php' or {cap_cat} == 'pccm':

        #         z=f"""  create or replace temporary view {dqm.taskprefix}_cap_{cap_cat} as

        #             select plan_id
        #                 ,coalesce(cap_{cap_cat}_pyee,0) as cap_{cap_cat}
            
        #             from {dqm.taskprefix}_cap_{cap_cat}_pyee a

        #             """
        #     else:

        #         z=f"""  create or replace temporary view {dqm.taskprefix}_cap_{cap_cat} as

        #         select coalesce(a.plan_id, b.plan_id) as plan_id
        #               ,coalesce(b.cap_{cap_cat}_pyee_1,0) + coalesce(d.cap_{cap_cat}_pyee_2,0) as cap_{cap_cat}
           
                
        #         from {dqm.taskprefix}_cap_{cap_cat}_pyee1 a

        #         full join  {dqm.taskprefix}_cap_{cap_cat}_pyee2 b

        #         on a.plan_id=b.plan_id
        #         """
        #     dqm.logger.debug(z)
        #     spark.sql(z)

        # ftx_cap_tot_tables(dqm, 'hmo')
        # ftx_cap_tot_tables(dqm, 'php')
        # ftx_cap_tot_tables(dqm, 'pccm')
        # ftx_cap_tot_tables(dqm, 'phi')
        # ftx_cap_tot_tables(dqm, 'oth')

        
        # (9) Count # of capitation OTHER payments for plan ID;
        #z = f"""
        #        create or replace temporary view {dqm.taskprefix}_cap_oth as
        #        select
        #            case when {DQClosure.parse('(%misslogicprv_id(plan_id_num,12) = 1)')}
        #                    then null else plan_id_num end as plan_id
        #            ,count(1) as cap_oth
        #        from
        #            {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
        #        where
        #            stc_cd not in ('119','120','121','122') and
        #            mdcd_pd_amt > 0 and
        #            clm_type_cd in ('2','B') and
        #            adjstmt_ind = '0'
        #        group by
        #            plan_id
        #    """
        #dqm.logger.debug(z)
        #spark.sql(z)

        # create table cap_oth as
        # select * from connection to tmsis_passthrough
        # (select * from {dqm.taskprefix}_cap_oth);

        # z = f"""
        #         create or replace temporary view {dqm.taskprefix}_cap_type as
        #         select
        #             case when {DQClosure.parse('(%misslogicprv_id(plan_id_num,12) = 1)')}
        #             then null else plan_id_num end as plan_id,
        #             max(case when clm_type_cd = '2' then 1 else 0 end) as clm_type_cd_2,
        #             max(case when clm_type_cd = 'B' then 1 else 0 end) as clm_type_cd_B
        #         from
        #             {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
        #         where
        #             mdcd_pd_amt > 0 and
        #             clm_type_cd in ('2','B') and
        #             adjstmt_ind = '0'
        #         group by
        #             plan_id
        #     """
        # dqm.logger.debug(z)
        # spark.sql(z)
        
        dqm.logger.info("107_EL Getting mdcd and schip flags")
        z = f"""
                create or replace temporary view {dqm.taskprefix}_cap_type as

                select plan_id
                       ,max(mdcd_flag) as mdcd_flag
                       ,max(schip_flag) as schip_flag
                from (
                    select
                        case when ({DQClosure.parse('%misslogicprv_id(pyee_id,12) = 1')})
                        then null else pyee_id end as plan_id,
                        max(case when mbescbes_form_grp  in ('1','2') then 1 else 0 end) as mdcd_flag,
                        max(case when mbescbes_form_grp in ('3') then 1 else 0 end) as schip_flag
                    from
                        {dqm.taskprefix}_tmsis_indvdl_cptatn_pmpm
                    where
                        pymt_or_rcpmt_amt  > 0 and
                        adjstmt_ind = '0'
                    group by
                        plan_id

                    union all

                    select
                        case when ({DQClosure.parse('%misslogicprv_id(pyee_id,12) = 1')})
                        then null else pyee_id end as plan_id,
                        max(case when mbescbes_form_grp  in ('1','2') then 1 else 0 end) as mdcd_flag,
                        max(case when mbescbes_form_grp in ('3') then 1 else 0 end) as schip_flag
                    from
                        {dqm.taskprefix}_tmsis_indvdl_hi_prm_pymt
                    where
                        pymt_or_rcpmt_amt  > 0 and
                        adjstmt_ind = '0'
                    group by
                        plan_id

                    union all

                    select
                        case when ({DQClosure.parse('%misslogicprv_id(pyee_id,12) = 1')})
                        then null else pyee_id end as plan_id,
                        max(case when mbescbes_form_grp  in ('1','2') then 1 else 0 end) as mdcd_flag,
                        max(case when mbescbes_form_grp in ('3') then 1 else 0 end) as schip_flag
                    from
                        {dqm.taskprefix}_tmsis_cst_shrng_ofst
                    where
                        pymt_or_rcpmt_amt  > 0 and
                        adjstmt_ind = '0'
                    group by
                        plan_id



                    ) a


                group by
                        plan_id


            """
        dqm.logger.debug(z)
        spark.sql(z)

     
        # create table cap_type as
        # select * from connection to tmsis_passthrough
        # (select * from {dqm.taskprefix}_cap_type);

        z = f"""
            create or replace temporary view {dqm.taskprefix}_capitation as
            select
                 a.plan_id
                ,a.plan_type_el
                ,a.MultiplePlanTypes_el
                ,a.plan_type_mc
                ,a.MultiplePlanTypes_mc
                ,a.linked
                ,coalesce(a.enrollment,0) as enrollment
                ,coalesce(b.cap_hmo,0) as cap_hmo
                ,coalesce(c.cap_php,0) as cap_php
                ,coalesce(d.cap_pccm,0) as cap_pccm
                ,coalesce(e.cap_phi,0) as cap_phi
                ,coalesce(f.cap_oth,0) as cap_oth
                ,(coalesce(b.cap_hmo,0) + coalesce(c.cap_php,0) + coalesce(d.cap_pccm,0)
                    + coalesce(e.cap_phi,0) + coalesce(f.cap_oth,0)) as cap_tot
                ,case 	when g.mdcd_flag = 1 and g.schip_flag = 1 then 'Medicaid & S-CHIP'
                        when g.mdcd_flag = 1 and g.schip_flag = 0 then 'Medicaid'
                        when g.mdcd_flag = 0 and g.schip_flag = 1 then 'S-CHIP'
                        else 'No cap' end as capitation_type
            from {dqm.taskprefix}_enrollment a
            left join {dqm.taskprefix}_cap_hmo  b on coalesce(a.plan_id,'0') = coalesce(b.plan_id,'0')
            left join {dqm.taskprefix}_cap_php  c on coalesce(a.plan_id,'0') = coalesce(c.plan_id,'0')
            left join {dqm.taskprefix}_cap_pccm d on coalesce(a.plan_id,'0') = coalesce(d.plan_id,'0')
            left join {dqm.taskprefix}_cap_phi  e on coalesce(a.plan_id,'0') = coalesce(e.plan_id,'0')
            left join {dqm.taskprefix}_cap_oth  f on coalesce(a.plan_id,'0') = coalesce(f.plan_id,'0')
            left join {dqm.taskprefix}_cap_type g on coalesce(a.plan_id,'0') = coalesce(g.plan_id,'0')
        """
        dqm.logger.debug(z)
        spark.sql(z)


        # --------------------------------------------------------------------
        #
        #def encounters(spark, dqm: DQMeasures, measure_id, x) :
        # --------------------------------------------------------------------

        # /***ENCOUNTERS: IP/LT/OT/RX****/

        dqm.logger.info("107_EL Getting encounter claims")

        def encounter_tables(ft, level):
            z = f"""
                create or replace temporary view {dqm.taskprefix}_ent_{ft} as
                select
                    case when {DQClosure.parse('(%misslogicprv_id(plan_id_num,12) = 1)')}
                        then null else plan_id_num end as plan_id,
                        max(case when clm_type_cd = '3' then 1 else 0 end) as clm_type_cd_3_{ft},
                    max(case when clm_type_cd = 'C' then 1 else 0 end) as clm_type_cd_C_{ft},
                    count(1) as NUM_{ft}
                from
                    {DQMeasures.getBaseTable(dqm, level, ft)}
                where
                    clm_type_cd in ('3','C') and
                    adjstmt_ind = '0'
                group by
                    plan_id
            """
            dqm.logger.debug(z)
            spark.sql(z)

        # (11) IP: Count # of encounters in claims file for plan ID;
        # (16) IP: Obtain Ratio of Number of Encounters to Number of Enrollees for Plan ID;
        encounter_tables('ip', 'clh')

        # (12) LT: Count # of encounters in claims file for plan ID;
        # (17) LT: Obtain Ratio of Number of Encounters to Number of Enrollees for Plan ID;
        encounter_tables('lt', 'clh')

        # (13) OT: Count # of encounters in claims file for plan ID*;
        # (18) OT: Obtain Ratio of Number of Encounters to Number of Enrollees for Plan ID;
        encounter_tables('ot', 'cll') # use line level for OT;

        # (14) RX: Count # of encounters in claims file for plan ID;
        # (19) RX: Obtain Ratio of Number of Encounters to Number of Enrollees for Plan ID;
        encounter_tables('rx', 'clh')


        # --------------------------------------------------------------------
        #
        #def el_802(spark, dqm: DQMeasures, measure_id, x) :
        # --------------------------------------------------------------------

        dqm.logger.info("107_EL Calculating table EL8.2")
        z = f"""
                select
                    '{dqm.state}' AS submtg_state_cd
                    ,'{measure_id}' AS measure_id
                    ,null as numer
                    ,null as denom
                    ,null as mvalue
                    ,'107' as submodule
                    ,a.*
                    ,coalesce(plan_type_el,plan_type_mc,'99') as plan_type
                    ,coalesce(b.num_ip,0) as enc_ip
                    ,coalesce(c.num_lt,0) as enc_lt
                    ,coalesce(d.num_ot,0) as enc_ot
                    ,coalesce(e.num_rx,0) as enc_rx
                    ,(coalesce(num_ip,0) + coalesce(num_lt,0) + coalesce(num_ot,0) + coalesce(num_rx,0)) as enc_tot
                    ,case when enrollment > 0 then (coalesce(b.num_ip,0)/enrollment) else null end as ip_ratio
                    ,case when enrollment > 0 then (coalesce(c.num_lt,0)/enrollment) else null end as lt_ratio
                    ,case when enrollment > 0 then (coalesce(d.num_ot,0)/enrollment) else null end as ot_ratio
                    ,case when enrollment > 0 then (coalesce(e.num_rx,0)/enrollment) else null end as rx_ratio
                    ,case when enrollment > 0 then cap_tot/enrollment else null end as cap_ratio
                    ,case when (b.clm_type_cd_3_ip = 1 or c.clm_type_cd_3_lt = 1 or d.clm_type_cd_3_ot = 1 or e.clm_type_cd_3_rx = 1)
                            and (b.clm_type_cd_C_ip = 1 or c.clm_type_cd_C_lt = 1 or d.clm_type_cd_C_ot = 1 or e.clm_type_cd_C_rx = 1)
                            then 'Medicaid & S-CHIP'
                    when (b.clm_type_cd_3_ip = 1 or c.clm_type_cd_3_lt = 1 or d.clm_type_cd_3_ot = 1 or e.clm_type_cd_3_rx = 1)
                            then 'Medicaid'
                    when (b.clm_type_cd_C_ip = 1 or c.clm_type_cd_C_lt = 1 or d.clm_type_cd_C_ot = 1 or e.clm_type_cd_C_rx = 1)
                            then 'S-CHIP'
                                else 'No enc' end as encounter_type
                from {dqm.taskprefix}_capitation a
                left join {dqm.taskprefix}_ent_ip b on coalesce(a.plan_id,'0') = coalesce(b.plan_id,'0')
                left join {dqm.taskprefix}_ent_lt c on coalesce(a.plan_id,'0') = coalesce(c.plan_id,'0')
                left join {dqm.taskprefix}_ent_ot d on coalesce(a.plan_id,'0') = coalesce(d.plan_id,'0')
                left join {dqm.taskprefix}_ent_rx e on coalesce(a.plan_id,'0') = coalesce(e.plan_id,'0')
            """
        dqm.logger.debug(z)
        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    v_table = {"el_802": el_802}

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