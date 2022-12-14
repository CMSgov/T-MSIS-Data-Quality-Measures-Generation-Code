from pandas import pandas as pd
from pandas import DataFrame

def create_run_101_input(
    series: str, cb: str, measure: str, id: str, 
    numerator: str = '', numerator_table: str = '', 
    denominator: str = '', denominator_table: str = '', 
    table: str = '', rounding: int = 2
    ):
    """
        generates an input item in the list below. existing measures are defined as a list of unnamed args, 
        but this function should be used from V3.4 onwards.  functionally, both approaches are the same, 
        however utilizing this helper function helps ensure arguments are provided in the correct order.

        Args:
            series: in this file, measures should be series '101'
            cb: callback function to call using these values
            measure: measure id, e.g. EL1.37
            id: string id with no `.`, e.g. el137t
            numerator: columns to select as the numerator
            numerator_table: table to select the numerator from
            denominator: columns to select as the denominator
            denominator_table: table to select the denominator from
            table:
            rounding: number of decimal places to round to
    """
    return [
        series,
        cb,
        measure,
        id,
        numerator,
        numerator_table,
        denominator,
        denominator_table,
        table,
        rounding
    ]

run_101 =[

    ['101', 'nonclaimspct', 'el1.1', 'el101t',
        "case when (%ssn_nmisslogic(ssn_num,9) = 1) and (%nmisslogic(msis_ident_num,20) = 1) then 1 else 0 end",
        '',
        "msis_ident_num is not null",
        '',
        '_tmsis_var_dmgrphc_elgblty',
        2],

    ['101', 'nonclaimspct', 'el1.2', 'el102t',
        "case when (%ssn_nmisslogic(ssn_num,9) = 1) and ssn_vrfctn_ind='1' then 1 else 0 end",
        '',
        "msis_ident_num is not null",
        '',
        '_tmsis_var_dmgrphc_elgblty',
        2],

    ['101', 'nonclaimspct', 'el1.6', 'el106t',
        "case when gndr_cd='F' then 1 else 0 end",
        '',
        "msis_ident_num is not null",
        '',
        '_tmsis_prmry_dmgrphc_elgblty',
        2],

    ['101', 'nonclaimspct', 'el1.9', 'el109t',
        "case when (ethncty_cd not in ('0','1','2','3','4','5') or ethncty_cd is null) then 1 else 0 end",
        '',
        'msis_ident_num is not null',
        '',
        '_tmsis_ethncty_info',
        2],

    ['101', 'nonclaimspct', 'el1.10', 'el110t',
        "case when (race_cd not in ('001','002','003','004','005','006','007','008','009','010','011','012','013','014','015','016','018') or race_cd is null) then 1 else 0 end",
        '',
        'msis_ident_num is not null',
        '',
        '_tmsis_race_info',
        2],

    ['101', 'nonclaimspct', 'el1.11', 'el111t',
        "case when race_cd='003' then 1 else 0 end",
        '',
        "crtfd_amrcn_indn_alskn_ntv_ind='2'",
        '',
        '_tmsis_race_info',
        2],

    ['101', 'nonclaimspct', 'el1.32', 'el132t',
        "case when ctznshp_vrfctn_ind='1' then 1 else 0 end",
        '',
        "ctznshp_ind='1' AND msis_ident_num is not null",
        '',
        '_tmsis_var_dmgrphc_elgblty',
        2],

    ['101', 'nonclaimspct', 'el1.14', 'el114t',
        "case when (imgrtn_vrfctn_ind='1') then 1 else 0 end",
        '',
        "imgrtn_stus_cd in ('1','2','3')",
        '',
        '_tmsis_var_dmgrphc_elgblty',
        2],

    ['101', 'nonclaimspct', 'el1.15', 'el115t',
        "case when death_dt >= '{m_start}' and death_dt <= '{m_end}' then 1 else 0 end",
        '',
        "msis_ident_num is not null",
        '',
        '_elig_in_month_prmry',
        4],

    ['101', 'nonclaimspct', 'el1.16', 'el116t',
        '%nmisslogic(msis_case_num,12)',
        '',
        "msis_ident_num is not null",
        '',
        '_tmsis_elgblty_dtrmnt',
        2],

    ['101', 'nonclaimspct', 'el1.17', 'el117t',
        "case when age = 0 then 1 else 0 end",
        '',
        "msis_ident_num is not null",
        '',
        '_tmsis_prmry_dmgrphc_elgblty',
        4],

    ['101', 'nonclaimspct', 'el1.18', 'el118t',
        "case when age >=0 and age <= 20 then 1 else 0 end",
        '',
        "msis_ident_num is not null",
        '',
        '_tmsis_prmry_dmgrphc_elgblty',
        2],

    ['101', 'nonclaimspct', 'el1.19', 'el119t',
        "case when age >=65 then 1 else 0 end",
        '',
        "msis_ident_num is not null",
        '',
        '_tmsis_prmry_dmgrphc_elgblty',
        4],

    ['101', 'nonclaimspct2tbl', 'el3.2', 'el302t',
        "case when enrld_mc_plan_type_cd='01' then 1 else 0 end",
        '_tmsis_mc_prtcptn_data',
        'age >= 65',
        '_tmsis_prmry_dmgrphc_elgblty',
        '',
        2],

    ['101', 'nonclaimspct2tbl', 'el3.4', 'el304t',
        "case when age >= 13 and age <= 64 then 1 else 0 end",
        '_tmsis_prmry_dmgrphc_elgblty',
        "elgblty_grp_cd in ('05','53','67','68')",
        '_tmsis_elgblty_dtrmnt',
        '',
        2],

    ['101', 'nonclaimspct2tbl', 'el3.5', 'el305t',
        "case when age < 26 then 1 else 0 end",
        '_tmsis_prmry_dmgrphc_elgblty',
        "elgblty_grp_cd in ('08','09','30')",
        '_tmsis_elgblty_dtrmnt',
        '',
        2],

    ['101', 'nonclaimspct2tbl', 'el3.6', 'el306t',
        "case when age >= 16 and age <= 64 then 1 else 0 end",
        '_tmsis_prmry_dmgrphc_elgblty',
        "elgblty_grp_cd in ('48','49')",
        '_tmsis_elgblty_dtrmnt',
        '',
        2],

    ['101', 'nonclaimspct', 'el3.7', 'el307t',
        "case when dual_elgbl_cd in ('01','02','03','04','05','06','08','09','10') then 1 else 0 end",
        '',
        "elgblty_grp_cd in ('23','24','25','26') and msis_ident_num is not null",
        '',
        '_tmsis_elgblty_dtrmnt',
        2],

    ['101', 'nonclaimspct', 'el3.8', 'el308t',
        "case when elgblty_grp_cd in ('08','09','30') then 1 else 0 end",
        '',
        'msis_ident_num is not null',
        '',
        '_tmsis_elgblty_dtrmnt',
        2],

    ['101', 'nonclaimspct2tbl', 'el3.9', 'el309t',
        "case when age>=16 and age< 65 then 1 else 0 end",
        '_tmsis_prmry_dmgrphc_elgblty',
        "elgblty_grp_cd = '34'",
        '_tmsis_elgblty_dtrmnt',
        '',
        2],

    ['101', 'nonclaimspct2tbl', 'el3.10', 'el310t',
        "case when gndr_cd = 'F' then 1 else 0 end",
        '_tmsis_prmry_dmgrphc_elgblty',
        "elgblty_grp_cd = '34'",
        '_tmsis_elgblty_dtrmnt',
        '',
        2],

    ['101', 'nonclaimspct2tbl', 'el3.11', 'el311t',
        "case when dual_elgbl_cd in ('01','02','03','04','05','06','08','09','10') then 1 else 0 end",
        '_tmsis_elgblty_dtrmnt',
        'age >= 65',
        '_tmsis_prmry_dmgrphc_elgblty',
        '',
        2],

    ['101', 'nonclaimspct', 'el3.12', 'el312t',
        "case when (%misslogic(elgblty_grp_cd,2) = 1) or elgblty_grp_cd not in \
            ('01','02','03','04','05','06','07','08','09', \
            '11','12','13','14','15','16','17','18','19','20', \
            '21','22','23','24','25','26','27','28','29','30', \
            '31','32','33','34','35','36','37','38','39','40', \
            '41','42','43','44','45','46','47','48','49','50', \
            '51','52','53','54','55','56','59','60', \
            '61','62','63','64','65','66','67','68','69','70', \
            '71','72','73','74','75','76') then 1 else 0 end",
        '',
        "msis_ident_num is not null",
        '',
        '_tmsis_elgblty_dtrmnt',
        2],

    ['101', 'nonclaimspct', 'el3.15', 'el315t',
        "case when dual_elgbl_cd in ('01','02','03','04','05','06','08','09','10') then 1 else 0 end",
        'msis_ident_num is not null',
        '',
        '',
        '_tmsis_elgblty_dtrmnt',
        2],

    ['101', 'nonclaimspct2tbl', 'el3.16', 'el316t',
        "case when b.enrlmt_type_cd='2' then 1 else 0 end",
        '_tmsis_enrlmt_time_sgmt_data',
        "chip_cd = '2'",
        '_tmsis_var_dmgrphc_elgblty',
        '',
        2],

    ['101', 'nonclaimspct2tbl', 'el10.6', 'el1006t',
        "case when enrld_mc_plan_type_cd='01' then 1 else 0 end",
        '_tmsis_mc_prtcptn_data',
        "chip_cd in ('2','3')",
        '_tmsis_var_dmgrphc_elgblty',
        '',
        2],

    ['101', 'nonclaimspct2tbl', 'el11.1', 'el1101t',
        "case when tpl_insrnc_cvrg_ind='1' then 1 else 0 end",
        '_tmsis_tpl_mdcd_prsn_mn',
        "dual_elgbl_cd in ('02','04','08')",
        '_tmsis_elgblty_dtrmnt',
        '',
        2],

    ['101', 'nonclaimspct2tbl', 'el10.5', 'el1005t',
        "case when enrld_mc_plan_type_cd='01' then 1 else 0 end",
        '_tmsis_mc_prtcptn_data',
        "rstrctd_bnfts_cd in ('2','3','4','5','6')",
        '_tmsis_elgblty_dtrmnt',
        '',
        2],

    ['101', 'nonclaimspct2tbl', 'el6.22', 'el622t',
        "case when rstrctd_bnfts_cd='6' then 1 else 0 end",
        '_tmsis_elgblty_dtrmnt',
        "wvr_type_cd='24'",
        '_tmsis_wvr_prtcptn_data',
        '',
        2],

    ['101', 'nonclaimspct', 'el10.8', 'el1008t',
        '%misslogic(enrld_mc_plan_type_cd,2)',
        '',
        '(%nmisslogic(mc_plan_id, 12) = 1)',
        '',
        '_tmsis_mc_prtcptn_data',
        2],

    ['101', 'nonclaimspct', 'el10.7', 'el1007t',
        '%misslogic(mc_plan_id, 12)',
        '',
        "enrld_mc_plan_type_cd in \
            ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', \
             '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', \
             '', \
             '60', '70', '80')",
        '',
        '_tmsis_mc_prtcptn_data',
        2],

    ['101', 'nonclaimspct', 'el1.21', 'el121t',
        "case when age > 120 or age < -1 then 1 else 0 end",
        '',
        'msis_ident_num is not null',
        '',
        '_tmsis_prmry_dmgrphc_elgblty',
        3],

    ['101', 'nonclaimspct2tbl', 'el3.17', 'el317t',
        "case when (elgblty_grp_cd is null or elgblty_grp_cd not in ('31', '61', '62', '63', '64', '65', '66', '67', '68') then 1 else 0 end",
        '_tmsis_elgblty_dtrmnt',
        "chip_cd in ('2', '3')",
        '_tmsis_var_dmgrphc_elgblty',
        '',
        3],

    ['101', 'nonclaimspct2tbl', 'el3.18', 'el318t',
        "case when (elgblty_grp_cd in ('61', '62', '63', '64', '65', '66', '67', '68')) then 1 else 0 end",
        '_tmsis_elgblty_dtrmnt',
        "chip_cd = '1'",
        '_tmsis_var_dmgrphc_elgblty',
        '',
        3],

    ['101', 'nonclaimspct', 'el6.25', 'el625t',
        "case when elgblty_grp_cd is null or elgblty_grp_cd not in ('35', '70') then 1 else 0 end",
        '',
        "rstrctd_bnfts_cd = '6'",
        '',
        '_tmsis_elgblty_dtrmnt',
        2],

    ['101', 'el16', 'el16.1', '', '', '', '', '', 'tmsis_prmry_dmgrphc_elgblty_view'],
    ['101', 'el16', 'el16.2', '', '', '', '', '', 'tmsis_var_dmgrphc_elgblty_view'],
    ['101', 'el16', 'el16.3', '', '', '', '', '', 'tmsis_elgbl_cntct_view'],
    ['101', 'el16', 'el16.4', '', '', '', '', '', 'tmsis_elgblty_dtrmnt_view'],
    ['101', 'el16', 'el16.5', '', '', '', '', '', 'tmsis_wvr_prtcptn_data_view'],
    ['101', 'el16', 'el16.6', '', '', '', '', '', 'tmsis_mc_prtcptn_data_view'],
    ['101', 'el16', 'el16.7', '', '', '', '', '', 'tmsis_ethncty_info_view'],
    ['101', 'el16', 'el16.8', '', '', '', '', '', 'tmsis_race_info_view'],
    ['101', 'el16', 'el16.9', '', '', '', '', '', 'tmsis_enrlmt_time_sgmt_data_view'],

    #-------------------------------------------------------
    ['101', 'el319t', 'el3.19', 'el319t', '', '', '', '', ''],
    ['101', 'el333t', 'el3.33', 'el333t', '', '', '', '', ''],
    ['101', 'el322t', 'el3.22', 'el322t', '', '', '', '', ''],
    ['101', 'el334t', 'el3.34', 'el334t', '', '', '', '', ''],
    ['101', 'el335t', 'el3.35', 'el335t', '', '', '', '', ''],
    ['101', 'el336t', 'el3.36', 'el336t', '', '', '', '', ''],
    #-------------------------------------------------------

    ['101', 'el626t', 'el6.26', 'el626t', '', '', '', '', ''],
    ['101', 'el627t', 'el6.27', 'el627t', '', '', '', '', ''],

    ['101', 'nonclaimspct2tbl', 'el17.1', 'el1701t'
        , "case when b.msis_id is null then 1 else 0 end"
        , '_tmsis_prmry_dmgrphc_elgblty'
        , "a.msis_ident_num is not null"
        , '_tmsis_enrlmt_time_sgmt_data'
        , ''
        , 3],

    ['101', 'nonclaimspct2tbl', 'el17.2', 'el1702t'
        , "case when b.msis_id is null then 1 else 0 end"
        , '_tmsis_var_dmgrphc_elgblty'
        , "a.msis_ident_num is not null"
        , '_tmsis_enrlmt_time_sgmt_data'
        , ''
        , 3],

    ['101', 'nonclaimspct2tbl', 'el17.3', 'el1703t'
        , "case when b.msis_id is null then 1 else 0 end"
        , '_tmsis_elgblty_dtrmnt'
        , "a.msis_ident_num is not null"
        , '_tmsis_enrlmt_time_sgmt_data'
        , ''
        , 3],

    ['101', 'nonclaimspct2tbl', 'el3.25', 'el325t'
        , "case when elgblty_grp_cd is null or elgblty_grp_cd not in ('07', '31', '61') then 1 else 0 end"
        , '_tmsis_elgblty_dtrmnt'
        , "chip_cd = '2'"
        , '_tmsis_var_dmgrphc_elgblty'
        , ''
        , 3],

    ['101', 'nonclaimspct2tbl', 'el3.26', 'el326t'
        , "case when elgblty_grp_cd is null or elgblty_grp_cd not in ('61', '62', '63', '64', '65', '66', '67', '68') then 1 else 0 end"
        , '_tmsis_elgblty_dtrmnt'
        , "chip_cd = '3'"
        , '_tmsis_var_dmgrphc_elgblty'
        , ''
        , 3],

    ['101', 'nonclaimspctwvr', 'el6.28', 'el628t'
        , "case when substring(wvr_id, 1, 5) not in ('11-W-', '21-W-') or concat(substring(wvr_id, 6, 5), substring(wvr_id, 12)) not rlike '^[0-9]+$' or substring(wvr_id, 11, 1) <> '/' or wvr_id is null then 1 else 0 end"
        , ''
        , "wvr_type_cd in ('01', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30')"
        , '_tmsis_wvr_prtcptn_data'
        , ''
        , 3],


    ['101', 'nonclaimspctwvr', 'el6.29', 'el629t'
        , "case when substring(wvr_id, 1, 1) not rlike '[A-Za-z]' or substring(wvr_id, 3, 1) <> '.' or substring(wvr_id, 4) not rlike '^[0-9]+$' or wvr_id is null then 1 else 0 end"
        ,''
        , "wvr_type_cd in ('02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '32', '33')"
        ,'_tmsis_wvr_prtcptn_data'
        ,''
        , 3],


    ['101', 'nonclaimspct2tbl', 'el6.30', 'el630t',
        "case when b.chip_cd <> '3' or b.chip_cd is NULL then 1 else 0 end",
        '_tmsis_var_dmgrphc_elgblty',
        "a.rstrctd_bnfts_cd = 'C'",
        '_tmsis_elgblty_dtrmnt',
        '',
        2],

    ['101', 'nonclaimspct2tbl', 'el6.31', 'el631t',
        "case when b.rstrctd_bnfts_cd <> 'D' or b.rstrctd_bnfts_cd is NULL then 1 else 0 end",
        '_tmsis_elgblty_dtrmnt',
        "a.mfp_enrlmt_efctv_dt is not null",
        '_tmsis_mfp_info',
        '',
        2],

    ['101', 'nonclaimspct2tbl', 'el6.35', 'el635t',
        "case when b.mfp_enrlmt_efctv_dt is null then 1 else 0 end",
        '_tmsis_mfp_info',
        "a.rstrctd_bnfts_cd = 'D'",
        '_tmsis_elgblty_dtrmnt',
        '',
        2],

    ['101', 'nonclaimspct2tbl', 'el6.33', 'el633t',
        "case when b.imgrtn_stus_cd not in ('1', '2', '3') or b.imgrtn_stus_cd is null then 1 else 0 end",
        '_tmsis_var_dmgrphc_elgblty',
        "a.rstrctd_bnfts_cd = '2'",
        '_tmsis_elgblty_dtrmnt',
        '',
        2],

    ['101', 'nonclaimspct2tbl', 'el6.34', 'el634t',
        "case when b.ctznshp_ind = '1' then 1 else 0 end",
        '_tmsis_var_dmgrphc_elgblty',
        "a.rstrctd_bnfts_cd = '2'",
        '_tmsis_elgblty_dtrmnt',
        '',
        2],

    ['101', 'nonclaimspct2tbl', 'el6.36', 'el636t',
        "case when b.mdcr_bene_id is null then 1 else 0 end",
        '_tmsis_var_dmgrphc_elgblty',
        "a.dual_elgbl_cd in ('01','02','03','04','05','06','08','09','10')",
        '_tmsis_elgblty_dtrmnt',
        '',
        2],

    ['101', 'nonclaimspct', 'el1.23', 'el123t',
        "case when elgbl_state_cd <> submtg_state_cd or nonmatchcounty=1 or nonmatchzip=1 then 1 else 0 end",
        '',
        "elgbl_adr_type_cd = '01'",
        '',
        '_el12x',
        2],

    ['101', 'nonclaimspct', 'el1.30', 'el130t',
        "case when ((elgbl_state_cd is not null) and (submtg_state_cd <> elgbl_state_cd)) or ((elgbl_cnty_cd is not null) and (nonmatchcounty=1)) or ((elgbl_zip_cd is not null) and (nonmatchzip=1)) then 1 else 0 end",
        '',
        "elgbl_adr_type_cd <> '01' or elgbl_adr_type_cd is null",
        '',
        '_el12x',
        2],

    ['101', 'nonclaimspct', 'el1.25', 'el125t',
        "case when ctznshp_ind <> '1' or ctznshp_ind is NULL then 1 else 0 end",
        '',
        "imgrtn_stus_cd = '8'",
        '',
        '_tmsis_var_dmgrphc_elgblty',
        2],

    ['101', 'nonclaimspct', 'el1.26', 'el126t',
        "case when imgrtn_stus_cd <> '8' or imgrtn_stus_cd is NULL then 1 else 0 end",
        '',
        "ctznshp_ind = '1'",
        '',
        '_tmsis_var_dmgrphc_elgblty',
        2],

    ['101', 'nonclaimspct_notany', 'el1.27', 'el127t',
        "case when race_cd = '003' then 1 else 0 end",
        '',
        "crtfd_amrcn_indn_alskn_ntv_ind = '1'",
        '',
        '_tmsis_race_info',
        2],

    ['101', 'nonclaimspct', 'el1.31', 'el131t',
        "case when elgbl_state_cd_match=1 and ((elgbl_cnty_cd is not null and nonmatchcounty_elgbl=1) or (elgbl_zip_cd is not null and nonmatchzip_elgbl=1)) then 1 else 0 end",
        '',
        "msis_ident_num is not null",
        '',
        '_el12x',
        2],

    ['101', 'nonclaimspct2tbl', 'el3.31', 'el331t',
        "case when b.enrlmt_type_cd = '2' then 1 else 0 end",
        '_tmsis_enrlmt_time_sgmt_data',
        "a.chip_cd = '1'",
        '_tmsis_var_dmgrphc_elgblty',
        '',
        2],

    ['101', 'nonclaimspct2tbl', 'el3.32', 'el332t',
        "case when b.enrlmt_type_cd = '1' then 1 else 0 end",
        '_tmsis_enrlmt_time_sgmt_data',
        "a.chip_cd = '3'",
        '_tmsis_var_dmgrphc_elgblty',
        '',
        2],

    ['101', 'el122t', 'el1.22', 'el122t',
        '',
        '',
        '',
        '',
        '',
        2],

    # from version 3.4 onwards, new measures should be defined like this, for clarity on what the arguments are.
    create_run_101_input(
        series='101',
        cb='nonclaimspct',
        measure='el1.33',
        id='el133t',
        numerator='case when race_cd="001" then 1 else 0 end',
        denominator='msis_ident_num is not null',
        table='_tmsis_race_info'
    ),

    create_run_101_input(
        series='101',
        cb='nonclaimspct',
        measure='el1.34',
        id='el134t',
        numerator='case when race_cd="002" then 1 else 0 end',
        denominator='msis_ident_num is not null',
        table='_tmsis_race_info'
    ),

    create_run_101_input(
        series='101',
        cb='nonclaimspct',
        measure='el1.35',
        id='el135t',
        numerator='case when race_cd="003" then 1 else 0 end',
        denominator='msis_ident_num is not null',
        table='_tmsis_race_info'
    ),

    create_run_101_input(
        series='101',
        cb='nonclaimspct',
        measure='el1.36',
        id='el136t',
        numerator='case when race_cd in ("004", "005", "006", "007", "008", "009", "010", "011") then 1 else 0 end',
        denominator='msis_ident_num is not null',
        table='_tmsis_race_info'
    ),

    create_run_101_input(
        series='101',
        cb='nonclaimspct',
        measure='el1.37',
        id='el137t',
        numerator='case when race_cd in ("012", "013", "014", "015", "016") then 1 else 0 end',
        denominator='msis_ident_num is not null',
        table='_tmsis_race_info'
    ),

    create_run_101_input(
        series='101',
        cb='nonclaimspct',
        measure='el1.38',
        id='el138t',
        numerator='case when race_cd="018" then 1 else 0 end',
        denominator='msis_ident_num is not null',
        table='_tmsis_race_info'
    ),

]

df = DataFrame(run_101, columns=['series', 'cb', 'measure', 'id', 'numer', 'numertbl', 'denom', 'denomtbl', 'tbl', 'round'])

df['measure_id'] = df['measure'].str.replace('.', '_', regex=False).str.upper()

df = df[['series', 'cb', 'measure_id', 'id', 'numer', 'denom', 'numertbl', 'denomtbl', 'tbl', 'round']]
df = df.sort_values(by=['series', 'cb', 'id'])

# df = df.drop_duplicates()

# df.drop(['B', 'C'], axis=1)

# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)
# pd.set_option('display.width', 1000)
# pd.set_option('display.expand_frame_repr', False)
with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 2000):
    print (df)
    # print(df.head(64))

df.to_pickle('./run_101.pkl')

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
