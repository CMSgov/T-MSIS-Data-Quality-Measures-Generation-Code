from pandas import DataFrame

run_902_countt = [
# *ffs10.85, ffs11.24, ffs9.103, mcr10.24, mcr14.24;
    ['902', 'countt', 'ffs9_103', 'A', '1=1', 'CLL', 'OT'],
    ['902', 'countt', 'ffs10_85', 'B', '1=1', 'CLL', 'OT'],
    ['902', 'countt', 'ffs11_24', 'F', '1=1', 'CLL', 'OT'],
    ['902', 'countt', 'mcr10_24', 'P', '1=1', 'CLL', 'OT'],
    ['902', 'countt', 'mcr14_24', 'R', '1=1', 'CLL', 'OT'],

    # * ffs12.5, mcr11.6, mcr15.5;
    ['902', 'countt', 'ffs12_5', 'G', '1=1', 'CLL', 'OT'],
    ['902', 'countt', 'mcr11_6', 'T', '1=1', 'CLL', 'OT'],
    ['902', 'countt', 'mcr15_5', 'V', '1=1', 'CLL', 'OT'],
    # *sumffs.20, sumffs.9, summcr.9;
    ['902', 'countt', 'sumffs_9', 'C', '1=1', 'CLL', 'OT'],
    ['902', 'countt', 'sumffs_20', 'I', '1=1', 'CLL', 'OT'],
    ['902', 'countt', 'summcr_9', 'O', '1=1', 'CLL', 'OT'],
    ['902', 'countt', 'summcr_20', 'U', '1=1', 'CLL', 'OT'],

    # *ffs1.30, ffs2.13, ffs3.18, mcr1.18, mcr3.18;
    ['902', 'countt', 'ffs1_30', 'A', '1=1', 'CLH', 'IP'],
    ['902', 'countt', 'ffs2_13', 'B', '1=1', 'CLH', 'IP'],
    ['902', 'countt', 'ffs3_18', 'F', '1=1', 'CLH', 'IP'],
    ['902', 'countt', 'mcr1_18', 'P', '1=1', 'CLH', 'IP'],
    ['902', 'countt', 'mcr3_18', 'R', '1=1', 'CLH', 'IP'],

    # * ffs5.30, ffs6.10, ffs7.20, mcr5.21, mcr7.20;
    ['902', 'countt', 'ffs5_30', 'A', '1=1', 'CLH', 'LT'],
    ['902', 'countt', 'ffs6_10', 'B', '1=1', 'CLH', 'LT'],
    ['902', 'countt', 'ffs7_20', 'F', '1=1', 'CLH', 'LT'],
    ['902', 'countt', 'mcr5_21', 'P', '1=1', 'CLH', 'LT'],
    ['902', 'countt', 'mcr7_20', 'R', '1=1', 'CLH', 'LT'],

    # * ffs14.15, ffs16.8, mcr17.8. mcr19.8;
    ['902', 'countt', 'ffs14_15', 'A', '1=1', 'CLH', 'RX'],
    ['902', 'countt', 'ffs16_8', 'F', '1=1', 'CLH', 'RX'],
    ['902', 'countt', 'mcr17_8', 'P', '1=1', 'CLH', 'RX'],
    ['902', 'countt', 'mcr19_8', 'R', '1=1', 'CLH', 'RX'],

    # * ffs4.13, mcr2.25, mcr4.13;
    ['902', 'countt', 'ffs4_13', 'G', '1=1', 'CLH', 'IP'],
    ['902', 'countt', 'mcr2_25', 'T', '1=1', 'CLH', 'IP'],

    # * ffs8.10, mcr6.26, mcr8.9;
    ['902', 'countt', 'mcr4_13', 'V', '1=1', 'CLH', 'IP'],
    ['902', 'countt', 'ffs8_10', 'G', '1=1', 'CLH', 'LT'],
    ['902', 'countt', 'mcr6_26', 'T', '1=1', 'CLH', 'LT'],
    ['902', 'countt', 'mcr8_9',  'V', '1=1', 'CLH', 'LT'],

    # * sumffs.14, sumffs.3, summcr.14, summcr.3;
    ['902', 'countt', 'sumffs_3', 'C', '1=1', 'CLH', 'IP'],
    ['902', 'countt', 'sumffs_14', 'I', '1=1', 'CLH', 'IP'],
    ['902', 'countt', 'summcr_3', 'O', '1=1', 'CLH', 'IP'],
    ['902', 'countt', 'summcr_14', 'U', '1=1', 'CLH', 'IP'],
    # * sumffs.17, sumffs.6, summcr.17, summcr.6;
    ['902', 'countt', 'sumffs_6', 'C', '1=1', 'CLH', 'LT'],
    ['902', 'countt', 'sumffs_17', 'I', '1=1', 'CLH', 'LT'],
    ['902', 'countt', 'summcr_6', 'O', '1=1', 'CLH', 'LT'],
    ['902', 'countt', 'summcr_17', 'U', '1=1', 'CLH', 'LT'],

    # * sumffs.22, sumffs.11, summcr.22, summcr.11;
    ['902', 'countt', 'sumffs_11', 'C', '1=1', 'CLH', 'RX'],
    ['902', 'countt', 'sumffs_22', 'I', '1=1', 'CLH', 'RX'],
    ['902', 'countt', 'summcr_11', 'O', '1=1', 'CLH', 'RX'],
    ['902', 'countt', 'summcr_22', 'U', '1=1', 'CLH', 'RX'],

    # /************************* mcr ONLY ****************************************************/

    #['902', 'countt', 'mcr9_1', 'D', "stc_cd='119'",                                                                                                   'CLL', 'OT'],
    #['902', 'countt', 'mcr9_2', 'D', "stc_cd='120'",                                                                                                   'CLL', 'OT'],
    #['902', 'countt', 'mcr9_3', 'D', "stc_cd='122'",                                                                                                   'CLL', 'OT'],
    #['902', 'countt', 'mcr9_5', 'D', "stc_cd='120' and SRVC_ENDG_DT <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and SRVC_ENDG_DT >= TMSIS_RPTG_PRD",   'CLL', 'OT'],
    #['902', 'countt', 'mcr9_6', 'D', "stc_cd='122' and SRVC_ENDG_DT <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and SRVC_ENDG_DT >= TMSIS_RPTG_PRD",   'CLL', 'OT'],
    #['902', 'countt', 'mcr9_7', 'D', "stc_cd='119' and SRVC_ENDG_DT <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and SRVC_ENDG_DT >= TMSIS_RPTG_PRD",   'CLL', 'OT'],
    #['902', 'countt', 'mcr9_8', 'D', "stc_cd='120' and SRVC_ENDG_DT > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ",                                     'CLL', 'OT'],
    #['902', 'countt', 'mcr9_9', 'D', "stc_cd='122' and SRVC_ENDG_DT > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ",                                     'CLL', 'OT'],
    #['902', 'countt', 'mcr9_10', 'D', "stc_cd='119' and SRVC_ENDG_DT > date_sub(add_months(TMSIS_RPTG_PRD,1),1)",                                    'CLL', 'OT'],
    #['902', 'countt', 'mcr9_11', 'D', "stc_cd='120' and SRVC_ENDG_DT < date_sub(TMSIS_RPTG_PRD,30) ",                                                 'CLL', 'OT'],
    #['902', 'countt', 'mcr9_12', 'D', "stc_cd='122' and SRVC_ENDG_DT < date_sub(TMSIS_RPTG_PRD,30) ",                                                 'CLL', 'OT'],
    #['902', 'countt', 'mcr9_13', 'D', "stc_cd='119' and SRVC_ENDG_DT < date_sub(TMSIS_RPTG_PRD,30) ",                                                 'CLL', 'OT'],
    #['902', 'countt', 'mcr9_14', 'D', "stc_cd='120' and SRVC_ENDG_DT < TMSIS_RPTG_PRD and SRVC_ENDG_DT >= date_sub(TMSIS_RPTG_PRD,30) ",              'CLL', 'OT'],
    #['902', 'countt', 'mcr9_15', 'D', "stc_cd='122' and SRVC_ENDG_DT < TMSIS_RPTG_PRD and SRVC_ENDG_DT >= date_sub(TMSIS_RPTG_PRD,30) ",              'CLL', 'OT'],
    #['902', 'countt', 'mcr9_16', 'D', "stc_cd='119' and SRVC_ENDG_DT < TMSIS_RPTG_PRD and SRVC_ENDG_DT >= date_sub(TMSIS_RPTG_PRD,30) ",              'CLL', 'OT'],

    #['902', 'countt', 'mcr9_17', 'D', '1=1',                                                                                                           'CLL', 'OT'],
    # ['902', 'countt', 'mcr13_1', 'J', "stc_cd='119'",'CLL', 'OT'],
    #['902', 'countt', 'mcr13_2', 'J', "stc_cd='120'",                                                                                                  'CLL', 'OT'],
    #['902', 'countt', 'mcr13_3', 'J', "stc_cd='122'",                                                                                                  'CLL', 'OT'],
    #['902', 'countt', 'mcr13_5', 'J', "stc_cd='120' and SRVC_ENDG_DT <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and SRVC_ENDG_DT >= TMSIS_RPTG_PRD ", 'CLL', 'OT'],
    #['902', 'countt', 'mcr13_6', 'J', "stc_cd='122' and SRVC_ENDG_DT <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and SRVC_ENDG_DT >= TMSIS_RPTG_PRD ", 'CLL', 'OT'],
    #['902', 'countt', 'mcr13_7', 'J', "stc_cd='119' and SRVC_ENDG_DT <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and SRVC_ENDG_DT >= TMSIS_RPTG_PRD ", 'CLL', 'OT'],
    #['902', 'countt', 'mcr13_8', 'J',  "stc_cd='120' and SRVC_ENDG_DT > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ",                                    'CLL', 'OT'],
    #['902', 'countt', 'mcr13_9', 'J',  "stc_cd='122' and SRVC_ENDG_DT > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ",                                    'CLL', 'OT'],
    #['902', 'countt', 'mcr13_10', 'J', "stc_cd='119' and SRVC_ENDG_DT > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ",                                    'CLL', 'OT'],
    #['902', 'countt', 'mcr13_11', 'J', "stc_cd='120' and SRVC_ENDG_DT < date_sub(TMSIS_RPTG_PRD,30) ",                                                 'CLL', 'OT'],
    #['902', 'countt', 'mcr13_12', 'J', "stc_cd='122' and SRVC_ENDG_DT < date_sub(TMSIS_RPTG_PRD,30) ",                                                 'CLL', 'OT'],
    #['902', 'countt', 'mcr13_13', 'J', "stc_cd='119' and SRVC_ENDG_DT < date_sub(TMSIS_RPTG_PRD,30) ",                                                 'CLL', 'OT'],
    #['902', 'countt', 'mcr13_14', 'J', "stc_cd='120' and SRVC_ENDG_DT < TMSIS_RPTG_PRD and SRVC_ENDG_DT >= date_sub(TMSIS_RPTG_PRD,30) ",              'CLL', 'OT'],
    #['902', 'countt', 'mcr13_15', 'J', "stc_cd='122' and SRVC_ENDG_DT < TMSIS_RPTG_PRD and SRVC_ENDG_DT >= date_sub(TMSIS_RPTG_PRD,30) ",              'CLL', 'OT'],
    #['902', 'countt', 'mcr13_16', 'J', "stc_cd='119' and SRVC_ENDG_DT < TMSIS_RPTG_PRD and SRVC_ENDG_DT >= date_sub(TMSIS_RPTG_PRD,30) ",              'CLL', 'OT'],
    #['902', 'countt', 'mcr13_17', 'J', '1=1',  'CLL', 'OT'],
    #['902', 'countt', 'summcr_23', 'E', '1=1', 'CLL', 'OT'],
    #['902', 'countt', 'summcr_24', 'K', '1=1', 'CLL', 'OT'],

    # * mcr12_156 through mcr12_163;
    #['902', 'countt', 'mcr12_156', 'BF', "clm_type_cd='6' ", 'CLH', 'OT'],
    #['902', 'countt', 'mcr12_157', 'BG', "clm_type_cd='F' ", 'CLH', 'OT'],
    ['902', 'countt', 'mcr12_158', 'Q', "src_lctn_cd='22' ", 'CLH', 'OT'],
    ['902', 'countt', 'mcr12_159', 'S', "src_lctn_cd='22' ", 'CLH', 'OT'],
    ['902', 'countt', 'mcr12_160', 'Q', "src_lctn_cd='23' ", 'CLH', 'OT'],
    ['902', 'countt', 'mcr12_161', 'S', "src_lctn_cd='23' ", 'CLH', 'OT'],
    #['902', 'countt', 'mcr12_162', 'BF', "src_lctn_cd='20' ", 'CLH', 'OT'],
    #['902', 'countt', 'mcr12_163', 'BG', "src_lctn_cd='20' ", 'CLH', 'OT'],

    # FTX measures added
    ['902', 'ftx_countt', 'mcr13_1', 'J', "(pyee_mcr_plan_type in  ('01', '04', '17') )",'','tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr13_7', 'J', "(pyee_mcr_plan_type in  ('01', '04', '17')) and cptatn_prd_end_dt  <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and cptatn_prd_end_dt  >= TMSIS_RPTG_PRD",'','tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr13_16', 'J', "(pyee_mcr_plan_type in  ('01', '04', '17')) and cptatn_prd_end_dt < TMSIS_RPTG_PRD and cptatn_prd_end_dt >= date_sub(TMSIS_RPTG_PRD,30) ",'','tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr13_13', 'J', "(pyee_mcr_plan_type in  ('01', '04', '17')) and cptatn_prd_end_dt < date_sub(TMSIS_RPTG_PRD,30) ", '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr13_10', 'J', "(pyee_mcr_plan_type in  ('01', '04', '17')) and cptatn_prd_end_dt > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ", '', 'tmsis_indvdl_cptatn_pmpm'],
  
   
    ['902', 'ftx_countt', 'mcr9_1', 'D', "(pyee_mcr_plan_type in  ('01', '04', '17') )",'','tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr9_7', 'D', "(pyee_mcr_plan_type in  ('01', '04', '17') ) and cptatn_prd_end_dt <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and cptatn_prd_end_dt >= TMSIS_RPTG_PRD", '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr9_16', 'D', "(pyee_mcr_plan_type in  ('01', '04', '17') ) and cptatn_prd_end_dt < TMSIS_RPTG_PRD and cptatn_prd_end_dt >= date_sub(TMSIS_RPTG_PRD,30) ", '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr9_13', 'D', "(pyee_mcr_plan_type in  ('01', '04', '17') ) and cptatn_prd_end_dt < date_sub(TMSIS_RPTG_PRD,30) ", '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr9_10', 'D', "(pyee_mcr_plan_type in  ('01', '04', '17') ) and cptatn_prd_end_dt > date_sub(add_months(TMSIS_RPTG_PRD,1),1)",'', 'tmsis_indvdl_cptatn_pmpm'],
  
    ['902', 'ftx_countt', 'mcr13_2', 'J', "pyee_mcr_plan_type in  ('02', '03') ",'','tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr13_5', 'J', "pyee_mcr_plan_type in  ('02', '03') and cptatn_prd_end_dt <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and cptatn_prd_end_dt >= TMSIS_RPTG_PRD", '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr13_14', 'J', "pyee_mcr_plan_type in  ('02', '03') and cptatn_prd_end_dt < TMSIS_RPTG_PRD and cptatn_prd_end_dt >= date_sub(TMSIS_RPTG_PRD,30) ", '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr13_11', 'J', "pyee_mcr_plan_type in  ('02', '03') and cptatn_prd_end_dt < date_sub(TMSIS_RPTG_PRD,30)", '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr13_8', 'J',  "pyee_mcr_plan_type in  ('02', '03') and cptatn_prd_end_dt > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ", '', 'tmsis_indvdl_cptatn_pmpm'],
   
  
    ['902', 'ftx_countt', 'mcr9_2', 'D', "pyee_mcr_plan_type in  ('02', '03') ",   '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr9_5', 'D', "pyee_mcr_plan_type in  ('02', '03') and cptatn_prd_end_dt <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and cptatn_prd_end_dt >= TMSIS_RPTG_PRD", '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr9_14', 'D', "pyee_mcr_plan_type in  ('02', '03') and cptatn_prd_end_dt < TMSIS_RPTG_PRD and cptatn_prd_end_dt >= date_sub(TMSIS_RPTG_PRD,30) ",  '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr9_11', 'D', "pyee_mcr_plan_type in  ('02', '03') and cptatn_prd_end_dt < date_sub(TMSIS_RPTG_PRD,30) ",  '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr9_8', 'D', "pyee_mcr_plan_type in  ('02', '03') and cptatn_prd_end_dt > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ", '', 'tmsis_indvdl_cptatn_pmpm'],
    

    ['902', 'ftx_countt', 'mcr13_3', 'J', "pyee_mcr_plan_type in  ('05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19')", '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr13_6', 'J', "pyee_mcr_plan_type in  ('05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19') and cptatn_prd_end_dt <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and cptatn_prd_end_dt >= TMSIS_RPTG_PRD ", '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr13_15', 'J', "pyee_mcr_plan_type in  ('05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19') and cptatn_prd_end_dt < TMSIS_RPTG_PRD and cptatn_prd_end_dt >= date_sub(TMSIS_RPTG_PRD,30) ",  '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr13_12', 'J', "pyee_mcr_plan_type in  ('05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19') and cptatn_prd_end_dt < date_sub(TMSIS_RPTG_PRD,30) ",  '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr13_9', 'J',  "pyee_mcr_plan_type in  ('05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19') and cptatn_prd_end_dt > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ",  '', 'tmsis_indvdl_cptatn_pmpm'],
   
    ['902', 'ftx_countt', 'mcr9_3', 'D', "pyee_mcr_plan_type in  ('05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19')",   '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr9_6', 'D', "pyee_mcr_plan_type in  ('05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19') and cptatn_prd_end_dt <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and cptatn_prd_end_dt >= TMSIS_RPTG_PRD",  '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr9_15', 'D', "pyee_mcr_plan_type in  ('05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19') and cptatn_prd_end_dt < TMSIS_RPTG_PRD and cptatn_prd_end_dt >= date_sub(TMSIS_RPTG_PRD,30) ",  '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr9_12', 'D', "pyee_mcr_plan_type in  ('05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19') and cptatn_prd_end_dt < date_sub(TMSIS_RPTG_PRD,30) ", '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr9_9', 'D', "pyee_mcr_plan_type in  ('05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19') and cptatn_prd_end_dt > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ", '', 'tmsis_indvdl_cptatn_pmpm'],
  
    ['902', 'ftx_multi_tbl_countt', 'mcr9_17', 'D', '', '', ''],
    ['902', 'ftx_multi_tbl_countt', 'mcr13_17', 'J', '',  '', ''],

    ['902', 'ftx_multi_tbl_countt', 'summcr_23', 'E', '', '', ''],
    ['902', 'ftx_multi_tbl_countt', 'summcr_24', 'K', '', '', ''],


    #['902', 'ftx_countt', 'mcr12_156', 'BF', "subcptatn_ind ='2' ", '', 'tmsis_indvdl_cptatn_pmpm'],
    #['902', 'ftx_countt', 'mcr12_157', 'BG', "subcptatn_ind ='2' ", '', 'tmsis_indvdl_cptatn_pmpm'],
   
    #['902', 'ftx_countt', 'mcr12_162', 'BF', "src_lctn_cd='20' ", '', 'tmsis_indvdl_cptatn_pmpm'],
    #['902', 'ftx_countt', 'mcr12_163', 'BG', "src_lctn_cd='20' ", '', 'tmsis_indvdl_cptatn_pmpm'],
 
    ['902', 'ftx_countt', 'mcr12_156','', "subcptatn_ind ='2' ", '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr12_157','' , "subcptatn_ind ='2' ", '', 'tmsis_indvdl_cptatn_pmpm'],
   
    ['902', 'ftx_countt', 'mcr12_162','' , "src_lctn_cd='20' ", '', 'tmsis_indvdl_cptatn_pmpm'],
    ['902', 'ftx_countt', 'mcr12_163', '', "src_lctn_cd='20' ", '', 'tmsis_indvdl_cptatn_pmpm'],

]

df = DataFrame(run_902_countt, columns=['series', 'cb', 'measure_id', 'claim_cat', 'constraint', 'level', 'claim_type'])
df['measure_id'] = df['measure_id'].str.upper()
# df = df.set_index("measure_id", drop = False)
print(df.head())
df.to_pickle('./run_902.pkl')

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
