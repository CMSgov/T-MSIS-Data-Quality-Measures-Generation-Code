T-MSIS DQ Measures – Technical Documentation
The programs in this repository are primarily organized by content area and measure type. The driver programs from each content area are called in a universal driver program. The universal driver program is the only program that needs to be executed to run all the measures.

Universal Driver Program
The universal driver program is MACBIS_DQ_Runner.sas. It contains calls to 10 other programs. The next three programs in this list are the SAS macro functions that are used throughout the rest of DQ Measures creation. The remaining seven programs are the driver programs from each content area.
•	MACBIS_DQ_Runner.sas
•	base_view_calls.sas
•	base_view_defs.sas
•	universal_macros.sas
•	001_elg_driver_sql.sas
•	200_exp_module_driver.sas
•	500_prov_driver_sql.sas
•	600_tpl_driver_sql.sas
•	700_utl_driver_sql.sas
•	800_miss_driver_sql.sas
•	900_ffs_mcr_module_driver.sas

Lookups
The following files contain the formats and S-CHIP designation for each state, measures metadata, as well as lookup values for particular measures. 
•	state_formats.sas
•	states.sas7bcat
•	SCHIP_Lookup.xlsx
•	thresholds.xlsx
•	State_DQ_Missingness_Measures.xlsx
•	AppendixC.xlsx
•	countystate_lookup.txt
•	Expansion Eligibility Groups_2018.txt
•	MIH Ever Pregnant Code Set.xlsx
•	ProviderTaxonomy.xlsx
•	TypeOfService.xlsx

Eligibility
The program 001_elg_driver_sql.sas is a driver program that calls each of the programs in the creation of DQ Measures for Eligibility files in sequence. 
The other programs in the list use the eligibility files created from the universal_macro.sas program to create measures for each specific purpose. 
•	001_elg_driver_sql.sas
•	101_el_pct_sql.sas
•	102_el_cnt_sql.sas
•	103_el_index_sql.sas
•	104_el_freq_sql.sas
•	105_el_oth1_sql.sas
•	105_el_oth2_sql.sas
•	105_el_oth3_sql.sas
•	105_el_oth4_sql.sas
•	105_el_oth5_sql.sas

Expenditures
The program 200_exp_module_driver.sas is a driver program that calls each of the programs in the creation of DQ Measures for Expenditures in sequence. 
The other programs in the list use the claims files created from the universal_macro.sas program to create measures for each specific purpose. 
•	200_exp_module_driver.sas
•	201_exp_claims_pct_macro.sas
•	202_exp_avg_macro.sas
•	204_exp_sum_macro.sas
•	205_exp_other_measures_macro.sas
•	206_exp_claims_count_macro.sas

Provider
The program 500_prv_driver_sql.sas is a driver program that calls each of the programs in the creation of DQ Measures for Provider in sequence. 
The other programs in the list use the provider files created from the universal_macro.sas program to create measures for each specific purpose. 
•	500_prov_driver_sql.sas
•	504_prvdr_pct_sql.sas
•	502_prvdr_cnt_sql.sas
•	503_prvdr_freq_sql.sas

Third Party Liability (TPL)
The program 600_tpl_driver_sql.sas is a driver program that calls each of the programs in the creation of DQ Measures for TPL in sequence. 
The other programs in the list use the claims and TPL files created from the universal_macro.sas program to create measures for each specific purpose.600_tpl_driver_sql.sas
•	601_tpl_clm_tab_othr_sql.sas
•	602_tpl_ot_tab_6_7_sql.sas
•	603_tpl_prsn_hi_tab_sql.sas
•	603_tpl_prsn_mn_ever_tab_sql
•	603_tpl_prsn_mn_tab_sql

Utilization (UTL)
The program 700_utl_driver_sql.sas is a driver program that calls each of the programs in the creation of DQ Measures for all utilization in sequence. 
The other programs in the list use the claim, provider, and eligibility files created from the universal_macro.sas program to create measures for each specific purpose.
•	700_utl_driver_sql.sas
•	701_utl_ot_prep_l_sql.sas
•	701_utl_stplan_prep_l_sql.sas
•	701_utl_wvr_prep_l_sql.sas
•	702_utl_el_tab_l_sql.sas
•	703_utl_ip_tab_n_sql.sas
•	703_utl_lt_tab_n_sql.sas
•	703_utl_ot_tab_n_sql.sas
•	704_utl_clms_prov_tab_w_sql.sas
•	704_utl_ip_tab_w_sql.sas
•	705_utl_ip_tab_ab_ac_sql.sas
•	705_utl_lt_tab_ab_ac_sql.sas
•	705_utl_ot_tab_ab_ac_sql.sas
•	705_utl_rx_tab_ab_ac_sql.sas
•	706_utl_all_clms_tab_aj_sql.sas
•	707_utl_all_clms_prov_tab_aj_sql.sas
•	707_utl_ot_tab_aj_sql.sas
•	708_utl_all_clms_tab_msng_sql.sas
•	709_utl_all_clms_tab_ah_sql.sas
•	710_utl_all_clms_freq_sql.sas
•	711_utl_all_clms_freq_sql.sas
•	712_utl_all_clms_freq_stc_cd
•	713_utl_all_pymnt_aj_sql.sas

Missingness
The program 800_miss_driver_sql.sas is a driver program that calls each of the programs in the creation of DQ Measures for missingness check in sequence. 
The rest of the programs in the list check the missingness of data elements on claims and non-claims records.
•	800_miss_driver_sql.sas
•	801_createMissingnessXlsx.sas
•	802_miss_claims_pct.sas
•	803_miss_non_claims_pct.sas

Fee-For-Service (FFS) / Managed Care (MCR)
The program 900_ffs_mcr_module_driver.sas is a driver program that calls each of the programs in the creation of DQ Measures for FFS/MCR claims in sequence. 
The other programs in the list use the claim, provider, and eligibility files created from the universal_macro.sas program to create measures for each specific purpose.
•	900_ffs_mcr_module_driver.sas
•	901_claims_pct_macro.sas
•	902_claims_count_macro.sas
•	903_claims_avg_per_unit_macro.sas
•	904_claims_avg_occur_macro.sas
•	905_claims_avg_count_macro.sas
•	906_ffs_clms_ad.sas
•	907_claims_ratio_macro.sas
•	909_claims_other_measures_macro.sas
•	910_claims_freq_macro.sas
•	911_claims_ever_elig_macro.sas
•	912_claims_other_measures_PCCM_macro.sas
•	913_claims_pct_amt_macro.sas
•	914_claims_pct_pymnt_macro.sas
•	915_claims_provider_taxonomy.sas
•	916_claims_luhn_check.sas
•	917_claims_pct_bill_type_cd_OT.sas
•	918_claims_sum_cll_mdcr_amt.sas
•	919_claims_schip_aq_ar.sas
