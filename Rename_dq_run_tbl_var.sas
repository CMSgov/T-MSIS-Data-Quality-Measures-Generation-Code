


%let progpath = /sasdata/users/&sysuserid./tmsisshare/prod/analytics_tk2_DEV;
%let specvrsn = %str(V1.4); /*change this for each version */

libname out "&progpath.";

proc freq data=out.dq_run_tbl;
tables separate_pa;
run;

data out.dq_run_tbl;
set out.dq_run_tbl;
rename separate_pa=separate_entity;
run;

proc freq;
tables separate_entity;
run;
