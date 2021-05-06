%let AREMACpath = /sasdata/users/&sysuserid./tmsisshare/prod/01_AREMAC;
%let progpath = &AREMACpath./Task_12/DQ_SAS_DEV;

*statements for accessing T-MSIS data in AREMAC;
%include "&AREMACpath./global/databricks_connection.sas";

filename zcc "&progpath/002_lookups/Zip_County_Crosswalk.csv";
data zcc;
    infile zcc delimiter=',' missover dsd lrecl=32767 firstobs=2;
    length ZipCode $5 State $2 City $30 County $22 StateFIPS $2 CountyFIPS $3;
    input ZipCode $ Sequence State $ City $ County $ StateFIPS $ CountyFIPS $ Percent;
run;
data zipstate_lookup;
    set
        zcc(keep=statefips zipcode)
        zcc(keep=statefips zipcode where=(statefips='19') in=in96)
        zcc(keep=statefips zipcode where=(statefips='42') in=in97)
        ;
    if in96 then statefips = '96';
    if in97 then statefips = '97';
run;
data countystate;
    set
        zcc(keep=statefips countyfips)
        zcc(keep=statefips countyfips where=(statefips='19') in=in96)
        zcc(keep=statefips countyfips where=(statefips='42') in=in97)
        ;
    if in96 then statefips = '96';
    if in97 then statefips = '97';
run;
proc summary data=countystate nway missing;
    class countyfips statefips;
    output out=countystate_lookup(keep=countyfips statefips);
run;

%macro aremac_insert(dsname=, prefix=, dbperm=);
    *get variable names and formats;
    proc contents data=&dsname. out=cnt (keep=name length type varnum formatd) noprint;
    run;
    *assign Redshift data type based on SAS format;
    data cnt (keep=name redshift_type varnum type);
        set cnt;
        if type=2 then redshift_type = cats('varchar(',length,')');
        else if type = 1 and formatd = 0 then redshift_type = 'bigint';
        else if type = 1 and formatd > 0 then redshift_type = 'float8';
    run;
    proc sql noprint;
        *get lists of variables and their types;
        select name, redshift_type, type
            into :varlist separated by ' ', :rtypelist separated by ' ', :typelist separated by ' '
            from cnt
            order by varnum;
        *get a count of variables;
        select count(*)
            into :varcount
            from cnt;
    quit;
    run;
    *create a dataset with the commands for inserting each row;
    data insertcmds (keep=insert);
        set &dsname end=eof;
        length insert $200.;
        insert = '(';
        *treat last variable differently, so only go through count - 1;
        %do j = 1 %to %eval(&varcount. - 1);
                        *if numeric, the data can be uploaded without quotes;
            %if %scan(&typelist.,&j.) = 1 %then %do;
                if not missing(%scan(&varlist.,&j.)) then  insert = cats(insert,%scan(&varlist.,&j.),',');
                else insert = cats(insert,'null',',');
                %end;
            %else %do;
                if not missing(%scan(&varlist.,&j.))  then insert = cats(insert,'''',%scan(&varlist.,&j.),''',');
                else insert = cats(insert,'null',',');
                %end;
            %end;
        *this is for the last variable on the list;
        *if numeric, no quotes needed;
        %if %scan(&typelist.,&varcount.) = 1 %then %do;
            if not eof and not missing(%scan(&varlist.,&j.)) then insert = cats(insert,%scan(&varlist.,&varcount.),'),');
            else if not eof and missing(%scan(&varlist.,&j.))     then insert = cats(insert,'null','),');
            else if eof and not missing(%scan(&varlist.,&j.)) then insert = cats(insert,%scan(&varlist.,&varcount.),');');
            else if eof and missing(%scan(&varlist.,&j.)) then insert = cats(insert,'null',');');
            %end;
        %else %do;
            if not eof and not missing(%scan(&varlist.,&j.)) then insert = cats(insert,'''',%scan(&varlist.,&varcount.),'''),');
            else if not eof and missing(%scan(&varlist.,&j.))     then insert = cats(insert,'null','),');
            else if eof and not missing(%scan(&varlist.,&j.)) then insert = cats(insert,'''',%scan(&varlist.,&varcount.),''');');
            else if eof and missing(%scan(&varlist.,&j.)) then insert = cats(insert,'null',');');
            %end;
    run;
    data createcmd;
        length create $500.;
        create = '';
        %do j = 1 %to %eval(&varcount. - 1);
            create = cats(create,"%scan(&varlist.,&j.) %scan(&rtypelist.,&j.,,s)",', ');
            %end;
        create = cats(create,"%scan(&varlist.,&varcount.) %scan(&rtypelist.,&varcount.,,s)");
    run;
    *build a text file of the needed SQL commands;
    filename inscmds "&progpath./002_lookups/&dsname..txt";
    data _null_;
        set
            createcmd
            insertcmds end=eof;
        file inscmds;
        if _n_ = 1 then do;
            put "execute(drop table if exists &dbperm..&prefix.&dsname.) by tmsis_passthrough;";
            put 'execute(';
            put "create table if not exists &dbperm..&prefix.&dsname. (";
            put create;
            put ')) by tmsis_passthrough;';
            put 'execute(';
            put "insert into &dbperm..&prefix.&dsname values";
            end;
        else do;
            put insert;
            if eof then put ') by tmsis_passthrough;';
            end;
    run;
    %mend;

%*aremac_insert(dsname=zipstate_lookup, prefix=, dbperm=macbis_t12_temp);
%aremac_insert(dsname=countystate_lookup, prefix=, dbperm=macbis_t12_perm);


proc sql;
%tmsis_connect;
%include "&progpath./002_lookups/countystate_lookup.txt";

/*%include "&progpath./002_lookups/zipstate_lookup.txt";*/
%tmsis_disconnect;
quit;
run;
