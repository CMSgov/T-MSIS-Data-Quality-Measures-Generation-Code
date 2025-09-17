# --------------------------------------------------------------------
#
#
#
# --------------------------------------------------------------------
from dqm.DQM_Metadata import DQM_Metadata
from dqm.DQMeasures import DQMeasures

class Runner_711:

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def run(spark, dqm: DQMeasures, measures: list = None) :
        dqm.logger.info('Module().run<series>.run() has been deprecated. Use dqm.run(spark) or dqm.run(spark, dqm.where(series="<series>")) instead.')
        if measures is None:
            return dqm.run(spark, dqm.reverse_measure_lookup[dqm.reverse_measure_lookup['series'] == '711']['measure_id'].tolist())
        else:
            return dqm.run(spark, measures)

    taxonomy_fvar = """CASE
                        WHEN SUBSTRING(blg_prvdr_txnmy_cd, 1, 4) IN
                            ('1932', '1934', '261Q', '282N', '283Q', '313M', '3140', '3902', '4053')
                            THEN SUBSTRING(blg_prvdr_txnmy_cd, 1, 4)
                        WHEN SUBSTRING(blg_prvdr_txnmy_cd, 1, 2) IN
                            ('10', '11', '12', '13', '14', '15', '16', '17', '18',
                             '20', '21', '22', '23', '24', '25', '26', '27', '28', '29',
                             '30', '31', '32', '33', '34', '36', '37', '38')
                            THEN SUBSTRING(blg_prvdr_txnmy_cd, 1, 2)
                        ELSE NULL END"""

    bill_fvar = """CASE WHEN substring(bill_type_cd, 1, 3) in
                    ('011', '012', '013', '014', '015', '016', '018',
                     '021', '022', '023', '024', '025', '026', '028',
                     '031', '032', '033', '034', '035', '036', '038',
                     '041', '042', '043', '044', '045', '046', '048',
                     '061', '062', '063', '064', '065', '066', '068',
                     '071', '072', '073', '074', '075', '076', '077', '078', '079',
                     '081', '082', '083', '084', '085', '086', '087', '089')
                    then substring(bill_type_cd, 1, 3) else null end"""

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def ALL32_1(spark, dqm: DQMeasures, measure_id, claim_type, x) :

        constraint = DQM_Metadata.create_base_clh_view.claim_cat['AJ']
        fvar = Runner_711.bill_fvar

        z = f"""
            select
                '{dqm.state}' as submtg_state_cd,
                '{measure_id}' as measure_id,
                '711' as submodule,
                m.numer,
                m.denom,
                coalesce(m.mvalue, v.mvalue) as mvalue,
                v.valid_value,
                '{claim_type}' as claim_type
            from
                {dqm.turboDB}.freq_msr_billtype as v
            left join (

                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'711' as submodule
                    , null as numer
                    , null as denom
                    , mvalue
                    , valid_value
                    , '{claim_type}' as claim_type

                from (

                    select distinct submtg_state_cd, {fvar} as valid_value, count({x['var']}) as mvalue
                    from {DQMeasures.getBaseTable(dqm, 'clh', claim_type)}
                    where ({constraint} and {fvar} in ({x['list']}))
                    group by submtg_state_cd, valid_value) a

        union all

                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'711' as submodule
                    , null as numer
                    , null as denom
                    , mvalue
                    , valid_value
                    , '{claim_type}' as claim_type

                from (

                    select distinct submtg_state_cd, 'A_' as valid_value, count({x['var']}) as mvalue
                    from {DQMeasures.getBaseTable(dqm, 'clh', claim_type)}
                    where ({constraint} and {fvar} in ({x['list']}) )
                    group by submtg_state_cd) a

        union all

                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'711' as submodule
                    , null as numer
                    , null as denom
                    , mvalue
                    , valid_value
                    , '{claim_type}' as claim_type

                from (

                    select distinct submtg_state_cd, 'N_' as valid_value, count(submtg_state_cd) as mvalue
                    from {DQMeasures.getBaseTable(dqm, 'clh', claim_type)}
                    where ({constraint} and ({fvar} is null ) and {x['var']} is not null )
                    group by submtg_state_cd) a

        union all

                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'711' as submodule
                    , null as numer
                    , null as denom
                    , mvalue
                    , valid_value
                    , '{claim_type}' as claim_type

                from (

                    select distinct submtg_state_cd, 'M_' as valid_value, count(submtg_state_cd) as mvalue
                    from {DQMeasures.getBaseTable(dqm, 'clh', claim_type)}
                    where ({constraint} and ({x['var']} is null ))
                    group by submtg_state_cd) a

        union all

                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'711' as submodule
                    , null as numer
                    , null as denom
                    , mvalue
                    , valid_value
                    , '{claim_type}' as claim_type

                from (

                    select distinct submtg_state_cd, 'T_' as valid_value, sum (1) as mvalue
                    from {DQMeasures.getBaseTable(dqm, 'clh', claim_type)}
                    where ({constraint})
                    group by submtg_state_cd) a

                ) as m on m.valid_value = v.valid_value
            """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def ALL33_1(spark, dqm: DQMeasures, measure_id, claim_type, x) :

        constraint = DQM_Metadata.create_base_clh_view.claim_cat['AJ']
        dist_var = Runner_711.taxonomy_fvar

        z = f"""
            select
                '{dqm.state}' as submtg_state_cd,
                '{measure_id}' as measure_id,
                '711' as submodule,
                m.numer,
                m.denom,
                coalesce(m.mvalue, v.mvalue) as mvalue,
                v.valid_value,
                '{claim_type}' as claim_type
            from
                {dqm.turboDB}.freq_msr_tax as v
            left join (

                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'711' as submodule
                    , null as numer
                    , null as denom
                    , mvalue
                    , case when valid_value IN ('1932', '1934', '261Q', '282N', '283Q', '313M', '3140', '3902', '4053') then valid_value else concat(valid_value, 'XX') end as valid_value
                    , '{claim_type}' as claim_type

                from (

                    select distinct submtg_state_cd, {dist_var} as valid_value, count({x['var']}) as mvalue
                    from {DQMeasures.getBaseTable(dqm, 'clh', claim_type)}
                    where ({constraint} and {dist_var} in ({x['dist_list']}) and {x['var']} in ({str(dqm.prvtxnmy['Taxonomy'].tolist())[1:-1]}))
                    group by submtg_state_cd, valid_value) a

        union all

                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'711' as submodule
                    , null as numer
                    , null as denom
                    , mvalue
                    , valid_value
                    , '{claim_type}' as claim_type

                from (

                    select distinct submtg_state_cd, 'A_' as valid_value, count({x['var']}) as mvalue
                    from {DQMeasures.getBaseTable(dqm, 'clh', claim_type)}
                    where ({constraint} and {dist_var} in ({x['dist_list']}) and {x['var']} in ({str(dqm.prvtxnmy['Taxonomy'].tolist())[1:-1]}) )
                    group by submtg_state_cd) a

        union all

                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'711' as submodule
                    , null as numer
                    , null as denom
                    , mvalue
                    , valid_value
                    , '{claim_type}' as claim_type

                    from (

                        select distinct submtg_state_cd, 'N_' as valid_value, count(submtg_state_cd) as mvalue
                        from {DQMeasures.getBaseTable(dqm, 'clh', claim_type)}
                        where {constraint} and {x['var']} is not null and ({dist_var} is null or {x['var']} not in ({str(dqm.prvtxnmy['Taxonomy'].tolist())[1:-1]}) )
                        group by submtg_state_cd) a

        union all

                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'711' as submodule
                    , null as numer
                    , null as denom
                    , mvalue
                    , valid_value
                    , '{claim_type}' as claim_type

                    from (

                        select distinct submtg_state_cd, 'M_' as valid_value, count(submtg_state_cd) as mvalue
                        from {DQMeasures.getBaseTable(dqm, 'clh', claim_type)}
                        where ({constraint} and ({x['var']} is null ))
                        group by submtg_state_cd) a

        union all

                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'711' as submodule
                    , null as numer
                    , null as denom
                    , mvalue
                    , valid_value
                    , '{claim_type}' as claim_type

                    from (

                        select distinct submtg_state_cd, 'T_' as valid_value, sum (1) as mvalue
                        from {DQMeasures.getBaseTable(dqm, 'clh', claim_type)}
                        where ({constraint})
                        group by submtg_state_cd) a

                ) as m on m.valid_value = v.valid_value
            """

        dqm.logger.debug(z)

        return spark.sql(z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def frq_list(spark, dqm: DQMeasures, measure_id, x) :

        spark_df_ip = Runner_711.ALL32_1(spark, dqm, measure_id, 'IP', x)
        spark_df_lt = Runner_711.ALL32_1(spark, dqm, measure_id, 'LT', x)
        spark_df_ot = Runner_711.ALL32_1(spark, dqm, measure_id, 'OT', x)

        return spark_df_ip.unionByName(spark_df_lt).unionByName(spark_df_ot)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def frq_list_tax(spark, dqm: DQMeasures, measure_id, x) :

        spark_df_ip = Runner_711.ALL33_1(spark, dqm, measure_id, 'IP', x)
        spark_df_lt = Runner_711.ALL33_1(spark, dqm, measure_id, 'LT', x)
        spark_df_ot = Runner_711.ALL33_1(spark, dqm, measure_id, 'OT', x)

        return spark_df_ip.unionByName(spark_df_lt).unionByName(spark_df_ot)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    v_table = {
            "frq_list": frq_list,
            "frq_list_tax": frq_list_tax
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