# --------------------------------------------------------------------
#
#
#
# --------------------------------------------------------------------
from dqm.DQM_Metadata import DQM_Metadata
from dqm.DQMeasures import DQMeasures


class Runner_714:

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def run(spark, dqm: DQMeasures, measures: list = None):
        dqm.logger.info('Module().run<series>.run() has been deprecated. Use dqm.run(spark) or dqm.run(spark, dqm.where(series="<series>")) instead.')
        if measures is None:
            return dqm.run(spark, dqm.reverse_measure_lookup[dqm.reverse_measure_lookup['series'] == '714']['measure_id'].tolist())
        else:
            return dqm.run(spark, measures)

    # --------------------------------------------------------------------
    #
    #  No. of service tracking claim lines with TYPE-OF-SERVICE =
    #  123 (DSH),
    #  131 (Drug Rebates),
    #  135 (EHR)
    #
    # --------------------------------------------------------------------
    def all36_1(spark, dqm: DQMeasures, measure_id, x):

        # Unique records within tmsis_cll_rec_ip
        # WHERE
        #   tmsis_cll_rec_ip.stc_cd = ('123', '131', or '135')

        # * Repeat for othr_toc and rx and sum the results

        z = f"""
            select
                '{dqm.state}' as submtg_state_cd,
                '{measure_id}' as measure_id,
                '714' as submodule,
                null as numer,
                null as denom,
                sum(m) as mvalue

            from (
                    select
                        count(*) as m
                    from
                        {DQMeasures.getBaseTable(dqm, 'cll', 'ip')}
                    where
                        stc_cd in ('123', '131', '135')
                        and {DQM_Metadata.create_base_clh_view.claim_cat['AS']}

                union all

                    select
                        count(*) as m
                    from
                        {DQMeasures.getBaseTable(dqm, 'cll', 'lt')}
                    where
                        stc_cd in ('123', '131', '135')
                        and {DQM_Metadata.create_base_clh_view.claim_cat['AS']}

                union all

                    select
                        count(*) as m
                    from
                        {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
                    where
                        stc_cd in ('123', '131', '135')
                        and {DQM_Metadata.create_base_clh_view.claim_cat['AS']}

                union all

                    select
                        count(*) as m
                    from
                        {DQMeasures.getBaseTable(dqm, 'cll', 'rx')}
                    where
                        stc_cd in ('123', '131', '135')
                        and {DQM_Metadata.create_base_clh_view.claim_cat['AS']}
                )
            """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #  % of claim lines with non-missing HCBS Service Code that
    #  have missing HCBS Taxonomy
    #
    # --------------------------------------------------------------------
    def all34_1(spark, dqm: DQMeasures, measure_id, x):

        # DENOM
        # Unique records within tmsis_cll_rec_othr_toc
        # WHERE
        #   tmsis_cll_rec_othr_toc.hcpcs_srvc_cd
        #   contains ((any alpha character A-Z OR a-z) OR (any digit 1-9))

        # NUMER
        # Denominator definition
        # AND
        #   tmsis_cll_rec_othr_toc.hcpcs_txnmy_cd does not contain ((any alpha character A-Z OR a-z) OR (any digit 1-9))
        # OR
        #   tmsis_cll_rec_othr_toc.hcpcs_txnmy_cd is NULL

        z = f"""
            select
                 '{dqm.state}' as submtg_state_cd
                ,'{measure_id}' as measure_id
                ,'714' as submodule
                ,coalesce(numer, 0) as numer
                ,coalesce(denom, 0) as denom
                ,CASE
                    WHEN coalesce(denom, 0) <> 0
                        THEN coalesce(numer, 0) / denom
                    ELSE NULL
                    END AS mvalue
            from (

                select
                    submtg_state_cd,
                    count(*) as denom
                from
                    {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
                where
                    hcpcs_srvc_cd rlike '[a-zA-Z1-9]'
                    and {DQM_Metadata.create_base_clh_view.claim_cat['AX']}
                group by
                    submtg_state_cd

            ) as d

            left join (

                select
                    submtg_state_cd,
                    count(*) as numer
                from
                    {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
                where
                    hcpcs_srvc_cd rlike '[a-zA-Z1-9]'
                    and (hcpcs_txnmy_cd not rlike '[a-zA-Z1-9]'
                      or hcpcs_txnmy_cd is null)
                    and {DQM_Metadata.create_base_clh_view.claim_cat['AX']}
                group by
                    submtg_state_cd

            ) as n

            on d.submtg_state_cd = n.submtg_state_cd
        """

        dqm.logger.debug(z)

        rs = spark.sql(z)

        if (rs.count() > 0):
            return rs
        else:
            z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'714' as submodule
                    ,0 as numer
                    ,0 as denom
                    ,typeof(NULL) as mvalue
            """
            dqm.logger.debug(z)
            return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #  % of claim lines with non-missing Procedure Code and either
    #  HCBS Service Code or HCBS Taxonomy that have a Procedure Code
    #  format that indicates a CPT or CDT code
    #
    # --------------------------------------------------------------------
    def all34_2(spark, dqm: DQMeasures, measure_id, x):

        # DENOM
        # Unique records within tmsis_cll_rec_othr_toc
        # WHERE
        #   tmsis_cll_rec_othr_toc.prcdr_cd
        #   contains ((any alpha character A-Z OR a-z) OR (any digit 1-9))
        # AND
        #   (tmsis_cll_rec_othr_toc.hcpcs_srvc_cd
        #   contains ((any alpha character A-Z OR a-z) OR (any digit 1-9))
        # OR
        #   tmsis_cll_rec_othr_toc.hcpcs_txnmy_cd
        #   contains ((any alpha character A-Z OR a-z) OR (any digit 1-9)))

        # NUMER
        # Denominator definition
        # AND
        #   Length of tmsis_cll_rec_othr_toc.prcdr_cd = 5
        # AND
        #   tmsis_cll_rec_othr_toc.prcdr_cd
        #   begins with "D" or any digit 0-9
        # AND
        #   tmsis_cll_rec_othr_toc.prcdr_cd
        #   only contains digits 0-9 in positions 2-5

        z = f"""
            select
                 '{dqm.state}' as submtg_state_cd
                ,'{measure_id}' as measure_id
                ,'714' as submodule
                ,coalesce(numer, 0) as numer
                ,coalesce(denom, 0) as denom
                ,CASE
                    WHEN coalesce(denom, 0) <> 0
                        THEN coalesce(numer, 0) / denom
                    ELSE NULL
                    END AS mvalue
            from (

                select
                    submtg_state_cd,
                    count(*) as denom
                from
                    {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
                where
                               prcdr_cd rlike '[a-zA-Z1-9]'
                    and ( hcpcs_srvc_cd rlike '[a-zA-Z1-9]'
                      or hcpcs_txnmy_cd rlike '[a-zA-Z1-9]')
                    and {DQM_Metadata.create_base_clh_view.claim_cat['AX']}
                group by
                    submtg_state_cd

            ) as d

            left join (

                select
                    submtg_state_cd,
                    count(*) as numer
                from
                    {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
                where
                    prcdr_cd rlike '[a-zA-Z1-9]'
                    and ( hcpcs_srvc_cd rlike '[a-zA-Z1-9]'
                      or hcpcs_txnmy_cd rlike '[a-zA-Z1-9]')
                    and length(trim(prcdr_cd)) = 5
                    and prcdr_cd rlike '^[D0-9]'
                    and prcdr_cd rlike '^.[0-9]'
                    and prcdr_cd rlike '^..[0-9]'
                    and prcdr_cd rlike '^...[0-9]'
                    and prcdr_cd rlike '^....[0-9]'
                    and {DQM_Metadata.create_base_clh_view.claim_cat['AX']}
                group by
                    submtg_state_cd

            ) as n

            on d.submtg_state_cd = n.submtg_state_cd
        """

        dqm.logger.debug(z)

        rs = spark.sql(z)

        if (rs.count() > 0):
            return rs
        else:
            z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'714' as submodule
                    ,0 as numer
                    ,0 as denom
                    ,typeof(NULL) as mvalue
            """
            dqm.logger.debug(z)
            return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #  % of claim lines with a Procedure Code indicating a
    #  sealant, filling, or root canal that are missing Tooth Number
    #
    # --------------------------------------------------------------------
    def all35_1_or_2(spark, dqm: DQMeasures, measure_id, x):

        # DENOM
        # Unique records within tmsis_cll_rec_othr_toc
        # WHERE
        #   tmsis_cll_rec_othr_toc.prcdr_cd
        #   =
        #   ("D1351" or "D2140" or "D2150" or "D2160" or
        #    "D2161" or "D2330" or "D2331" or "D2332" or
        #    "D2335" or "D2390" or "D2391" or "D2392" or
        #    "D2393" or "D2394" or "D3230" or "D3240" or
        #    "D3310" or "D3320" or "D3330")

        # NUMER
        # Denominator definition
        # AND
        #   tmsis_cll_rec_othr_toc.tooth_num
        #   does not contain ((any alpha character A-Z OR a-z) OR (any digit 1-9))
        #       OR
        #   tmsis_cll_rec_othr_toc.tooth_num is NULL

        z = f"""
            select
                 '{dqm.state}' as submtg_state_cd
                ,'{measure_id}' as measure_id
                ,'714' as submodule
                ,coalesce(numer, 0) as numer
                ,coalesce(denom, 0) as denom
                ,CASE
                    WHEN coalesce(denom, 0) <> 0
                        THEN coalesce(numer, 0) / denom
                    ELSE NULL
                    END AS mvalue
            from (

                select
                    submtg_state_cd,
                    count(*) as denom
                from
                    {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
                where
                    prcdr_cd
                    in('D1351', 'D2140', 'D2150', 'D2160',
                       'D2161', 'D2330', 'D2331', 'D2332',
                       'D2335', 'D2390', 'D2391', 'D2392',
                       'D2393', 'D2394', 'D3230', 'D3240',
                       'D3310', 'D3320', 'D3330')
                    and {DQM_Metadata.create_base_clh_view.claim_cat[x['claim_cat']]}

                group by
                    submtg_state_cd

            ) as d

            left join (

                select
                    submtg_state_cd,
                    count(*) as numer
                from
                    {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
                where
                    prcdr_cd
                    in('D1351', 'D2140', 'D2150', 'D2160',
                       'D2161', 'D2330', 'D2331', 'D2332',
                       'D2335', 'D2390', 'D2391', 'D2392',
                       'D2393', 'D2394', 'D3230', 'D3240',
                       'D3310', 'D3320', 'D3330')
                    and (
                        tooth_num is null or
                        tooth_num not rlike '[a-zA-Z1-9]'
                    )
                    and {DQM_Metadata.create_base_clh_view.claim_cat[x['claim_cat']]}
                group by
                    submtg_state_cd

            ) as n

            on d.submtg_state_cd = n.submtg_state_cd
        """

        dqm.logger.debug(z)

        rs = spark.sql(z)

        if (rs.count() > 0):
            return rs
        else:
            z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'714' as submodule
                    ,0 as numer
                    ,0 as denom
                    ,typeof(NULL) as mvalue
            """
            dqm.logger.debug(z)
            return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #  % of claim lines with non-missing Tooth Number that do not have a
    #  procedure code that indicates a CDT code
    #
    # --------------------------------------------------------------------
    def all35_3_or_4(spark, dqm: DQMeasures, measure_id, x):

        # DENOM
        #   Unique records within tmsis_cll_rec_othr_toc
        # WHERE
        #   tmsis_cll_rec_othr_toc.tooth_num
        #       contains ((any alpha character A-Z OR a-z) OR (any digit 1-9))

        # NUMER
        # Denominator definition
        # AND
        #   NOT
        #   (Length of tmsis_cll_rec_othr_toc.prcdr_cd = 5
        #           AND
        #       tmsis_cll_rec_othr_toc.prcdr_cd begins with "D"
        #           AND
        #       tmsis_cll_rec_othr_toc.prcdr_cd
        #           only contains digits 0-9 in positions 2-5)
        #   OR
        #   tmsis_cll_rec_othr_toc.prcdr_cd is NULL

        z = f"""
            select
                 '{dqm.state}' as submtg_state_cd
                ,'{measure_id}' as measure_id
                ,'714' as submodule
                ,coalesce(numer, 0) as numer
                ,coalesce(denom, 0) as denom
                ,CASE
                    WHEN coalesce(denom, 0) <> 0
                        THEN coalesce(numer, 0) / denom
                    ELSE NULL
                    END AS mvalue
            from (

                select
                    submtg_state_cd,
                    count(*) as denom
                from
                    {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
                where
                    tooth_num rlike '[a-zA-Z1-9]'
                    and {DQM_Metadata.create_base_clh_view.claim_cat[x['claim_cat']]}

                group by
                    submtg_state_cd

            ) as d

            left join (

                select
                    submtg_state_cd,
                    count(*) as numer
                from
                    {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
                where
                    tooth_num rlike '[a-zA-Z1-9]'
                    and not (
                      ( length(trim(prcdr_cd)) = 5
                        and prcdr_cd rlike '^[D]'
                        and prcdr_cd rlike '^.[0-9]'
                        and prcdr_cd rlike '^..[0-9]'
                        and prcdr_cd rlike '^...[0-9]'
                        and prcdr_cd rlike '^....[0-9]'
                      )
                      or prcdr_cd is NULL
                    )
                    and {DQM_Metadata.create_base_clh_view.claim_cat[x['claim_cat']]}

                group by
                    submtg_state_cd

            ) as n

            on d.submtg_state_cd = n.submtg_state_cd
        """

        dqm.logger.debug(z)

        rs = spark.sql(z)

        if (rs.count() > 0):
            return rs
        else:
            z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'714' as submodule
                    ,0 as numer
                    ,0 as denom
                    ,typeof(NULL) as mvalue
            """
            dqm.logger.debug(z)
            return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    v_table = {
            "all36_1": all36_1,

            "all34_1": all34_1,
            "all34_2": all34_2,

            "all35_1_or_2": all35_1_or_2,  # Medicaid
            "all35_1_or_2": all35_1_or_2,  # CHIP
            "all35_3_or_4": all35_3_or_4,  # Medicaid
            "all35_3_or_4": all35_3_or_4   # CHIP
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
