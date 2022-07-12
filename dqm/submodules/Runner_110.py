# --------------------------------------------------------------------
#
#
#
# --------------------------------------------------------------------
from dqm.DQMeasures import DQMeasures


class Runner_110:

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def run(spark, dqm: DQMeasures, measures: list = None) :
        dqm.logger.info('Module().run<series>.run() has been deprecated. Use dqm.run(spark) or dqm.run(spark, dqm.where(series="<series>")) instead.')
        if measures is None:
            return dqm.run(spark, dqm.reverse_measure_lookup[dqm.reverse_measure_lookup['series'] == '110']['measure_id'].tolist())
        else:
            return dqm.run(spark, measures)

    # --------------------------------------------------------------------
    #
    #  EL1.29
    #
    #  % of MSIS IDs with more than one valid, specified race value
    #
    # --------------------------------------------------------------------
    def el1_29(spark, dqm: DQMeasures, measure_id, x):

        # similar to EL1.10

        # 1)
        # SET Denominator = COUNT Unique tmsis_race_info.msis_ident_num records
        # WHERE
        # tmsis_race_info_Constraints

        z = f"""
                create or replace temporary view el1_29_denom as
                select
                    '{dqm.state}' as submtg_state_cd,
                    count(distinct msis_ident_num) as denom
                from
                    {dqm.taskprefix}_tmsis_race_info
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # 2)
        # SET Numerator = COUNT Denominator records
        # WHERE
        # tmsis_race_info.race_cd = ("001", "002", "003", "004", "005", "006",
        #                            "007", "008", "009", "010", "011", "012",
        #                            "013", "014", "015", "016", or "018"")
        # AND
        # distinct (tmsis_race_info.msis_ident_num and tmsis_race_info.race_cd) > 1

        z = f"""
                create or replace temporary view el1_29_numer as
                select distinct
                    '{dqm.state}' as submtg_state_cd,
                    count(distinct msis_ident_num) as numer
                from (
                    select
                        msis_ident_num,
                        count(distinct race_cd) as r
                    from
                        {dqm.taskprefix}_tmsis_race_info
                    where
                        race_cd in ('001','002','003','004','005','006','007','008','009','010',
                                    '011','012','013','014','015','016','018')
                    group by
                        msis_ident_num
                    having
                        r > 1)
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # 3) SET Measure_Current_Month = Numerator/ Denominator
        # 4) ROUND Measure_Current_Month to 2 decimal places (the nearest whole percent)

        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'110' as submodule
                    , n.numer
                    , d.denom
                    , round(n.numer / d.denom, 3) as mvalue
                from
                    el1_29_numer as n
                inner join
                    el1_29_denom as d
                    on
                        d.submtg_state_cd = n.submtg_state_cd
            """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #  EL19.1
    #
    #  % of MSIS IDs enrolled any day in the previous month but
    #  not any day in the current month, with missing, invalid, unknown,
    #  or other ELIGIBILITY-CHANGE-REASON
    #
    # --------------------------------------------------------------------
    def el19_1(spark, dqm: DQMeasures, measure_id, x):

        # 1
        # Unique tmsis_enrlmt_time_sgmt.msis_ident_num records
        # WHERE
        #   Ever_Eligible
        #       AND
        #   tmsis_enrlmt_time_sgmt_data.enrlmt_efctv_dt <= Measure_Month_End and is not NULL
        #       AND
        #   tmsis_enrlmt_time_sgmt_data.enrlmt_end_dt >= Measure_Month_Begin or is NULL
        #
        # (Identify MSIS IDs enrolled any day in the current month)

        z = f"""
                create or replace temporary view tscurrent_base as
                select
                     coalesce(a.msis_ident_num, b.msis_id) as msis_ident_num
                    ,submtg_state_orig as submtg_state_cd
                    ,b.*
                from
                    (select distinct
                        msis_ident_num
                    from
                        {dqm.taskprefix}_ever_elig) a
                inner join
                    tmsis_enrlmt_time_sgmt_data_view as b
                    on
                        a.msis_ident_num = b.msis_id
                    and {dqm.run_id_filter()}
                    and (
                            (enrlmt_efctv_dt <= '{dqm.m_end}' and enrlmt_efctv_dt is not null)
                        and (enrlmt_end_dt >= '{dqm.m_start}' or enrlmt_end_dt is null)
                    )
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view tscurrent as
                select distinct
                    a.msis_ident_num,
                    a.submtg_state_cd
                from
                    tscurrent_base as a
                inner join
                    (select distinct
                        msis_ident_num,
                        submtg_state_cd
                    from
                        {dqm.taskprefix}_ever_elig
                    where
                        ever_eligible = 1) as b
                    on
                        a.msis_ident_num = b.msis_ident_num and
                        a.submtg_state_cd = b.submtg_state_cd
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # 2
        # Unique tmsis_enrlmt_time_sgmt.msis_ident_num records
        # WHERE
        #   Ever_Eligible
        #       AND
        #   tmsis_enrlmt_time_sgmt_data.enrlmt_efctv_dt <= Previous Measure_Month_End and is not NULL
        #       AND
        #   tmsis_enrlmt_time_sgmt_data.enrlmt_end_dt >= Previous Measure_Month_Begin or is NULL
        #
        # (Identify MSIS IDs enrolled any day of the previous month)

        z = f"""
                create or replace temporary view tsprior_base as
                select
                     coalesce(a.msis_ident_num, b.msis_id) as msis_ident_num
                    ,submtg_state_orig as submtg_state_cd
                    ,b.*
                from
                    (select distinct
                        msis_ident_num
                    from
                        {dqm.taskprefix}_ever_elig) as a
                inner join
                    tmsis_enrlmt_time_sgmt_data_view as b
                    on
                        a.msis_ident_num = b.msis_id
                    and {dqm.run_id_filter()}
                    and (
                            (enrlmt_efctv_dt <= '{dqm.prior_m_end}' and enrlmt_efctv_dt is not null)
                        and (enrlmt_end_dt >= '{dqm.prior_m_start}' or enrlmt_end_dt is null)
                    )
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view tsprior as
                select distinct
                    a.msis_ident_num,
                    a.submtg_state_cd
                from
                    tsprior_base as a
                inner join
                    (select distinct
                        msis_ident_num,
                        submtg_state_cd
                    from
                        {dqm.taskprefix}_ever_elig
                    where
                        ever_eligible = 1) as b
                    on
                        a.msis_ident_num = b.msis_ident_num and
                        a.submtg_state_cd = b.submtg_state_cd
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # If the reporting period is Oct 2021, then Measure_Month_Begin is 10/1/2021 and Measure_Month_End is 10/31/2021.
        # So then, the Previous Measure_Month_Begin is 9/1/2021 and the Previous Measure_Month_End is 9/30/2021.

        # 3
        # SET Denominator =
        #   Unique tmsis_enrlmt_time_sgmt.msis_ident_num in Step 2 but not Step 1
        #
        # The denominator of the measure is the number of MSIS IDs enrolled any day
        # in the previous month but not any day of the current month

        z = f"""
                create or replace temporary view denom as
                select distinct
                    '{dqm.state}' as submtg_state_cd,
                    msis_ident_num
                from (
                    select distinct p.msis_ident_num from tsprior as p
                except
                    select distinct c.msis_ident_num from tscurrent as c
                )
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # 4
        # SET Numerator = Denominator Definition
        #       AND
        #   (tmsis_elgblty_dtrmnt.elgblty_chg_rsn_cd <>
        #       ("01", "02", "03", "04", "05", "06","07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20")
        #       OR
        #   tmsis_elgblty_dtrmnt.elgblty_chg_rsn is NULL)
        #   WHERE
        #       Ever_Eligible_Det
        #   AND
        #       tmsis_elgblty_dtrmnt.elgblty_dtrmnt_efctv_dt <= Previous Measure_Month_End and is not NULL
        #   AND
        #       tmsis_elgblty_dtrmnt.elgblty_dtrmnt_end_dt >= Previous Measure_Month_Begin or is NULL
        #
        #  The numerator of the measure is the number of MSIS IDs from the denominator that does not have a valid, known,
        #  non-missing value for ELIGIBILITY-CHANGE-REASON on the segment from the previous month.
        #
        #  Note: MSIS IDs should be counted in the numerator if they don't merge to the tmsis_elgblty_dtrmnt segment at all.
        #
        #  Nuanced Note: If there are multiple tmsis_elgblty_dtrmnt segments in the previous month for one MSIS ID,
        #  only consider the segment with the max(tmsis_elgblty_dtrmnt.elgblty_dtrmnt_end_dt) for this measure.

        z = f"""
                create or replace temporary view edprior as
                select
                    coalesce(a.msis_ident_num, b.msis_id) as msis_ident_num
                    ,submtg_state_orig as submtg_state_cd
                    ,b.*
                from
                    (select distinct
                        msis_ident_num
                    from
                        {dqm.taskprefix}_ever_elig) a
                inner join
                    tmsis_elgblty_dtrmnt_view b
                    on
                        a.msis_ident_num = b.msis_id
                    and {dqm.run_id_filter()}
                    and prmry_elgblty_grp_ind = '1'
                    and (
                        (
                            (elgblty_dtrmnt_efctv_dt <= '{dqm.prior_m_end}' and elgblty_dtrmnt_efctv_dt is not null)
                        and (elgblty_dtrmnt_end_dt >= '{dqm.prior_m_start}' or elgblty_dtrmnt_end_dt is null)
                        )
                    )
           """

        dqm.logger.debug(z)
        spark.sql(z)

        z = f"""
                create or replace temporary view edprior_ever as
                select
                    a.msis_ident_num,
                    a.submtg_state_cd

                    ,last_value(a.elgblty_chg_rsn_cd) over
                        (partition by
                            a.msis_ident_num,
                            a.submtg_state_cd
                        order by
                            a.msis_ident_num,
                            a.submtg_state_cd,
                            a.elgblty_dtrmnt_end_dt,
                            a.elgblty_dtrmnt_efctv_dt) as last_chg_rsn_cd
                    ,row_number() over
                        (partition by
                            a.msis_ident_num,
                            a.submtg_state_cd
                        order by
                            a.msis_ident_num,
                            a.submtg_state_cd) as rn
                from
                    edprior a
                inner join (
                    select distinct
                        msis_ident_num,
                        submtg_state_cd
                    from
                        {dqm.taskprefix}_ever_elig_dtrmnt
                    where
                        ever_eligible_det = 1
                ) b
                    on
                        a.msis_ident_num = b.msis_ident_num and
                        a.submtg_state_cd = b.submtg_state_cd
            """

        dqm.logger.debug(z)
        spark.sql(z)

        z = """
                create or replace temporary view measure_data as
                select
                    a.*,
                    case when a.msis_ident_num is not null then 1 else 0 end denom,
                    case when b.last_chg_rsn_cd is null or
                                b.last_chg_rsn_cd not in (
                                    '01','02','03','04','05','06','07','08','09','10',
                                    '11','12','13','14','15','16','17','18','19','20')
                        then 1
                        else 0 end as numer
                from
                    denom a
                left join
                    (select * from edprior_ever where rn = 1) b
                    on
                            a.msis_ident_num = b.msis_ident_num
                        and a.submtg_state_cd = b.submtg_state_cd
            """

        dqm.logger.debug(z)
        spark.sql(z)

        # 5
        # SET Measure_Current_Month = Numerator/ Denominator

        # 6
        # ROUND Measure_Current_Month to 3 decimal places (the nearest whole percent)

        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'110' as submodule
                    , sum(numer) as numer
                    , count(denom) as denom
                    , round(sum(numer) / count(denom), 3) as mvalue
                from
                    measure_data
            """

        dqm.logger.debug(z)

        return spark.sql(z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    v_table = {
            "el1_29": el1_29,
            "el19_1": el19_1
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
