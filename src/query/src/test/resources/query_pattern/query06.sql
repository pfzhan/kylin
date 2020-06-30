--
-- Copyright (C) 2020 Kyligence Inc. All rights reserved.
--
-- http://kyligence.io
--
-- This software is the confidential and proprietary information of
-- Kyligence Inc. ("Confidential Information"). You shall not disclose
-- such Confidential Information and shall use it only in accordance
-- with the terms of the license agreement you entered into with
-- Kyligence Inc.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
-- "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
-- LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
-- A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
-- OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
-- SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
-- LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
-- DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
-- THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
-- (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
-- OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
--
WITH
    T1  AS  (
        SELECT
            CASE
                WHEN
                    CLIENTVERSION   IS  NOT NULL
                AND MODEL           IS  NULL
                THEN
                    'DM_JOVI_VOICE_SESSION_INFO_D.CLIENTVERSION'
                WHEN
                    CLIENTVERSION   IS  NULL
                AND MODEL           IS  NOT NULL
                THEN
                    'DM_JOVI_VOICE_SESSION_INFO_D.MODEL'
                ELSE
                    'all'
            END
                                                                        AS  DIM1
        ,   CAST(COALESCE(CLIENTVERSION, MODEL, 'all')  AS  VARCHAR)    AS  DIM2
        ,   MEASURE
        FROM
            (
                SELECT
                    CLIENTVERSION
                ,   MODEL
                ,   AVG(MEASURE_D)  AS  MEASURE
                FROM
                    (
                        SELECT
                            DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT
                        ,   DM_JOVI_VOICE_SESSION_INFO_D.CLIENTVERSION
                        ,   DM_JOVI_VOICE_SESSION_INFO_D.MODEL
                        ,   SUM(DM_JOVI_VOICE_SESSION_INFO_D.SESSION_CNT)*1.0   AS  MEASURE_D
                        FROM
                            AI_ANALYSIS_PRD.DM_JOVI_VOICE_SESSION_INFO_D    AS  DM_JOVI_VOICE_SESSION_INFO_D
                        WHERE
                            DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT >=  '2019-09-29'
                        AND DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT <=  '2019-10-02'
                        GROUP BY
                        GROUPING                            SETS(DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, (DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, DM_JOVI_VOICE_SESSION_INFO_D.CLIENTVERSION), (DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, DM_JOVI_VOICE_SESSION_INFO_D.MODEL))
                        HAVING
                            SUM(DM_JOVI_VOICE_SESSION_INFO_D.SESSION_CNT)   >   0
                    )
                GROUP BY
                    CLIENTVERSION
                ,   MODEL
            )
        UNION ALL
        SELECT
            CASE
                WHEN
                    SYSTEMVERSION   IS  NOT NULL
                AND DROOPING_NAME   IS  NULL
                THEN
                    'DM_JOVI_VOICE_SESSION_INFO_D.SYSTEMVERSION'
                WHEN
                    SYSTEMVERSION   IS  NULL
                AND DROOPING_NAME   IS  NOT NULL
                THEN
                    'DM_JOVI_VOICE_SESSION_INFO_D.DROOPING_NAME'
                ELSE
                    'all'
            END
                                                                                AS  DIM1
        ,   CAST(COALESCE(SYSTEMVERSION, DROOPING_NAME, 'all')  AS  VARCHAR)    AS  DIM2
        ,   MEASURE
        FROM
            (
                SELECT
                    SYSTEMVERSION
                ,   DROOPING_NAME
                ,   AVG(MEASURE_D)  AS  MEASURE
                FROM
                    (
                        SELECT
                            DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT
                        ,   DM_JOVI_VOICE_SESSION_INFO_D.SYSTEMVERSION
                        ,   DM_JOVI_VOICE_SESSION_INFO_D.DROOPING_NAME
                        ,   SUM(DM_JOVI_VOICE_SESSION_INFO_D.SESSION_CNT)*1.0   AS  MEASURE_D
                        FROM
                            AI_ANALYSIS_PRD.DM_JOVI_VOICE_SESSION_INFO_D    AS  DM_JOVI_VOICE_SESSION_INFO_D
                        WHERE
                            DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT >=  '2019-09-29'
                        AND DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT <=  '2019-10-02'
                        GROUP BY
                        GROUPING                            SETS(DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, (DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, DM_JOVI_VOICE_SESSION_INFO_D.SYSTEMVERSION), (DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, DM_JOVI_VOICE_SESSION_INFO_D.DROOPING_NAME))
                        HAVING
                            SUM(DM_JOVI_VOICE_SESSION_INFO_D.SESSION_CNT)   >   0
                    )
                GROUP BY
                    SYSTEMVERSION
                ,   DROOPING_NAME
            )
        UNION ALL
        SELECT
            CASE
                WHEN
                    INTENT_NAME IS  NOT NULL
                AND IS_NEW      IS  NULL
                THEN
                    'DM_JOVI_VOICE_SESSION_INFO_D.INTENT_NAME'
                WHEN
                    INTENT_NAME IS  NULL
                AND IS_NEW      IS  NOT NULL
                THEN
                    'DM_JOVI_VOICE_SESSION_INFO_D.IS_NEW'
                ELSE
                    'all'
            END
                                                                        AS  DIM1
        ,   CAST(COALESCE(INTENT_NAME, IS_NEW, 'all')   AS  VARCHAR)    AS  DIM2
        ,   MEASURE
        FROM
            (
                SELECT
                    INTENT_NAME
                ,   IS_NEW
                ,   AVG(MEASURE_D)  AS  MEASURE
                FROM
                    (
                        SELECT
                            DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT
                        ,   DM_JOVI_VOICE_SESSION_INFO_D.INTENT_NAME
                        ,   DM_JOVI_VOICE_SESSION_INFO_D.IS_NEW
                        ,   SUM(DM_JOVI_VOICE_SESSION_INFO_D.SESSION_CNT)*1.0   AS  MEASURE_D
                        FROM
                            AI_ANALYSIS_PRD.DM_JOVI_VOICE_SESSION_INFO_D    AS  DM_JOVI_VOICE_SESSION_INFO_D
                        WHERE
                            DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT >=  '2019-09-29'
                        AND DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT <=  '2019-10-02'
                        GROUP BY
                        GROUPING                            SETS(DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, (DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, DM_JOVI_VOICE_SESSION_INFO_D.INTENT_NAME), (DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, DM_JOVI_VOICE_SESSION_INFO_D.IS_NEW))
                        HAVING
                            SUM(DM_JOVI_VOICE_SESSION_INFO_D.SESSION_CNT)   >   0
                    )
                GROUP BY
                    INTENT_NAME
                ,   IS_NEW
            )
    )
,   T2  AS  (
        SELECT
            CASE
                WHEN
                    CLIENTVERSION   IS  NOT NULL
                AND MODEL           IS  NULL
                THEN
                    'DM_JOVI_VOICE_SESSION_INFO_D.CLIENTVERSION'
                WHEN
                    CLIENTVERSION   IS  NULL
                AND MODEL           IS  NOT NULL
                THEN
                    'DM_JOVI_VOICE_SESSION_INFO_D.MODEL'
                ELSE
                    'all'
            END
                                                                        AS  DIM1
        ,   CAST(COALESCE(CLIENTVERSION, MODEL, 'all')  AS  VARCHAR)    AS  DIM2
        ,   MEASURE
        FROM
            (
                SELECT
                    CLIENTVERSION
                ,   MODEL
                ,   AVG(MEASURE_D)  AS  MEASURE
                FROM
                    (
                        SELECT
                            DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT
                        ,   DM_JOVI_VOICE_SESSION_INFO_D.CLIENTVERSION
                        ,   DM_JOVI_VOICE_SESSION_INFO_D.MODEL
                        ,   SUM(DM_JOVI_VOICE_SESSION_INFO_D.SESSION_CNT)*1.0   AS  MEASURE_D
                        FROM
                            AI_ANALYSIS_PRD.DM_JOVI_VOICE_SESSION_INFO_D    AS  DM_JOVI_VOICE_SESSION_INFO_D
                        WHERE
                            DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT >=  '2019-09-25'
                        AND DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT <=  '2019-09-28'
                        GROUP BY
                        GROUPING                            SETS(DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, (DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, DM_JOVI_VOICE_SESSION_INFO_D.CLIENTVERSION), (DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, DM_JOVI_VOICE_SESSION_INFO_D.MODEL))
                        HAVING
                            SUM(DM_JOVI_VOICE_SESSION_INFO_D.SESSION_CNT)   >   0
                    )
                GROUP BY
                    CLIENTVERSION
                ,   MODEL
            )
        UNION ALL
        SELECT
            CASE
                WHEN
                    SYSTEMVERSION   IS  NOT NULL
                AND DROOPING_NAME   IS  NULL
                THEN
                    'DM_JOVI_VOICE_SESSION_INFO_D.SYSTEMVERSION'
                WHEN
                    SYSTEMVERSION   IS  NULL
                AND DROOPING_NAME   IS  NOT NULL
                THEN
                    'DM_JOVI_VOICE_SESSION_INFO_D.DROOPING_NAME'
                ELSE
                    'all'
            END
                                                                                AS  DIM1
        ,   CAST(COALESCE(SYSTEMVERSION, DROOPING_NAME, 'all')  AS  VARCHAR)    AS  DIM2
        ,   MEASURE
        FROM
            (
                SELECT
                    SYSTEMVERSION
                ,   DROOPING_NAME
                ,   AVG(MEASURE_D)  AS  MEASURE
                FROM
                    (
                        SELECT
                            DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT
                        ,   DM_JOVI_VOICE_SESSION_INFO_D.SYSTEMVERSION
                        ,   DM_JOVI_VOICE_SESSION_INFO_D.DROOPING_NAME
                        ,   SUM(DM_JOVI_VOICE_SESSION_INFO_D.SESSION_CNT)*1.0   AS  MEASURE_D
                        FROM
                            AI_ANALYSIS_PRD.DM_JOVI_VOICE_SESSION_INFO_D    AS  DM_JOVI_VOICE_SESSION_INFO_D
                        WHERE
                            DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT >=  '2019-09-25'
                        AND DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT <=  '2019-09-28'
                        GROUP BY
                        GROUPING                            SETS(DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, (DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, DM_JOVI_VOICE_SESSION_INFO_D.SYSTEMVERSION), (DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, DM_JOVI_VOICE_SESSION_INFO_D.DROOPING_NAME))
                        HAVING
                            SUM(DM_JOVI_VOICE_SESSION_INFO_D.SESSION_CNT)   >   0
                    )
                GROUP BY
                    SYSTEMVERSION
                ,   DROOPING_NAME
            )
        UNION ALL
        SELECT
            CASE
                WHEN
                    INTENT_NAME IS  NOT NULL
                AND IS_NEW      IS  NULL
                THEN
                    'DM_JOVI_VOICE_SESSION_INFO_D.INTENT_NAME'
                WHEN
                    INTENT_NAME IS  NULL
                AND IS_NEW      IS  NOT NULL
                THEN
                    'DM_JOVI_VOICE_SESSION_INFO_D.IS_NEW'
                ELSE
                    'all'
            END
                                                                        AS  DIM1
        ,   CAST(COALESCE(INTENT_NAME, IS_NEW, 'all')   AS  VARCHAR)    AS  DIM2
        ,   MEASURE
        FROM
            (
                SELECT
                    INTENT_NAME
                ,   IS_NEW
                ,   AVG(MEASURE_D)  AS  MEASURE
                FROM
                    (
                        SELECT
                            DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT
                        ,   DM_JOVI_VOICE_SESSION_INFO_D.INTENT_NAME
                        ,   DM_JOVI_VOICE_SESSION_INFO_D.IS_NEW
                        ,   SUM(DM_JOVI_VOICE_SESSION_INFO_D.SESSION_CNT)*1.0   AS  MEASURE_D
                        FROM
                            AI_ANALYSIS_PRD.DM_JOVI_VOICE_SESSION_INFO_D    AS  DM_JOVI_VOICE_SESSION_INFO_D
                        WHERE
                            DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT >=  '2019-09-25'
                        AND DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT <=  '2019-09-28'
                        GROUP BY
                        GROUPING                            SETS(DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, (DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, DM_JOVI_VOICE_SESSION_INFO_D.INTENT_NAME), (DM_JOVI_VOICE_SESSION_INFO_D.PRT_DT, DM_JOVI_VOICE_SESSION_INFO_D.IS_NEW))
                        HAVING
                            SUM(DM_JOVI_VOICE_SESSION_INFO_D.SESSION_CNT)   >   0
                    )
                GROUP BY
                    INTENT_NAME
                ,   IS_NEW
            )
    )
,   T3  AS  (
        SELECT
            T2.DIM1
        ,   T2.DIM2
        ,   COALESCE(T1.MEASURE, 0)                     MEASUREJIQI
        ,   COALESCE(T2.MEASURE, 0)                     MEASUREBENQI
        ,   COALESCE((T2.MEASURE*1.0/T1.MEASURE-1), 0)  MEASUREGAP
        FROM
            T2
        LEFT JOIN
            T1
        ON
            T2.DIM1 =   T1.DIM1
        AND T2.DIM2 =   T1.DIM2
    )

SELECT
    T4.DIM1
,   T4.DIM2
,   T4.MEASUREJIQI
,   T4.MEASUREBENQI
,   T4.MEASUREGAP
,   T4.MEASUREBENQI*1.0/T5.MEASUREJIQI                                              RATIO
,   ABS((T4.MEASUREGAP*(T4.MEASUREBENQI*1.0/T5.MEASUREBENQI))/T5.MEASUREGAP*1.0)    CONTRIBUTION
FROM
    (
        SELECT
            'tag'
            TAG
        ,   *
        FROM
            T3
    )       T4
JOIN
    (
        SELECT
            'tag'
            TAG
        ,   *
        FROM
            T3
        WHERE
            DIM1    =   'all'
    )   T5
ON
    T4.TAG  =   T5.TAG