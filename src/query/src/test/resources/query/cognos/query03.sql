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

SELECT "X1"."DIM1_1" "DIM1_1"
       ,"TABLE2"."DIM2_1" "DIM2_1"
       ,SUM("FACT"."M1") "M1"
       ,SUM("FACT"."M2") "M2"
  FROM
  ( (
  SELECT "F1" FROM "T1") X0 LEFT OUTER JOIN (SELECT "DIM1_1", "PK_1" FROM COGNOS"."TABLE1" WHERE DIM1_1 = '1') "X1" ON "FACT"."FK_1" = "X1"."PK_1")
  LEFT OUTER JOIN "COGNOS"."TABLE2" "TABLE2"
    ON "FACT"."FK_2" = "TABLE2"."PK_2"
 GROUP BY "TABLE2"."DIM2_1"
          ,"X1"."DIM1_1";