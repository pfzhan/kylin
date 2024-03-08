--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

select
CASE
WHEN "TEST_KYLIN"."TYPE" = 'abc' THEN 'abc'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'bcd' THEN 'bcd'
ELSE CASE
WHEN "TEST_KYLIN"."TYPE" = 'cde' THEN 'def'
ELSE CASE
WHEN "TEST_KYLIN"."TYPE" = 'efg' THEN 'fgh'
ELSE CASE
WHEN POSITION('ghi' IN "TEST_KYLIN"."NAME") <> 0 THEN 'hij'
ELSE CASE
WHEN POSITION(
'ijk' IN "TEST_KYLIN"."NAME"
) <> 0 THEN 'jkl'
ELSE CASE
WHEN POSITION(
'klm' IN "TEST_KYLIN"."NAME"
) <> 0 THEN 'lmn'
ELSE CASE
WHEN POSITION(
'mno' IN "TEST_KYLIN"."NAME"
) <> 0 THEN 'mno'
ELSE CASE
WHEN POSITION(
'nop' IN "TEST_KYLIN"."NAME"
) <> 0 THEN 'nop'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'opq' THEN 'mno'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'klm' THEN 'lmn'
ELSE CASE
WHEN "TEST_KYLIN"."NAME" = 'pqr' THEN 'rst'
ELSE CASE
WHEN "TEST_KYLIN"."NAME" = 'qrs' THEN 'stu'
ELSE CAST(
CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'nop' THEN 'nop'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'tuv' THEN 'tuv'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'uvw' THEN 'uvw'
ELSE CASE
WHEN POSITION('wxy' IN "TEST_KYLIN"."NAME") <> 0 THEN 'vwx'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'xyz' THEN 'xyz'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'ab' THEN 'uvw'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'bc' THEN 'bc'
ELSE CASE
WHEN POSITION(
'de' IN "TEST_KYLIN"."MARKET_NAME"
) <> 0 THEN 'cd'
ELSE CASE
WHEN "TEST_KYLIN"."MARKET_NAME" = 'ef' THEN 'fg'
ELSE CAST(
CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'gh' THEN 'gh'
ELSE CASE
WHEN "TEST_KYLIN"."MARKET_NAME" = 'hi' THEN 'hi'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'ij' THEN 'ij'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'jk' THEN 'jk'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'kl' THEN 'kl'
ELSE CASE
WHEN POSITION(
'lm' IN "TEST_KYLIN"."SOURCE_NAME"
) <> 0 THEN 'lm'
ELSE CASE
WHEN "TEST_KYLIN"."TYPE" = 'mn' THEN 'no'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'hi' THEN 'hi'
ELSE CASE
WHEN POSITION(
'pq' IN "TEST_KYLIN"."SOURCE_NAME"
) <> 0 THEN 'op'
ELSE CASE
WHEN "TEST_KYLIN"."MARKET_NAME" = 'qr' THEN 'qr'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'bcd' THEN 'bcd'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'ijk' THEN 'jkl'
ELSE CASE
WHEN POSITION(
'rs' IN "TEST_KYLIN"."SOURCE_NAME"
) <> 0 THEN 'rs'
ELSE CASE
WHEN POSITION('rs' IN "TEST_KYLIN"."TYPE") <> 0 THEN 'rs'
ELSE CASE
WHEN POSITION(
'st' IN "TEST_KYLIN"."MARKET_NAME"
) <> 0 THEN 'st'
ELSE CASE
WHEN POSITION(
'tu' IN "TEST_KYLIN"."MARKET_NAME"
) <> 0 THEN 'tu'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'uv' THEN 'st'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'vw' THEN 'st'
ELSE CAST(
CASE
WHEN "TEST_KYLIN"."TYPE" = 'wx' THEN 'qr'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'xy' THEN 'qr'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'yz' THEN 'qr'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'a' THEN 'qr'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'b' THEN 'qr'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'c' THEN 'qr'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'd' THEN 'qr'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'e' THEN 'qr'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'f' THEN 'qr'
ELSE CASE
WHEN "TEST_KYLIN"."SOURCE_NAME" = 'g' THEN 'qr'
ELSE 'def'
END
END
END
END
END
END
END
END
END
END AS VARCHAR(4)
)
END
END
END
END
END
END
END
END
END
END
END
END
END
END
END
END
END
END AS VARCHAR(6)
)
END
END
END
END
END
END
END
END
END AS VARCHAR(7)
)
END
END
END
END
END
END
END
END
END
END
END
END
END
FROM
"SSB"."TEST_KYLIN"
