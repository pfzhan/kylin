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
	    count(DATE_TRUNC('YEAR', cast("CAL_DT" as string))),
	    count(DATE_TRUNC('YYYY', cast("CAL_DT" as string))),
	    count(DATE_TRUNC('YY', cast("CAL_DT" as string))),
	    count(DATE_TRUNC('MM', cast("CAL_DT" as string))),
	    count(DATE_TRUNC('MONTH', cast("CAL_DT" as string))),
	    count(DATE_TRUNC('DAY', cast("CAL_DT" as string))),
	    count(DATE_TRUNC('DD', cast("CAL_DT" as string))),
	    count(DATE_TRUNC('HOUR', cast("CAL_DT" as string))),
	    count(DATE_TRUNC('WEEK', cast("CAL_DT" as string))),

	    count(distinct DATE_TRUNC('YEAR', cast("CAL_DT" as string))),
	    count(distinct DATE_TRUNC('YYYY', cast("CAL_DT" as string))),
	    count(distinct DATE_TRUNC('YY', cast("CAL_DT" as string))),
	    count(distinct DATE_TRUNC('MM', cast("CAL_DT" as string))),
	    count(distinct DATE_TRUNC('MONTH', cast("CAL_DT" as string))),
	    count(distinct DATE_TRUNC('DAY', cast("CAL_DT" as string))),
	    count(distinct DATE_TRUNC('DD', cast("CAL_DT" as string))),
	    count(distinct DATE_TRUNC('HOUR', cast("CAL_DT" as string))),
	    count(distinct DATE_TRUNC('WEEK', cast("CAL_DT" as string)))
from TEST_KYLIN_FACT
