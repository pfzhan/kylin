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


CREATE TABLE IF NOT EXISTS KE_IDENTIFIED_job_info (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  job_id varchar(100) NOT NULL,
  job_type varchar(50) NOT NULL,
  job_status varchar(50) NOT NULL,
  project varchar(512) NOT NULL,
  subject varchar(512) NOT NULL,
  model_id varchar(512) NOT NULL,
  mvcc bigint(10),
  job_content longblob NOT NULL,
  create_time datetime(3) DEFAULT CURRENT_TIMESTAMP(3),
  update_time datetime(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  job_duration_millis bigint(10) NOT NULL DEFAULT '0' COMMENT 'total duration milliseconds',
  PRIMARY KEY (id),
  UNIQUE KEY uk_job_id (job_id)
) AUTO_INCREMENT=10000 ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;