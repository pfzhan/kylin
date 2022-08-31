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
  create_time datetime DEFAULT CURRENT_TIMESTAMP,
  update_time datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  job_duration_millis bigint(10) NOT NULL DEFAULT '0' COMMENT 'total duration milliseconds',
  PRIMARY KEY (id),
  UNIQUE KEY uk_job_id (job_id)
) AUTO_INCREMENT=10000 ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
