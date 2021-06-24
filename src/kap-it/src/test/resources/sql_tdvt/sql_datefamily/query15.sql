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
select TIMESTAMPDIFF(day, date'2018-01-01', date'2018-10-10'),
        TIMESTAMPDIFF(day, timestamp'2018-01-01 00:00:00', date'2018-10-10'),
        TIMESTAMPDIFF(day, timestamp'2018-01-01 00:00:00', timestamp'2018-10-10 00:00:00'),
        TIMESTAMPDIFF(day, date'2018-01-01', timestamp'2018-10-10 00:00:00'),
        TIMESTAMPDIFF(year, date'2017-01-01', timestamp'2018-01-01 00:00:00'),
        TIMESTAMPDIFF(quarter, date'2018-01-01', timestamp'2018-10-10 00:00:00'),
        TIMESTAMPDIFF(month, date'2018-01-01', timestamp'2018-10-10 00:00:00'),
        TIMESTAMPDIFF(week, date'2018-01-01', timestamp'2018-10-10 00:00:00'),
        TIMESTAMPDIFF(hour, date'2018-01-01', timestamp'2018-10-10 00:00:00'),
        TIMESTAMPDIFF(minute, date'2018-01-01', timestamp'2018-10-10 00:00:00'),
        TIMESTAMPDIFF(second, date'2018-01-01', timestamp'2018-10-10 00:00:00')
