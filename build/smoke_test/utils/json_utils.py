#!/usr/bin/env python
# encoding: utf-8
#
# Copyright (C) 2020 Kyligence Inc. All rights reserved.
#
# http://kyligence.io
#
# This software is the confidential and proprietary information of
# Kyligence Inc. ("Confidential Information"). You shall not disclose
# such Confidential Information and shall use it only in accordance
# with the terms of the license agreement you entered into with
# Kyligence Inc.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

_array_types = (list, tuple, set)
_object_types = (dict,)


def compare_keys(actual, expected, ignore=[]):
    def _get_value(ignore):
        def get_value(key, container):
            if isinstance(container, _object_types):
                return container.get(key)
            elif isinstance(container, _array_types):
                errmsg = ''
                for item in container:
                    try:
                        compare_keys(item, key, ignore=ignore)
                        return item
                    except AssertionError as e:
                        errmsg += str(e) + '\n'
                raise AssertionError(errmsg)

            return None

        return get_value

    getvalue = _get_value(ignore)
    assert_failed = AssertionError(f'assert json failed, expected: [{expected}], actual: [{actual}]')

    if isinstance(expected, _array_types):
        if not isinstance(actual, _array_types):
            raise assert_failed
        for item in expected:
            compare_keys(getvalue(item, actual), item, ignore=ignore)

    elif isinstance(expected, _object_types):
        if not isinstance(actual, _object_types):
            raise assert_failed
        for key, value in expected.items():
            if key not in ignore:
                compare_keys(getvalue(key, actual), value, ignore=ignore)
            else:
                if key not in actual:
                    raise assert_failed
    else:
        if actual != expected:
            raise assert_failed