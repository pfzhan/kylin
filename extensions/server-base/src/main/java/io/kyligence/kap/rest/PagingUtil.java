/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.rest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class PagingUtil {
    
    public static <T> List<T> cutPage(List<T> full, int pageOffset, int pageSize) {
        if (full == null)
            return null;
        
        int begin = pageOffset * pageSize;
        int end = begin + pageSize;
        
        if (begin >= full.size())
            return Collections.emptyList();
        
        if (end > full.size())
            end = full.size();
        
        return full.subList(begin, end);
    }

    public static List<String> getIdentifierAfterFuzzyMatching(String nameSeg, boolean isCaseSensitive, List<String> noAccessList) {
        List<String> users = new ArrayList<>();
        if (nameSeg != null) {
            for (String u : noAccessList) {
                if (!isCaseSensitive && StringUtils.containsIgnoreCase(u, nameSeg)) {
                    users.add(u);
                }
                if (isCaseSensitive && StringUtils.contains(u, nameSeg)) {
                    users.add(u);
                }
            }
            return users;
        }
        return noAccessList;
    }
}
