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
package io.kyligence.kap.rest.util;

import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.BeansException;
import org.springframework.beans.support.MutableSortDefinition;
import org.springframework.beans.support.SortDefinition;

/**
 * this class is used to performs a comparison of two beans
 * refer spring's PropertyComparator
 */
public class ModelTripleComparator implements Comparator<ModelTriple> {
    private static final Logger logger = LoggerFactory.getLogger(ModelTripleComparator.class);
    private final SortDefinition sortDefinition;
    private final BeanWrapperImpl beanWrapper = new BeanWrapperImpl(false);

    private int sortKey;

    public ModelTripleComparator(String property, boolean ascending, int sortKey) {
        this.sortDefinition = new MutableSortDefinition(property, false, ascending);
        this.sortKey = sortKey;
    }

    public int compare(ModelTriple o1, ModelTriple o2) {
        if (o1 == null || o2 == null) {
            return 0;
        }
        Object v1;
        Object v2;

        int result;
        try {
            if (sortKey == ModelTriple.SORT_KEY_DATAFLOW) {
                v1 = this.getPropertyValue(o1.getDataflow());
                v2 = this.getPropertyValue(o2.getDataflow());
            } else if (sortKey == ModelTriple.SORT_KEY_DATA_MODEL) {
                v1 = this.getPropertyValue(o1.getDataModel());
                v2 = this.getPropertyValue(o2.getDataModel());
            } else {
                v1 = o1.getCalcObject();
                v2 = o2.getCalcObject();
            }

            if (v1 != null) {
                result = v2 != null ? ((Comparable) v1).compareTo(v2) : -1;
            } else {
                result = v2 != null ? 1 : 0;
            }
        } catch (RuntimeException ex) {
            logger.warn("Could not sort objects [{}] and [{}]", o1, o2, ex);
            return 0;
        }

        return this.sortDefinition.isAscending() ? result : -result;
    }

    public Object getPropertyValue(Object obj) {
        try {
            this.beanWrapper.setWrappedInstance(obj);
            return this.beanWrapper.getPropertyValue(this.sortDefinition.getProperty());
        } catch (BeansException ex) {
            logger.info("PropertyComparator could not access property - treating as null for sorting", ex);
            return null;
        }
    }

}
