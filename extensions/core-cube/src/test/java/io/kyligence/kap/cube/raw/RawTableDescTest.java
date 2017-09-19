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

package io.kyligence.kap.cube.raw;

import static io.kyligence.kap.cube.raw.RawTableColumnDesc.INDEX_DISCRETE;
import static io.kyligence.kap.cube.raw.RawTableColumnDesc.INDEX_SORTED;

import java.util.Collection;

import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class RawTableDescTest extends LocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    // Plain desc is desc containing no sortby and shardby columns
    private RawTableDesc getPlainDesc() {
        RawTableDescManager mgr = RawTableDescManager.getInstance(getTestConfig());
        final String name = "ci_left_join_cube";
        RawTableDesc result = mgr.getRawTableDesc(name);
        Assert.assertNotNull(result);

        // remove all sorted columns
        for (RawTableColumnDesc column : result.getColumnDescsInOrder()) {
            if (column.getIndex().equals(INDEX_SORTED)) {
                column.setIndex(INDEX_DISCRETE);
            }

            if (column.isSortby()) {
                column.setSortby(null);
            }

            if (column.isShardby()) {
                column.setShardby(null);
            }
        }

        return result;
    }

    @Test
    public void backwardCompatibleTest() {
        RawTableDesc rawDesc = getPlainDesc();
        for (RawTableColumnDesc column : rawDesc.getColumnDescsInOrder()) {
            if (column.getEncoding().equals("date")) {
                column.setIndex(INDEX_SORTED);
                break;
            }
        }

        rawDesc.init(getTestConfig());
        Assert.assertEquals(1, rawDesc.getSortbyColumnDescs().size());
        Assert.assertEquals(1, rawDesc.getSortbyColumns().size());
    }

    @Test
    public void noSortedColumnTest() {
        RawTableDesc rawDesc = getPlainDesc();

        try {
            rawDesc.init(getTestConfig());
        } catch (IllegalStateException ex) {
            Assert.assertTrue(ex.getMessage().contains("missing sortby column"));
            return;
        }

        Assert.fail("Should throw IllegalStateException, should at least contains one sorted column");
    }

    @Test
    public void noShardbyColumnTest() {
        RawTableDesc rawDesc = getPlainDesc();
        for (RawTableColumnDesc column : rawDesc.getColumnDescsInOrder()) {
            if (column.getEncoding().equals("date")) {
                column.setSortby(true);
                break;
            }
        }

        rawDesc.init(getTestConfig());
        Assert.assertEquals(rawDesc.getColumnDescsInOrder().size(), rawDesc.getColumnsInOrder().size());
        Assert.assertEquals(rawDesc.getColumnDescsInOrder().size(), rawDesc.getShardbyColumns().size());
    }

    @Test
    public void oneShardbyColumnTest() {
        RawTableDesc rawDesc = getPlainDesc();
        for (RawTableColumnDesc column : rawDesc.getColumnDescsInOrder()) {
            if (column.getEncoding().equals("date")) {
                column.setSortby(true);
                break;
            }
        }

        rawDesc.getColumnDescsInOrder().get(0).setShardby(true);

        rawDesc.init(getTestConfig());
        Assert.assertEquals(1, rawDesc.getShardbyColumns().size());
        Assert.assertEquals(rawDesc.getColumnDescsInOrder().get(0).getColumn(), rawDesc.getShardbyColumns().iterator().next());
    }

    @Test
    public void moreThanOneShardbyColumnsTest() {
        RawTableDesc rawDesc = getPlainDesc();
        for (RawTableColumnDesc column : rawDesc.getColumnDescsInOrder()) {
            if (column.getEncoding().equals("date")) {
                column.setSortby(true);
                break;
            }
        }

        rawDesc.getColumnDescsInOrder().get(0).setShardby(true);
        rawDesc.getColumnDescsInOrder().get(1).setShardby(true);

        try {
            rawDesc.init(getTestConfig());
        } catch (IllegalStateException ex) {
            Assert.assertTrue(ex.getMessage().contains("Only one shardby column is supported."));
            return;
        }

        Assert.fail("Should throw IllegalStateException, only no more than one shardby column supported");
    }

    @Test
    public void sortedColumnEncodingFailTest() {
        RawTableDesc rawDesc = getPlainDesc();
        for (RawTableColumnDesc column : rawDesc.getColumnDescsInOrder()) {
            if (!column.getEncoding().equals("date") && !column.getEncoding().equals("time") && !column.getEncoding().equals("integer")) {
                column.setSortby(true);
                break;
            }
        }

        try {
            rawDesc.init(getTestConfig());
        } catch (IllegalStateException ex) {
            Assert.assertTrue(ex.getMessage().contains("it should be integer, date or time"));
            return;
        }

        Assert.fail("Should throw IllegalStateException");
    }

    @Test
    public void sortedColumnEncodingPassTest() {
        String[] validEncoding = new String[] { "date", "time", "integer" };
        for (String encoding : validEncoding) {
            RawTableDesc rawDesc = getPlainDesc();
            boolean hasSetSortby = false;
            for (RawTableColumnDesc column : rawDesc.getColumnDescsInOrder()) {
                if (column.getEncoding().equals(encoding)) {
                    column.setSortby(true);
                    hasSetSortby = true;
                    break;
                }
            }

            if (!hasSetSortby) {
                RawTableColumnDesc column = rawDesc.getColumnDescsInOrder().iterator().next();
                column.setEncoding(encoding);
                column.setSortby(true);
            }

            rawDesc.init(getTestConfig());
            Collection<TblColRef> sortbyColumns = rawDesc.getSortbyColumns();
            Assert.assertEquals(1, sortbyColumns.size());

            for (RawTableColumnDesc column : rawDesc.getColumnDescsInOrder()) {
                if (column.getColumn().equals(sortbyColumns.iterator().next())) {
                    column.getEncoding().equals(encoding);
                }
            }
        }
    }
}
