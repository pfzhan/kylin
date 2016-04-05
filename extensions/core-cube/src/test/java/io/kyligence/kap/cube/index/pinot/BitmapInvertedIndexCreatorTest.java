/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kyligence.kap.cube.index.pinot;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Ints;

import io.kyligence.kap.cube.index.pinot.FieldSpec.DataType;

public class BitmapInvertedIndexCreatorTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(BitmapInvertedIndexCreatorTest.class);

  @Test
  public void testSingleValue() throws IOException {

    boolean singleValue = true;
    String colName = "single_value_col";
    FieldSpec spec = new DimensionFieldSpec(colName, DataType.INT, singleValue);
    int numDocs = 20;
    int[] data = new int[numDocs];
    int cardinality = 10;

    File indexDirHeap = new File("/tmp/indexDirHeap");
    FileUtils.forceMkdir(indexDirHeap);
    indexDirHeap.mkdirs();
    File indexDirOffHeap = new File("/tmp/indexDirOffHeap");
    FileUtils.forceMkdir(indexDirOffHeap);
    indexDirOffHeap.mkdirs();
    File bitmapIndexFileOffHeap = new File(indexDirOffHeap,
        colName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    File bitmapIndexFileHeap =
        new File(indexDirHeap, colName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);

    // GENERATE RANDOM DATA SET
    Random r = new Random();
    Map<Integer, Set<Integer>> postingListMap = new HashMap<>();
    for (int i = 0; i < cardinality; i++) {
      postingListMap.put(i, new LinkedHashSet<Integer>());
    }
    for (int i = 0; i < numDocs; i++) {
      data[i] = r.nextInt(cardinality);
      LOGGER.debug("docId:" + i + "  dictId:" + data[i]);
      postingListMap.get(data[i]).add(i);
    }
    for (int i = 0; i < cardinality; i++) {
      LOGGER.debug("Posting list for " + i + " : " + postingListMap.get(i));
    }

    // GENERATE BITMAP USING OffHeapCreator and validate
    OffHeapBitmapInvertedIndexCreator offHeapCreator =
        new OffHeapBitmapInvertedIndexCreator(indexDirOffHeap, cardinality, numDocs, numDocs, spec);
    for (int i = 0; i < numDocs; i++) {
      offHeapCreator.add(i, data[i]);
    }
    offHeapCreator.seal();
    validate(colName, bitmapIndexFileOffHeap, cardinality, postingListMap);

    // GENERATE BITMAP USING HeapCreator and validate
    HeapBitmapInvertedIndexCreator heapCreator =
        new HeapBitmapInvertedIndexCreator(indexDirHeap, cardinality, numDocs, 0, spec);
    for (int i = 0; i < numDocs; i++) {
      heapCreator.add(i, data[i]);
    }
    heapCreator.seal();
    validate(colName, bitmapIndexFileHeap, cardinality, postingListMap);

    // assert that the file sizes and contents are the same
    Assert.assertEquals(bitmapIndexFileHeap.length(), bitmapIndexFileHeap.length());
    Assert.assertTrue(FileUtils.contentEquals(bitmapIndexFileHeap, bitmapIndexFileHeap));
    
    FileUtils.deleteQuietly(indexDirHeap);
    FileUtils.deleteQuietly(indexDirOffHeap);
  }

  private void validate(String colName, File bitmapIndexFile, int cardinality,
      Map<Integer, Set<Integer>> postingListMap) throws IOException {
    Assert.assertTrue(bitmapIndexFile.exists());
    BitmapInvertedIndexReader reader =
        new BitmapInvertedIndexReader(bitmapIndexFile, cardinality, false);
    for (int i = 0; i < cardinality; i++) {
      ImmutableRoaringBitmap bitmap = reader.getImmutable(i);
      Set<Integer> expected = postingListMap.get(i);
      Assert.assertEquals(expected.size(), bitmap.getCardinality());
      int[] actual = bitmap.toArray();
      List<Integer> actualList = Ints.asList(actual);
      Assert.assertTrue(expected.containsAll(actualList));
    }
  }

  @Test
  public void testMultiValue() throws IOException {
    boolean singleValue = false;
    String colName = "multi_value_col";
    FieldSpec spec = new DimensionFieldSpec(colName, DataType.INT, singleValue);
    int numDocs = 20;
    int[][] data = new int[numDocs][];
    int maxLength = 10;
    int cardinality = 10;
    File indexDirHeap = new File("/tmp/indexDirHeap");
    FileUtils.forceMkdir(indexDirHeap);
    indexDirHeap.mkdirs();
    File indexDirOffHeap = new File("/tmp/indexDirOffHeap");
    FileUtils.forceMkdir(indexDirOffHeap);
    indexDirOffHeap.mkdirs();
    File bitmapIndexFileOffHeap = new File(indexDirOffHeap,
        colName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    File bitmapIndexFileHeap =
        new File(indexDirHeap, colName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);

    // GENERATE RANDOM MULTI VALUE DATA SET
    Random r = new Random();
    Map<Integer, Set<Integer>> postingListMap = new HashMap<>();
    for (int i = 0; i < cardinality; i++) {
      postingListMap.put(i, new LinkedHashSet<Integer>());
    }
    int totalNumberOfEntries = 0;
    for (int docId = 0; docId < numDocs; docId++) {
      int length = r.nextInt(maxLength);
      data[docId] = new int[length];
      totalNumberOfEntries += length;
      for (int j = 0; j < length; j++) {
        data[docId][j] = r.nextInt(cardinality);
        postingListMap.get(data[docId][j]).add(docId);
      }
      LOGGER.debug("docId:" + docId + "  dictId:" + data[docId]);
    }
    for (int i = 0; i < cardinality; i++) {
      LOGGER.debug("Posting list for " + i + " : " + postingListMap.get(i));
    }

    // GENERATE BITMAP USING OffHeapCreator and validate
    OffHeapBitmapInvertedIndexCreator offHeapCreator =
        new OffHeapBitmapInvertedIndexCreator(indexDirOffHeap, cardinality, numDocs, totalNumberOfEntries, spec);
    for (int i = 0; i < numDocs; i++) {
      offHeapCreator.add(i, data[i]);
    }
    offHeapCreator.seal();
    validate(colName, bitmapIndexFileOffHeap, cardinality, postingListMap);

    // GENERATE BITMAP USING HeapCreator and validate
    HeapBitmapInvertedIndexCreator heapCreator =
        new HeapBitmapInvertedIndexCreator(indexDirHeap, cardinality, numDocs, totalNumberOfEntries, spec);
    for (int i = 0; i < numDocs; i++) {
      heapCreator.add(i, data[i]);
    }
    heapCreator.seal();
    validate(colName, bitmapIndexFileHeap, cardinality, postingListMap);

    // assert that the file sizes and contents are the same
    Assert.assertEquals(bitmapIndexFileHeap.length(), bitmapIndexFileHeap.length());
    Assert.assertTrue(FileUtils.contentEquals(bitmapIndexFileHeap, bitmapIndexFileHeap));
    FileUtils.deleteQuietly(indexDirHeap);
    FileUtils.deleteQuietly(indexDirOffHeap);

  }
}
