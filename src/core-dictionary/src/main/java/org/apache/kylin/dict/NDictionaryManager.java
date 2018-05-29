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

 
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.dict;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.IReadableTable.TableSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;

public class NDictionaryManager {

    private static final Logger logger = LoggerFactory.getLogger(NDictionaryManager.class);

    private static final NDictionaryInfo NONE_INDICATOR = new NDictionaryInfo();

    public static NDictionaryManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NDictionaryManager.class);
    }

    // called by reflection
    static NDictionaryManager newInstance(KylinConfig config, String project) throws IOException {
        return new NDictionaryManager(config, project);
    }

    // ============================================================================

    private KylinConfig config;
    private String project;
    private LoadingCache<String, NDictionaryInfo> dictCache; // resource

    private NDictionaryManager(KylinConfig config, String project) {
        this.config = config;
        this.project = project;
        this.dictCache = CacheBuilder.newBuilder()//
                .softValues()//
                .removalListener(new RemovalListener<String, NDictionaryInfo>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, NDictionaryInfo> notification) {
                        NDictionaryManager.logger.info("Dict with resource path " + notification.getKey()
                                + " is removed due to " + notification.getCause());
                    }
                })//
                .maximumSize(config.getCachedDictMaxEntrySize())//
                .expireAfterWrite(1, TimeUnit.DAYS).build(new CacheLoader<String, NDictionaryInfo>() {
                    @Override
                    public NDictionaryInfo load(String key) throws Exception {
                        NDictionaryInfo dictInfo = NDictionaryManager.this.load(key, true);
                        if (dictInfo == null) {
                            return NONE_INDICATOR;
                        } else {
                            return dictInfo;
                        }
                    }
                });
    }

    public Dictionary<String> getDictionary(String resourcePath) throws IOException {
        NDictionaryInfo dictInfo = getDictionaryInfo(resourcePath);
        return dictInfo == null ? null : dictInfo.getDictionaryObject();
    }

    public NDictionaryInfo getDictionaryInfo(final String resourcePath) throws IOException {
        try {
            NDictionaryInfo result = dictCache.get(resourcePath);
            if (result == NONE_INDICATOR) {
                return null;
            } else {
                return result;
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    /**
     * Save the dictionary as it is.
     * More often you should consider using its alternative trySaveNewDict to save dict space
     */
    public NDictionaryInfo forceSave(Dictionary<String> newDict, NDictionaryInfo newDictInfo) throws IOException {
        initDictInfo(newDict, newDictInfo);
        logger.info("force to save dict directly");
        return saveNewDict(newDictInfo);
    }

    /**
     * @return may return another dict that is a super set of the input
     * @throws IOException
     */
    public NDictionaryInfo trySaveNewDict(Dictionary<String> newDict, NDictionaryInfo newDictInfo) throws IOException {

        initDictInfo(newDict, newDictInfo);

        if (config.isGrowingDictEnabled()) {
            logger.info("Growing dict is enabled, merge with largest dictionary");
            NDictionaryInfo largestDictInfo = findLargestDictInfo(newDictInfo);
            if (largestDictInfo != null) {
                largestDictInfo = getDictionaryInfo(largestDictInfo.getResourcePath());
                Dictionary<String> largestDictObject = largestDictInfo.getDictionaryObject();
                if (largestDictObject.contains(newDict)) {
                    logger.info("dictionary content " + newDict + ", is contained by  dictionary at "
                            + largestDictInfo.getResourcePath());
                    return largestDictInfo;
                } else if (newDict.contains(largestDictObject)) {
                    logger.info("dictionary content " + newDict + " is by far the largest, save it");
                    return saveNewDict(newDictInfo);
                } else {
                    logger.info("merge dict and save...");
                    return mergeDictionary(Lists.newArrayList(newDictInfo, largestDictInfo));
                }
            } else {
                logger.info("first dict of this column, save it directly");
                return saveNewDict(newDictInfo);
            }
        } else {
            String dupDict = checkDupByContent(newDictInfo, newDict);
            if (dupDict != null) {
                logger.info("Identical dictionary content, reuse existing dictionary at " + dupDict);
                return getDictionaryInfo(dupDict);
            }

            return saveNewDict(newDictInfo);
        }
    }

    private String checkDupByContent(NDictionaryInfo dictInfo, Dictionary<String> dict) throws IOException {
        ResourceStore store = getStore();
        NavigableSet<String> existings = store.listResources(dictInfo.getResourceDir());
        if (existings == null)
            return null;

        logger.info("{} existing dictionaries of the same column", existings.size());
        if (existings.size() > 100) {
            logger.warn("Too many dictionaries under {}, dict count: {}", dictInfo.getResourceDir(), existings.size());
        }

        for (String existing : existings) {
            NDictionaryInfo existingInfo = getDictionaryInfo(existing);
            if (existingInfo != null && dict.equals(existingInfo.getDictionaryObject())) {
                return existing;
            }
        }

        return null;
    }

    private void initDictInfo(Dictionary<String> newDict, NDictionaryInfo newDictInfo) {
        newDictInfo.setCardinality(newDict.getSize());
        newDictInfo.setDictionaryObject(newDict);
        newDictInfo.setDictionaryClass(newDict.getClass().getName());
    }

    private NDictionaryInfo saveNewDict(NDictionaryInfo newDictInfo) throws IOException {

        save(newDictInfo);
        dictCache.put(newDictInfo.getResourcePath(), newDictInfo);

        return newDictInfo;
    }

    public NDictionaryInfo mergeDictionary(List<NDictionaryInfo> dicts) throws IOException {

        if (dicts.size() == 0)
            return null;

        if (dicts.size() == 1)
            return dicts.get(0);

        /**
         * AppendTrieDictionary needn't merge
         * more than one AppendTrieDictionary will generate when user use {@link SegmentAppendTrieDictBuilder}
         */
        for (NDictionaryInfo dict : dicts) {
            if (dict.getDictionaryClass().equals(AppendTrieDictionary.class.getName())) {
                return dict;
            }
        }

        NDictionaryInfo firstDictInfo = null;
        int totalSize = 0;
        for (NDictionaryInfo info : dicts) {
            // check
            if (firstDictInfo == null) {
                firstDictInfo = info;
            } else {
                if (!firstDictInfo.isDictOnSameColumn(info)) {
                    // don't throw exception, just output warning as legacy cube segment may build dict on PK
                    logger.warn("Merging dictionaries are not structurally equal : " + firstDictInfo.getResourcePath()
                            + " and " + info.getResourcePath());
                }
            }
            totalSize += info.getInput().getSize();
        }

        if (firstDictInfo == null) {
            throw new IllegalArgumentException("DictionaryManager.mergeDictionary input cannot be null");
        }

        //identical
        NDictionaryInfo newDictInfo = new NDictionaryInfo(firstDictInfo);
        TableSignature signature = newDictInfo.getInput();
        signature.setSize(totalSize);
        signature.setLastModifiedTime(System.currentTimeMillis());
        signature.setPath("merged_with_no_original_path");

        //        String dupDict = checkDupByInfo(newDictInfo);
        //        if (dupDict != null) {
        //            logger.info("Identical dictionary input " + newDictInfo.getInput() + ", reuse existing dictionary at " + dupDict);
        //            return getDictionaryInfo(dupDict);
        //        }

        //check for cases where merging dicts are actually same
        boolean identicalSourceDicts = true;
        for (int i = 1; i < dicts.size(); ++i) {
            if (!dicts.get(0).getDictionaryObject().equals(dicts.get(i).getDictionaryObject())) {
                identicalSourceDicts = false;
                break;
            }
        }

        if (identicalSourceDicts) {
            logger.info("Use one of the merging dictionaries directly");
            return dicts.get(0);
        } else {
            Dictionary<String> newDict = NDictionaryGenerator
                    .mergeDictionaries(DataType.getType(newDictInfo.getDataType()), dicts);
            return trySaveNewDict(newDict, newDictInfo);
        }
    }

    public NDictionaryInfo buildDictionary(TblColRef col, IReadableTable inpTable) throws IOException {
        return buildDictionary(col, inpTable, null);
    }

    public NDictionaryInfo buildDictionary(TblColRef col, IReadableTable inpTable, String builderClass)
            throws IOException {
        if (inpTable.exists() == false)
            return null;

        logger.info("building dictionary for " + col);

        NDictionaryInfo dictInfo = createDictionaryInfo(col, inpTable);
        String dupInfo = checkDupByInfo(dictInfo);
        if (dupInfo != null) {
            logger.info(
                    "Identical dictionary input " + dictInfo.getInput() + ", reuse existing dictionary at " + dupInfo);
            return getDictionaryInfo(dupInfo);
        }

        logger.info("Building dictionary object " + JsonUtil.writeValueAsString(dictInfo));

        Dictionary<String> dictionary;
        dictionary = buildDictFromReadableTable(inpTable, dictInfo, builderClass, col);
        return trySaveNewDict(dictionary, dictInfo);
    }

    private Dictionary<String> buildDictFromReadableTable(IReadableTable inpTable, NDictionaryInfo dictInfo,
            String builderClass, TblColRef col) throws IOException {
        Dictionary<String> dictionary;
        IDictionaryValueEnumerator columnValueEnumerator = null;
        try {
            columnValueEnumerator = new TableColumnValueEnumerator(inpTable.getReader(),
                    dictInfo.getSourceColumnIndex());
            if (builderClass == null) {
                dictionary = DictionaryGenerator.buildDictionary(DataType.getType(dictInfo.getDataType()),
                        columnValueEnumerator);
            } else {
                INDictionaryBuilder builder = (INDictionaryBuilder) ClassUtil.newInstance(builderClass);
                dictionary = NDictionaryGenerator.buildDictionary(builder, dictInfo, columnValueEnumerator);
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create dictionary on " + col, ex);
        } finally {
            if (columnValueEnumerator != null)
                columnValueEnumerator.close();
        }
        return dictionary;
    }

    public static Dictionary<String> buildDictionary(TblColRef col, NDictionaryInfo DictionaryInfo, String builderClass,
            IterableDictionaryValueEnumerator enumerator) throws IOException {
        Dictionary<String> dictionary;
        try {
            if (builderClass == null) {
                dictionary = NDictionaryGenerator.buildDictionary(col.getType(), enumerator);
            } else {
                INDictionaryBuilder builder = (INDictionaryBuilder) ClassUtil.newInstance(builderClass);
                dictionary = NDictionaryGenerator.buildDictionary(builder, DictionaryInfo, enumerator);
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create dictionary on " + col, ex);
        }
        return dictionary;
    }

    public NDictionaryInfo saveDictionary(TblColRef col, IReadableTable inpTable, Dictionary<String> dictionary)
            throws IOException {
        NDictionaryInfo dictInfo = createDictionaryInfo(col, inpTable);
        String dupInfo = checkDupByInfo(dictInfo);
        if (dupInfo != null) {
            logger.info(
                    "Identical dictionary input " + dictInfo.getInput() + ", reuse existing dictionary at " + dupInfo);
            return getDictionaryInfo(dupInfo);
        }

        return trySaveNewDict(dictionary, dictInfo);
    }

    private NDictionaryInfo createDictionaryInfo(TblColRef col, IReadableTable inpTable) throws IOException {
        TableSignature inputSig = inpTable.getSignature();
        if (inputSig == null) // table does not exists
            throw new IllegalStateException("Input table does not exist: " + inpTable);

        NDictionaryInfo dictInfo = new NDictionaryInfo(col.getColumnDesc(), col.getDatatype(), inputSig, project);
        return dictInfo;
    }

    private String checkDupByInfo(NDictionaryInfo dictInfo) throws IOException {
        final ResourceStore store = getStore();
        final List<DictionaryInfo> allResources = store.getAllResources(dictInfo.getResourceDir(), DictionaryInfo.class,
                DictionaryInfoSerializer.INFO_SERIALIZER);

        TableSignature input = dictInfo.getInput();

        for (DictionaryInfo DictionaryInfo : allResources) {
            if (input.equals(DictionaryInfo.getInput())) {
                return DictionaryInfo.getResourcePath();
            }
        }
        return null;
    }

    private NDictionaryInfo findLargestDictInfo(NDictionaryInfo dictInfo) throws IOException {
        final ResourceStore store = getStore();
        final List<NDictionaryInfo> allResources = store.getAllResources(dictInfo.getResourceDir(),
                NDictionaryInfo.class, NDictionaryInfoSerializer.INFO_SERIALIZER);

        NDictionaryInfo largestDict = null;
        for (NDictionaryInfo DictionaryInfo : allResources) {
            if (largestDict == null) {
                largestDict = DictionaryInfo;
                continue;
            }

            if (largestDict.getCardinality() < DictionaryInfo.getCardinality()) {
                largestDict = DictionaryInfo;
            }
        }
        return largestDict;
    }

    public void removeDictionary(String resourcePath) throws IOException {
        logger.info("Remvoing dict: " + resourcePath);
        ResourceStore store = getStore();
        store.deleteResource(resourcePath);
        dictCache.invalidate(resourcePath);
    }

    public void removeDictionaries(String srcTable, String srcCol) throws IOException {
        DictionaryInfo info = new DictionaryInfo();
        info.setSourceTable(srcTable);
        info.setSourceColumn(srcCol);

        ResourceStore store = getStore();
        NavigableSet<String> existings = store.listResources(info.getResourceDir());
        if (existings == null)
            return;

        for (String existing : existings)
            removeDictionary(existing);
    }

    void save(NDictionaryInfo dict) throws IOException {
        ResourceStore store = getStore();
        String path = dict.getResourcePath();
        logger.info("Saving dictionary at " + path);

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);
        NDictionaryInfoSerializer.FULL_SERIALIZER.serialize(dict, dout);
        dout.close();
        buf.close();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(buf.toByteArray());
        store.putResource(path, inputStream, System.currentTimeMillis());
        inputStream.close();
    }

    NDictionaryInfo load(String resourcePath, boolean loadDictObj) throws IOException {
        ResourceStore store = getStore();

        logger.info("DictionaryManager(" + System.identityHashCode(this) + ") loading DictionaryInfo(loadDictObj:"
                + loadDictObj + ") at " + resourcePath);
        NDictionaryInfo info = store.getResource(resourcePath, NDictionaryInfo.class,
                loadDictObj ? NDictionaryInfoSerializer.FULL_SERIALIZER : NDictionaryInfoSerializer.INFO_SERIALIZER);
        info.setProject(project);
        return info;
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(config);
    }

}
