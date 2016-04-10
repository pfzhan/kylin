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

package io.kyligence.kap.engine.mr;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.kyligence.kap.cube.ColumnIndexFactory;
import io.kyligence.kap.cube.index.IColumnForwardIndex;
import io.kyligence.kap.cube.index.IColumnInvertedIndex;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Merger;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dimension.Dictionary;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class MergeSecondaryIndexStep extends AbstractExecutable {
    private static final Logger logger = LoggerFactory.getLogger(MergeSecondaryIndexStep.class);

    public MergeSecondaryIndexStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager mgr = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = mgr.getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        final CubeSegment newSegment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));
        final List<CubeSegment> mergingSegments = cube.getMergingSegments(newSegment);
        final String indexPath = CubingExecutableUtil.getIndexPath(this.getParams());

        KylinConfig conf = cube.getConfig();
        Collections.sort(mergingSegments);

        try {

            //1. download index files to local
            File localTempFolder = downloadToLocal(mergingSegments);

            //2. merge
            List<File> newIndexFiles = mergeSegIndex(conf, newSegment, mergingSegments, localTempFolder);

            //3. upload new index files to hdfs
            uploadToHdfs(indexPath, newIndexFiles);

            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to merge secondary index files", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    private List<File> mergeSegIndex(KylinConfig conf, CubeSegment newSeg, List<CubeSegment> mergingSegments, File localTempFolder) throws IOException {
        final int colNeedIndex = newSeg.getCubeDesc().getRowkey().getRowKeyColumns().length; //FIXME: read from metadata
        final long baseCuboidId = Cuboid.getBaseCuboidId(newSeg.getCubeDesc());
        final Cuboid baseCuboid = Cuboid.findById(newSeg.getCubeDesc(), baseCuboidId);
        File newSegFolder = new File(localTempFolder, newSeg.getName());
        newSegFolder.mkdirs();

        final int numberOfSegments = mergingSegments.size();
        List<IndexTableTranslator> translators = Lists.newArrayList();
        for (int i = 0; i < numberOfSegments; i++) {
            translators.add(new IndexTableTranslator(conf, mergingSegments.get(i), newSeg, colNeedIndex, localTempFolder));
        }

        Iterator<ByteArray> merged = Iterators.mergeSorted(translators, new Comparator<ByteArray>() {

            @Override
            public int compare(ByteArray o1, ByteArray o2) {
                return o1.compareTo(o2);
            }

            @Override
            public boolean equals(Object obj) {
                return false;
            }
        });

        RowKeyColumnIO colIO = new RowKeyColumnIO(new CubeDimEncMap(newSeg));
        ColumnIndexWriter[] columnIndexWriters = new ColumnIndexWriter[colNeedIndex];
        int offset = 0;
        List<File> localFiles = Lists.newArrayList();
        for (int i = 0; i < colNeedIndex; i++) {
            TblColRef col = baseCuboid.getColumns().get(i);
            int colLength = colIO.getColumnLength(col);
            File fwdFile = new File(newSegFolder, col.getName() + ".fwd");
            File invFile = new File(newSegFolder, col.getName() + ".inv");
            columnIndexWriters[i] = new ColumnIndexWriter(newSeg, col, offset, colLength, fwdFile, invFile);
            localFiles.add(fwdFile);
            localFiles.add(invFile);
            offset += colLength;
        }

        ByteArray lastInserted = null;
        while (merged.hasNext()) {
            ByteArray byteArray = merged.next();
            if (lastInserted == null || !lastInserted.equals(byteArray)) {
                //write
                for (int i = 0; i < colNeedIndex; i++) {
                    columnIndexWriters[i].write(byteArray.array());
                }

                lastInserted = byteArray;
            }
        }
        for (int i = 0; i < colNeedIndex; i++) {
            columnIndexWriters[i].close();
        }

        return localFiles;

    }

    private File downloadToLocal(List<CubeSegment> mergingSegments) throws IOException {
        logger.info("downloading index files to local for merge");
        FileSystem fs = FileSystem.get(HadoopUtil.getCurrentConfiguration());

        File localFolder = new File(File.createTempFile("", "").getParent());
        logger.info("use local folder '" + localFolder.getAbsolutePath() + "'");
        for (CubeSegment seg : mergingSegments) {
            File folderForSeg = new File(localFolder, seg.getName());
            folderForSeg.mkdirs();
            FileStatus[] statuses = fs.listStatus(new Path(seg.getIndexPath()));
            for (int i = 0; i < statuses.length; i++) {
                if (statuses[i].isFile()) {
                    logger.info("copyToLocal: " + statuses[i].getPath());
                    fs.copyToLocalFile(false, statuses[i].getPath(), new Path(folderForSeg.toURI()));
                }
            }
        }

        logger.info("downloading finished.");
        return localFolder;
    }

    private void uploadToHdfs(String hdfsPath, List<File> newIndexFiles) throws IOException {

        // upload to hdfs
        try (FileSystem fs = FileSystem.get(HadoopUtil.getCurrentConfiguration())) {
            Path path = new Path(hdfsPath);
            fs.delete(path, true);
            fs.mkdirs(path);
            for (File f : newIndexFiles) {
                fs.copyFromLocalFile(true, new Path(f.toURI()), new Path(path, f.getName()));
                f.delete();
            }
        }

    }


    /**
     * translate a forward index table
     */
    private class IndexTableTranslator implements Iterator<ByteArray> {

        Dictionary[] oldDictionaries;
        Dictionary[] newDictionaries;
        int colNeedIndex;
        KylinConfig conf;
        IColumnForwardIndex.Reader[] columnReaders;
        int totalLength;
        int cursor;
        int totalRows;

        IndexTableTranslator(KylinConfig conf, CubeSegment oldSegment, CubeSegment newSegment, int colNeedIndex, File localTempFolder) {
            this.conf = conf;
            this.colNeedIndex = colNeedIndex;
            columnReaders = new IColumnForwardIndex.Reader[colNeedIndex];
            final long baseCuboidId = Cuboid.getBaseCuboidId(newSegment.getCubeDesc());
            final Cuboid baseCuboid = Cuboid.findById(newSegment.getCubeDesc(), baseCuboidId);
            try {
                for (int j = 0; j < colNeedIndex; j++) {
                    TblColRef column = baseCuboid.getColumns().get(j);
                    oldDictionaries[j] = DictionaryManager.getInstance(conf).getDictionary(oldSegment.getDictResPath(column));
                    newDictionaries[j] = DictionaryManager.getInstance(conf).getDictionary(newSegment.getDictResPath(column));
                    totalLength += newDictionaries[j].getSizeOfId();
                    columnReaders[j] = ColumnIndexFactory.createLocalForwardIndex(oldSegment, column, new File(new File(localTempFolder, oldSegment.getName()), column.getName() + ".fwd").getAbsolutePath()).getReader();
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalStateException();
            }

            totalRows = columnReaders[0].getNumberOfRows();
            cursor = 0;
        }

        byte[] getColumn(int row, int column) {
            int v = columnReaders[column].get(row);
            return oldDictionaries[column].getValueBytesFromId(v);
        }

        @Override
        public boolean hasNext() {
            return cursor < totalRows;
        }

        @Override
        public ByteArray next() {
            ByteArray value = translateRow(cursor);
            cursor++;
            return value;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        ByteArray translateRow(int row) {
            byte[] value = new byte[totalLength];
            int offset = 0;
            for (int i = 0; i < colNeedIndex; i++) {
                byte[] oldValue = getColumn(row, i);
                int newId = newDictionaries[i].getIdFromValueBytes(oldValue, 0, oldValue.length);
                BytesUtil.writeUnsigned(newId, value, offset, newDictionaries[i].getSizeOfId());
                offset += newDictionaries[i].getSizeOfId();
            }

            return new ByteArray(value);
        }

    }
}
