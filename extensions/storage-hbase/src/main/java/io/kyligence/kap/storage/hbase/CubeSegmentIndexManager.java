package io.kyligence.kap.storage.hbase;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.dimension.Dictionary;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.cube.index.ColumnIndexFactory;
import io.kyligence.kap.cube.index.IColumnForwardIndex;
import io.kyligence.kap.cube.index.IColumnInvertedIndex;

public class CubeSegmentIndexManager {
    private static final Logger logger = LoggerFactory.getLogger(CubeSegmentIndexManager.class);

    // static cached instances
    private static final ConcurrentHashMap<CubeSegment, CubeSegmentIndexManager> CACHE = new ConcurrentHashMap<>();

    private CubeSegment cubeSegment;
    private Map<TblColRef, IColumnInvertedIndex> invertedIndexes = Maps.newHashMap();
    private Map<TblColRef, IColumnForwardIndex> forwardIndexes = Maps.newHashMap();
    private Map<TblColRef, Integer> dictionaryCardinalities = Maps.newHashMap();

    private CubeSegmentIndexManager(CubeSegment cubeSegment) throws IOException {
        this.cubeSegment = cubeSegment;
        reload();
    }

    public static CubeSegmentIndexManager getInstance(CubeSegment cubeSegment) {
        CubeSegmentIndexManager indexTable = CACHE.get(cubeSegment);
        if (indexTable != null) {
            return indexTable;
        }
        synchronized (CubeSegmentIndexManager.class) {
            indexTable = CACHE.get(cubeSegment);
            if (indexTable != null) {
                return indexTable;
            }
            try {
                indexTable = new CubeSegmentIndexManager(cubeSegment);
                CACHE.put(cubeSegment, indexTable);
                if (CACHE.size() > 1) {
                    logger.warn("More than one CubeSegmentIndexManager singleton exist");
                }
                return indexTable;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init CubeSegmentIndexManager from " + cubeSegment, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    public void reload() throws IOException {
        long startTime = System.currentTimeMillis();

        for (TblColRef tblCol : cubeSegment.getCubeDesc().getAllColumnsNeedDictionary()) {
            Dictionary dict = cubeSegment.getDictionary(tblCol);
            if (dict == null)
                continue;
            dictionaryCardinalities.put(tblCol, cubeSegment.getDictionary(tblCol).getSize());
            IColumnForwardIndex fwIdx = createLocalColumnForwardIndex(tblCol);
            IColumnInvertedIndex ivIdx = createLocalColumnInvertedIndex(tblCol);

            if (fwIdx == null || ivIdx == null)
                continue;
            invertedIndexes.put(tblCol, ivIdx);
            forwardIndexes.put(tblCol, fwIdx);
        }
        logger.info("CubeSegmentIndexManager was initialized in {} millis for segment {}.", System.currentTimeMillis() - startTime, cubeSegment);
    }

    private IColumnInvertedIndex createLocalColumnInvertedIndex(TblColRef tblColRef) throws IOException {
        String indexLocalPath = getLocalIndexPath(tblColRef, IndexType.INVERTED);
        if (indexLocalPath == null)
            return null;

        return ColumnIndexFactory.createLocalInvertedIndex(tblColRef.getName(), dictionaryCardinalities.get(tblColRef), indexLocalPath);
    }

    private IColumnForwardIndex createLocalColumnForwardIndex(TblColRef tblColRef) throws IOException {
        String indexLocalPath = getLocalIndexPath(tblColRef, IndexType.FORWARD);
        if (indexLocalPath == null)
            return null;

        return ColumnIndexFactory.createLocalForwardIndex(tblColRef.getName(), dictionaryCardinalities.get(tblColRef), indexLocalPath);
    }

    public IColumnForwardIndex getColumnForwardIndex(TblColRef tblColRef) {
        return forwardIndexes.get(tblColRef);
    }

    public IColumnInvertedIndex getColumnInvertedIndex(TblColRef tblColRef) {
        return invertedIndexes.get(tblColRef);
    }

    public enum IndexType {
        FORWARD, INVERTED
    }

    private String getLocalIndexPath(TblColRef tblColRef, IndexType indexType) throws IOException {
        StringBuilder indexFilename = new StringBuilder();
        indexFilename.append(cubeSegment.getIndexPath()).append("/");
        indexFilename.append(tblColRef.getName());
        switch (indexType) {
        case FORWARD:
            indexFilename.append(".fwd");
            break;
        case INVERTED:
            indexFilename.append(".inv");
            break;
        default:
            break;
        }

        URI indexPath = HadoopUtil.makeURI(indexFilename.toString());
        if (indexPath.toString().startsWith("file://")) {
            if (new File(indexPath.getPath()).exists()) {
                return indexPath.getPath();
            }
        } else {
            FileSystem fs = HadoopUtil.getFileSystem(indexPath.toString());
            if (fs.exists(new Path(HadoopUtil.makeURI(indexFilename.toString())))) {
                File localFile = File.createTempFile(tblColRef.getCanonicalName(), ".fwd");
                fs.copyToLocalFile(new Path(indexPath), new Path(localFile.getAbsolutePath()));
                localFile.deleteOnExit();
                return localFile.getAbsolutePath();
            }
        }

        return null;
    }

}
