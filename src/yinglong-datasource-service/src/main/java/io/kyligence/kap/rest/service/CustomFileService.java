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

package io.kyligence.kap.rest.service;

import static io.kyligence.kap.metadata.jar.JarTypeEnum.STREAMING_CUSTOM_PARSER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_JAR_EXISTS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_JAR_PARSERS_NOT_EXISTS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_NOT_JAR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_PARSER_EXISTS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_UPLOAD_JAR_FAILED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_UPLOAD_PARSER_LIMIT;

import java.io.IOException;
import java.security.AccessController;
import java.util.List;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.metadata.jar.JarInfo;
import io.kyligence.kap.metadata.jar.JarInfoManager;
import io.kyligence.kap.metadata.jar.JarTypeEnum;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.streaming.DataParserInfo;
import io.kyligence.kap.metadata.streaming.DataParserManager;
import io.kyligence.kap.parser.AbstractDataParser;
import io.kyligence.kap.parser.loader.AddToClassPathAction;
import io.kyligence.kap.parser.loader.ParserClassLoader;
import io.kyligence.kap.parser.loader.ParserClassLoaderState;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("customFileService")
public class CustomFileService extends BasicService {

    @Autowired
    private AclEvaluate aclEvaluate;

    @SneakyThrows
    public Set<String> uploadJar(MultipartFile jarFile, String project, String jarType) {
        aclEvaluate.checkProjectWritePermission(project);
        JarTypeEnum.validate(jarType);

        checkJarLegal(jarFile, project, jarType);
        Set<String> classList;
        if (JarTypeEnum.valueOf(jarType) == STREAMING_CUSTOM_PARSER) {
            classList = uploadStreamingCustomJar(jarFile, project, jarType);
        } else {
            classList = Sets.newHashSet();
        }
        return classList;
    }

    public String removeJar(String project, String jarFileName, String jarType) {
        aclEvaluate.checkProjectWritePermission(project);
        JarTypeEnum.validate(jarType);

        String removeJarName;
        if (JarTypeEnum.valueOf(jarType) == STREAMING_CUSTOM_PARSER) {
            removeJarName = removeStreamingCustomJar(project, jarFileName, jarType);
        } else {
            removeJarName = "";
        }
        return removeJarName;
    }

    public Set<String> uploadStreamingCustomJar(MultipartFile jarFile, String project, String jarType)
            throws IOException {
        String jarPath = uploadCustomJar(jarFile, project, jarType);
        return loadParserJar(jarFile.getOriginalFilename(), jarPath, project);
    }

    /**
     * check jar file is legal
     */
    public void checkJarLegal(MultipartFile jarFile, String project, String jarType) {
        checkJarLegal(jarFile.getOriginalFilename(), project, jarType);
    }

    /**
     * check jar file is legal
     * - The jar file does not exist in the metadata
     */
    public void checkJarLegal(String jarFileName, String project, String jarType) throws IllegalArgumentException {
        if (!StringUtils.endsWithIgnoreCase(jarFileName, ".jar")) {
            throw new KylinException(CUSTOM_PARSER_NOT_JAR, jarFileName);
        }

        String parseJarProjectPath = KylinConfig.getInstanceFromEnv().getHdfsCustomJarPath(project, jarType);
        String jarHdfsPath = parseJarProjectPath + jarFileName;

        // meta
        List<DataParserInfo> dataParserByJar = getManager(DataParserManager.class, project)
                .getDataParserByJar(jarHdfsPath);
        JarInfo jarInfo = getManager(JarInfoManager.class, project)
                .getJarInfo(String.format(Locale.ROOT, "%s_%s", JarTypeEnum.valueOf(jarType), jarFileName));
        if (!dataParserByJar.isEmpty() || jarInfo != null) {
            throw new KylinException(CUSTOM_PARSER_JAR_EXISTS, jarFileName);
        }
        log.info("The jar file [{}] can be loaded", jarFileName);
    }

    /**
     * upload custom jar to hdfs
     */
    public String uploadCustomJar(MultipartFile jarFile, String project, String jarType)
            throws KylinException, IOException {
        String jarFileName = jarFile.getOriginalFilename();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String parseJarProjectPath = kylinConfig.getHdfsCustomJarPath(project, jarType);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();

        String jarHdfsPath = parseJarProjectPath + jarFileName;
        Path path = new Path(jarHdfsPath);
        try (FSDataOutputStream out = fs.create(path)) {
            IOUtils.copyBytes(jarFile.getInputStream(), out, 4096, true);
        } catch (IOException e) {
            // delete hdfs jar file
            HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), path);
            throw new KylinException(CUSTOM_PARSER_UPLOAD_JAR_FAILED, e);
        }
        log.info("uploaded jar file [{}] to [{}]", jarFileName, jarHdfsPath);
        return jarHdfsPath;
    }

    /**
     * check class legal then add parser class to meta
     */
    public Set<String> loadParserJar(String jarName, String jarHdfsPath, String project) throws IOException {
        Set<String> dataParserSet = null;
        try {
            dataParserSet = checkParserLegal(jarName, jarHdfsPath, project);
            // load class to meta
            Set<String> finalDataParserSet = dataParserSet;
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                DataParserManager dataParserManager = getManager(DataParserManager.class, project);
                for (String className : finalDataParserSet) {
                    dataParserManager.createDataParserInfo(new DataParserInfo(project, className, jarHdfsPath));
                }
                return null;
            }, project);
        } catch (Exception e) {
            HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), new Path(jarHdfsPath));
            ExceptionUtils.rethrow(e);
        }
        return dataParserSet;
    }

    @SneakyThrows
    public Set<String> checkParserLegal(String jarName, String jarPath, String project) {
        Set<String> jarSet = Sets.newHashSet(jarPath);
        Set<String> dataParserSet = Sets.newHashSet();
        Set<String> loadParserSet = Sets.newHashSet();

        AddToClassPathAction addAction = new AddToClassPathAction(Thread.currentThread().getContextClassLoader(),
                jarSet);
        final ParserClassLoader parserClassLoader = AccessController.doPrivileged(addAction);
        val loadParsers = ServiceLoader.load(AbstractDataParser.class, parserClassLoader);
        for (val parser : loadParsers) {
            loadParserSet.add(parser.getClass().getName());
            log.info(jarName + " get parser: " + parser.getClass().getName());
        }
        parserClassLoader.close();
        if (CollectionUtils.isEmpty(loadParserSet)) {
            throw new KylinException(CUSTOM_PARSER_JAR_PARSERS_NOT_EXISTS, jarName);
        }
        Set<String> loadedClassMetaList = getManager(DataParserManager.class, project).listDataParserInfo().stream()
                .map(DataParserInfo::resourceName).collect(Collectors.toSet());

        for (String className : loadParserSet) {
            if (loadedClassMetaList.contains(className)) {
                throw new KylinException(CUSTOM_PARSER_PARSER_EXISTS, className);
            }
            dataParserSet.add(className);
        }
        int customParserLimit = getConfig().getCustomParserLimit();
        if ((loadedClassMetaList.size() - 1 + loadParserSet.size()) > customParserLimit) {
            throw new KylinException(CUSTOM_PARSER_UPLOAD_PARSER_LIMIT, customParserLimit);
        }

        ParserClassLoaderState instance = ParserClassLoaderState.getInstance(project);
        instance.registerJars(jarSet);
        JarInfo jarInfo = new JarInfo(project, jarName, jarPath, STREAMING_CUSTOM_PARSER);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(
                () -> getManager(JarInfoManager.class, project).createJarInfo(jarInfo), project);
        return dataParserSet;
    }

    /**
     * To delete a jar
     * first check whether the class is referenced by the table. If so, it cannot be deleted
     */
    @SneakyThrows
    public String removeStreamingCustomJar(String project, String jarFileName, String jarType) {
        JarTypeEnum.validate(jarType);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String parseJarProjectPath = kylinConfig.getHdfsCustomJarPath(project, jarType);
        String jarPath = parseJarProjectPath + jarFileName;
        // remove from meta
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            getManager(DataParserManager.class, project).removeJar(jarPath);
            getManager(JarInfoManager.class, project)
                    .removeJarInfo(String.format(Locale.ROOT, "%s_%s", JarTypeEnum.valueOf(jarType), jarFileName));
            return null;
        }, project);
        // remove from classloader
        ParserClassLoaderState.getInstance(project).unregisterJar(Sets.newHashSet(jarPath));
        // delete hdfs jar file
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), new Path(jarPath));
        log.info("remove jar {} success", jarPath);
        return jarFileName;
    }

}
