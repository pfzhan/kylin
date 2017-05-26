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

package io.kyligence.kap.rest.cache;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Test;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.ConfigurationFactory;

@Ignore
public class KAPEhcacheTest {

    @Test
    public void basicTest() throws InterruptedException, URISyntaxException, IOException {

        System.out.println("runtime used memory: "
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL resource = classLoader.getResource("ehcache-test.xml");

        String x = FileUtils.readFileToString(new File(resource.toURI()), Charset.defaultCharset());
        System.out.println(resource);
        System.out.println(x);

        Configuration conf = ConfigurationFactory.parseConfiguration(resource);
        CacheManager cacheManager = CacheManager.create(conf);

        //Create a Cache specifying its configuration.
        Cache testCache = cacheManager.getCache("SequenceSQLResults");

        System.out.println("runtime used memory: "
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");
        byte[] blob = null;
        Random random = new Random();
        int msize = 40;

        blob = new byte[(1024 * msize * 1024)];//400M
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) random.nextInt();
        }

        testCache.put(new Element("1", blob));
        System.out.println(testCache.get("1") == null);
        System.out.println(testCache.getSize());
        System.out.println(testCache.getStatistics().getLocalHeapSizeInBytes());
        System.out.println(testCache.getStatistics().getLocalDiskSizeInBytes());
        System.out.println("runtime used memory: "
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");
        System.gc();
        Thread.sleep(5000);
        System.out.println("runtime used memory: "
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");

        blob = new byte[(1024 * msize * 1024)];//400M
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) random.nextInt();
        }
        testCache.put(new Element("2", blob));
        System.out.println(testCache.get("2") == null);
        System.out.println(testCache.get("1") == null);
        System.out.println(testCache.getSize());
        System.out.println(testCache.getStatistics().getLocalHeapSizeInBytes());
        System.out.println(testCache.getStatistics().getLocalDiskSizeInBytes());
        System.out.println("runtime used memory: "
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");
        System.gc();
        Thread.sleep(5000);
        System.out.println("runtime used memory: "
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");

        blob = new byte[(1024 * msize * 1024)];//400M
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) random.nextInt();
        }
        testCache.put(new Element("3", blob));
        System.out.println(testCache.get("1") == null);
        System.out.println(testCache.get("2") == null);
        System.out.println(testCache.get("3") == null);
        System.out.println(testCache.getSize());
        System.out.println(testCache.getStatistics().getLocalHeapSizeInBytes());
        System.out.println(testCache.getStatistics().getLocalDiskSizeInBytes());
        System.out.println("runtime used memory: "
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");
        System.gc();
        Thread.sleep(5000);
        System.out.println("runtime used memory: "
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");

        blob = new byte[(1024 * msize * 1024)];//400M
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) random.nextInt();
        }
        testCache.put(new Element("4", blob));
        System.out.println(testCache.get("1") == null);
        System.out.println(testCache.get("2") == null);
        System.out.println(testCache.get("3") == null);
        System.out.println(testCache.get("4") == null);
        System.out.println(testCache.getSize());
        System.out.println(testCache.getStatistics().getLocalHeapSizeInBytes());
        System.out.println(testCache.getStatistics().getLocalDiskSizeInBytes());
        System.out.println("runtime used memory: "
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");
        System.gc();
        Thread.sleep(5000);
        System.out.println("runtime used memory: "
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");

        cacheManager.shutdown();
    }
}
