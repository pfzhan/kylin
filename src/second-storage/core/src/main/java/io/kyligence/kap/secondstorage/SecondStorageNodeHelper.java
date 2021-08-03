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

package io.kyligence.kap.secondstorage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;

import com.google.common.base.Preconditions;

import io.kyligence.kap.secondstorage.config.Cluster;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;

public class SecondStorageNodeHelper {
    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private static final ConcurrentMap<String, Node> NODE_MAP = new ConcurrentHashMap<>();
    private static Function<Node, String> node2url;

    public static void initFromCluster(Cluster cluster, Function<Node, String> node2url) {
        synchronized (SecondStorageNodeHelper.class) {
            cluster.getNodes().forEach(node -> NODE_MAP.put(node.getName(), node));
            SecondStorageNodeHelper.node2url = node2url;
            initialized.set(true);
        }
    }

    public static List<String> resolve(List<String> names) {
        Preconditions.checkState(initialized.get());
        return names.stream().map(name -> {
            Preconditions.checkState(NODE_MAP.containsKey(name));
            return node2url.apply(NODE_MAP.get(name));
        }).collect(Collectors.toList());
    }

    public static List<String> resolveToJDBC(List<String> names) {
        return resolve(names);
    }

    public static String resolve(String name) {
        Preconditions.checkState(initialized.get());
        return node2url.apply(NODE_MAP.get(name));
    }

    private SecondStorageNodeHelper() {
    }

    public static Node getNode(String name) {
        return new Node(NODE_MAP.get(name));
    }

    public static List<Node> getALlNodes() {
        Preconditions.checkState(initialized.get());
        return new ArrayList<>(NODE_MAP.values());
    }

    public static List<Node> getALlNodesInProject(String project) {
        Optional<Manager<NodeGroup>> nodeGroupOptional = SecondStorageUtil.nodeGroupManager(KylinConfig.getInstanceFromEnv(), project);
        Preconditions.checkState(nodeGroupOptional.isPresent(), "node group manager is not init");
        return nodeGroupOptional.get().listAll().stream()
                .flatMap(nodeGroup -> nodeGroup.getNodeNames().stream()).map(NODE_MAP::get)
                .distinct()
                .collect(Collectors.toList());
    }

    public static List<String> getAllNames() {
        Preconditions.checkState(initialized.get());
        return new ArrayList<>(NODE_MAP.keySet());
    }

    public static Map<Integer, List<String>> separateReplicaGroup(int replicaNum, String... names) {
        Preconditions.checkArgument(names.length % replicaNum == 0);
        Map<Integer, List<String>> nodeMap = new HashMap<>(replicaNum);
        ListIterator<String> it = Arrays.asList(names).listIterator();
        while (it.hasNext()) {
            List<String> nodes = nodeMap.computeIfAbsent(it.nextIndex() % replicaNum, idx -> new ArrayList<>());
            nodes.add(it.next());
        }
        return nodeMap;
    }

    public static void clear() {
        synchronized (SecondStorageNodeHelper.class) {
            NODE_MAP.clear();
            initialized.set(false);
        }
    }
}
