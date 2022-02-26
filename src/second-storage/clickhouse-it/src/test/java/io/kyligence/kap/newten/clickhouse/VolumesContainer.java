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

package io.kyligence.kap.newten.clickhouse;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Bind;
import lombok.NonNull;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.SelinuxContext;
import org.testcontainers.containers.startupcheck.StartupCheckStrategy;
import org.testcontainers.containers.traits.LinkableContainer;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.images.ImagePullPolicy;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class VolumesContainer implements Container {

    private final com.github.dockerjava.api.model.Container innerContainer;

    public VolumesContainer(com.github.dockerjava.api.model.Container realContainer) {
        this.innerContainer = realContainer;
    }


    @Override
    public void setCommand(@NonNull String command) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCommand(@NonNull String... commandParts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addEnv(String key, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addFileSystemBind(String hostPath, String containerPath, BindMode mode, SelinuxContext selinuxContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addLink(LinkableContainer otherContainer, String alias) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addExposedPort(Integer port) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addExposedPorts(int... ports) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container waitingFor(@NonNull WaitStrategy waitStrategy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withFileSystemBind(String hostPath, String containerPath, BindMode mode) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withVolumesFrom(Container container, BindMode mode) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withExposedPorts(Integer... ports) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withCopyFileToContainer(MountableFile mountableFile, String containerPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withEnv(String key, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withLabel(String key, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withCommand(String cmd) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withCommand(String... commandParts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withExtraHost(String hostname, String ipAddress) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withNetworkMode(String networkMode) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withNetwork(Network network) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withNetworkAliases(String... aliases) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withImagePullPolicy(ImagePullPolicy policy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withClasspathResourceMapping(String resourcePath, String containerPath, BindMode mode, SelinuxContext selinuxContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withStartupTimeout(Duration startupTimeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withPrivilegedMode(boolean mode) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withMinimumRunningDuration(Duration minimumRunningDuration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withStartupCheckStrategy(StartupCheckStrategy strategy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withWorkingDirectory(String workDir) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDockerImageName(@NonNull String dockerImageName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull
    String getDockerImageName() {
        return innerContainer.getImage();
    }

    @Override
    public String getTestHostIpAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Integer> getExposedPorts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getPortBindings() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InspectContainerResponse getContainerInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getExtraHosts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<String> getImage() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getEnv() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> getEnvMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] getCommandParts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Bind> getBinds() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, LinkableContainer> getLinkedContainers() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DockerClient getDockerClient() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCommandParts(String[] commandParts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setWaitStrategy(WaitStrategy waitStrategy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLinkedContainers(Map linkedContainers) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBinds(List list) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setEnv(List env) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setImage(Future image) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setExtraHosts(List extraHosts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPortBindings(List portBindings) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setExposedPorts(List exposedPorts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withLogConsumer(Consumer consumer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withLabels(Map labels) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withEnv(Map env) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getContainerName() {
        return innerContainer.getNames()[0];
    }
}