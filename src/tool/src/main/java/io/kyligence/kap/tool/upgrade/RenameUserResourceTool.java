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

package io.kyligence.kap.tool.upgrade;

import static org.apache.kylin.common.persistence.ResourceStore.GLOBAL_PROJECT;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;

import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.acl.AclTCR;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RenameUserResourceTool extends ExecutableApplication {

    private Set<String> users = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private boolean collectOnly = true;

    private final Set<String> existsUserNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    private final Map<String, String> renameUserMap = new HashMap<>();

    private static final Option OPTION_DIR = OptionBuilder.hasArg().withArgName("dir")
            .withDescription("Specify the directory to operator").isRequired(true).create("dir");

    private static final Option OPTION_USERS = OptionBuilder.hasArg().withArgName("username")
            .withDescription("Specify users (optional)").isRequired(false).withLongOpt("user").create("u");

    private static final Option OPTION_COLLECT_ONLY = OptionBuilder.hasArg().withArgName("true/false")
            .withDescription("collect only, show rename resource.(default true)").isRequired(false)
            .withLongOpt("collect-only").create("collect");

    private static final Option OPTION_HELP = OptionBuilder.hasArg(false).withDescription("print help message.")
            .isRequired(false).withLongOpt("help").create("h");

    private KylinConfig config = KylinConfig.getInstanceFromEnv();

    private ResourceStore resourceStore;

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_DIR);
        options.addOption(OPTION_USERS);
        options.addOption(OPTION_COLLECT_ONLY);
        options.addOption(OPTION_HELP);
        return options;
    }

    private boolean printUsage(OptionsHelper optionsHelper) {
        boolean help = optionsHelper.hasOption(OPTION_HELP);
        if (help) {
            optionsHelper.printUsage(this.getClass().getName(), getOptions());
        }
        return help;
    }

    private void initOptionValues(OptionsHelper optionsHelper) {
        while (true) {
            System.out.println(
                    "This script will help you modify the duplicate user name.  The system will add a number after the group name created according to the modification time, for example abc-> abc1\n"
                            + "Please confirm if you need to execute the script？(y/n)");
            Scanner scanner = new Scanner(System.in, Charset.defaultCharset().name());

            String prompt = scanner.nextLine();

            if (StringUtils.equals("y", prompt)) {
                break;
            }

            if (StringUtils.equals("n", prompt)) {
                Unsafe.systemExit(0);
            }
        }

        if (optionsHelper.hasOption(OPTION_USERS)) {
            users.addAll(Arrays.asList(optionsHelper.getOptionValue(OPTION_USERS).split(",")));
        }

        if (optionsHelper.hasOption(OPTION_COLLECT_ONLY)) {
            collectOnly = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_COLLECT_ONLY));
        }

        String metadataUrl = getMetadataUrl(optionsHelper.getOptionValue(OPTION_DIR));

        config.setMetadataUrl(metadataUrl);

        resourceStore = ResourceStore.getKylinMetaStore(config);

        List<ManagedUser> managedUsers = NKylinUserManager.getInstance(config).list();
        existsUserNames.addAll(managedUsers.stream().map(ManagedUser::getUsername).collect(Collectors.toList()));
    }

    public static void main(String[] args) {
        val tool = new RenameUserResourceTool();
        tool.execute(args);
        System.out.println("Rename user resource finished.");
        Unsafe.systemExit(0);
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (printUsage(optionsHelper)) {
            return;
        }

        initOptionValues(optionsHelper);
        if (optionsHelper.hasOption(OPTION_USERS)) {
            String originUsername = optionsHelper.getOptionValue(OPTION_USERS);
            String destUsername;
            int index = originUsername.indexOf(':');
            if (index > 0) {
                destUsername = originUsername.substring(index + 1);
                originUsername = originUsername.substring(0, index);
            } else {
                destUsername = generateAvailableUsername(originUsername);
            }

            renameUserMap.put(originUsername, destUsername);

            NKylinUserManager kylinUserManager = NKylinUserManager.getInstance(config);
            ManagedUser managedUser = kylinUserManager.get(originUsername);
            if (managedUser == null) {
                System.out.printf(Locale.ROOT, "user %s does not exists%n", originUsername);
                Unsafe.systemExit(1);
            }
        } else {
            collectDuplicateUser();
        }

        List<RenameEntity> renameEntities = new ArrayList<>();

        for (Map.Entry<String, String> entry : renameUserMap.entrySet()) {
            String originUsername = entry.getKey();
            String destUsername = entry.getValue();

            renameEntities.addAll(renameUser(originUsername, destUsername));
        }

        for (RenameEntity renameEntity : renameEntities) {
            System.out.println(renameEntity);
        }

        if (!collectOnly) {
            for (RenameEntity renameEntity : renameEntities) {
                renameEntity.updateMetadata();
            }
        }
    }

    /**
     * collect duplicate user to map renameUserMap
     */
    private void collectDuplicateUser() {
        List<ManagedUser> managedUsers = NKylinUserManager.getInstance(config).list();

        final ConcurrentSkipListMap<String, List<ManagedUser>> duplicateUserMap = managedUsers.stream()
                .collect(Collectors.groupingByConcurrent(ManagedUser::getUsername,
                        () -> new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER), Collectors.toList()));

        for (Map.Entry<String, List<ManagedUser>> entry : duplicateUserMap.entrySet()) {
            List<ManagedUser> userList = entry.getValue().stream()
                    .sorted(Comparator.comparingLong(ManagedUser::getCreateTime)).collect(Collectors.toList());
            if (userList.size() == 1) {
                continue;
            }

            for (int i = 1; i < userList.size(); i++) {
                ManagedUser managedUser = userList.get(i);
                String destUserName = generateAvailableUsername(managedUser.getUsername());
                renameUserMap.put(managedUser.getUsername(), destUserName);
            }
        }
    }

    private List<RenameEntity> renameUser(String oriUsername, String destUsername) {
        List<RenameEntity> results = new ArrayList<>();
        List<ProjectInstance> allProjectInstanceList = NProjectManager.getInstance(config).listAllProjects();

        NKylinUserManager kylinUserManager = NKylinUserManager.getInstance(config);
        ManagedUser user = kylinUserManager.get(oriUsername);

        for (ProjectInstance projectInstance : allProjectInstanceList) {
            // user acl
            updateUserAcl(oriUsername, destUsername, projectInstance).ifPresent(results::add);

            // project acl
            updateProjectAcl(oriUsername, destUsername, projectInstance).ifPresent(results::add);

            // saved queries
            updateSavedQueries(oriUsername, destUsername, projectInstance).ifPresent(results::add);

            // project owner
            updateProjectOwner(oriUsername, destUsername, projectInstance).ifPresent(results::add);

            // model owner
            results.addAll(updateModelOwner(oriUsername, destUsername, projectInstance));
        }

        // user
        String oriResourcePath = GLOBAL_PROJECT + "/user" + "/" + user.getUsername();
        user.setUsername(destUsername);
        String destResourcePath = GLOBAL_PROJECT + "/user" + "/" + destUsername;

        results.add(new RenameEntity(oriResourcePath, destResourcePath, user, ManagedUser.class));

        return results;
    }

    /**
     * 
     * @param oriUsername
     * @param destUsername
     * @param projectInstance
     * @return
     */
    private Optional<RenameEntity> updateUserAcl(String oriUsername, String destUsername,
            ProjectInstance projectInstance) {
        Optional<RenameEntity> result = Optional.empty();
        String projectName = projectInstance.getName();
        AclTCRManager tcrManager = AclTCRManager.getInstance(config, projectName);
        AclTCR aclTCR = tcrManager.getAclTCR(oriUsername, true);

        if (aclTCR != null) {
            String oriUserAclPath = String.format(Locale.ROOT, "/%s/acl/user/%s%s", projectName, oriUsername,
                    MetadataConstants.FILE_SURFIX);
            String destUserAclPath = String.format(Locale.ROOT, "/%s/acl/user/%s%s", projectName, destUsername,
                    MetadataConstants.FILE_SURFIX);
            result = Optional.of(new RenameEntity(oriUserAclPath, destUserAclPath));
        }
        return result;
    }

    /**
     * 
     * @param oriUsername
     * @param destUsername
     * @param projectInstance
     * @return 
     */
    private Optional<RenameEntity> updateProjectAcl(String oriUsername, String destUsername,
            ProjectInstance projectInstance) {
        Optional<RenameEntity> result = Optional.empty();
        String projectAclPath = String.format(Locale.ROOT, "/_global/acl/%s", projectInstance.getUuid());
        RawResource rs = resourceStore.getResource(projectAclPath);
        if (rs == null) {
            return result;
        }

        try (InputStream is = rs.getByteSource().openStream()) {
            JsonNode aclJsonNode = JsonUtil.readValue(is, JsonNode.class);
            if (aclJsonNode.has("ownerInfo")) {
                JsonNode ownerInfo = aclJsonNode.get("ownerInfo");
                String sid = ownerInfo.get("sid").asText();
                boolean principal = ownerInfo.get("principal").asBoolean();
                if (principal && StringUtils.equals(sid, oriUsername)) {
                    ((ObjectNode) ownerInfo).put("sid", destUsername);
                }
            }

            if (aclJsonNode.has("entries")) {
                ArrayNode entries = (ArrayNode) aclJsonNode.get("entries");
                for (JsonNode entry : entries) {
                    // p for person
                    if (entry.has("p")) {
                        String p = entry.get("p").asText();
                        if (StringUtils.equals(p, oriUsername)) {
                            ((ObjectNode) entry).put("p", destUsername);
                        }
                    }
                }
            }

            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream dout = new DataOutputStream(buf);
            JsonUtil.writeValue(dout, aclJsonNode);
            dout.close();
            buf.close();

            ByteSource byteSource = ByteSource.wrap(buf.toByteArray());

            rs = new RawResource(projectAclPath, byteSource, System.currentTimeMillis(), rs.getMvcc());

        } catch (IOException e) {
            log.warn("read resource {} failed", projectAclPath);
        }
        result = Optional.of(new RenameEntity(projectAclPath, projectAclPath, rs));
        return result;
    }

    /**
     * 
     * @param oriUsername
     * @param destUsername
     * @param projectInstance
     * @return
     */
    private Optional<RenameEntity> updateSavedQueries(String oriUsername, String destUsername,
            ProjectInstance projectInstance) {
        Optional<RenameEntity> result = Optional.empty();

        String originSavedQueriesPath = "/" + projectInstance.getName() + "/query/" + oriUsername
                + MetadataConstants.FILE_SURFIX;
        RawResource rs = resourceStore.getResource(originSavedQueriesPath);

        if (rs != null) {
            String destSavedQueriesPath = "/" + projectInstance.getName() + "/query/" + destUsername
                    + MetadataConstants.FILE_SURFIX;
            result = Optional.of(new RenameEntity(originSavedQueriesPath, destSavedQueriesPath));
        }

        return result;
    }

    /**
     * 
     * @param oriUsername
     * @param destUsername
     * @param projectInstance
     * @return
     */
    private Optional<RenameEntity> updateProjectOwner(String oriUsername, String destUsername,
            ProjectInstance projectInstance) {
        Optional<RenameEntity> result = Optional.empty();
        String owner = projectInstance.getOwner();
        if (StringUtils.equals(owner, oriUsername)) {
            projectInstance.setOwner(destUsername);
            result = Optional.of(new RenameEntity(projectInstance.getResourcePath(), projectInstance.getResourcePath(),
                    projectInstance, ProjectInstance.class));
        }

        return result;
    }

    /**
     *
     * @param oriUsername
     * @param destUsername
     * @param projectInstance
     * @return
     */
    private List<RenameEntity> updateModelOwner(String oriUsername, String destUsername,
            ProjectInstance projectInstance) {
        List<RenameEntity> results = new ArrayList<>();
        NDataModelManager dataModelManager = NDataModelManager.getInstance(config, projectInstance.getName());
        List<NDataModel> dataModels = dataModelManager.listAllModels().stream()
                .filter(nDataModel -> StringUtils.equals(nDataModel.getOwner(), oriUsername))
                .collect(Collectors.toList());
        for (NDataModel dataModel : dataModels) {
            dataModel.setOwner(destUsername);
            results.add(new RenameEntity(dataModel.getResourcePath(), dataModel.getResourcePath(), dataModel,
                    NDataModel.class));
        }
        return results;
    }

    private String generateAvailableUsername(String originUserName) {
        if (renameUserMap.get(originUserName) != null) {
            return renameUserMap.get(originUserName);
        }

        String destName = generateAvailableResourceName(originUserName, existsUserNames);
        existsUserNames.add(destName);
        renameUserMap.put(originUserName, destName);
        return destName;
    }

    private String generateAvailableResourceName(String originName, Set<String> existsResourceNames) {
        int suffix = 1;
        while (true) {
            String destName = String.format(Locale.ROOT, "%s%s", originName, suffix);
            if (!existsResourceNames.contains(destName)) {
                return destName;
            }
            suffix++;
        }
    }

    private String getMetadataUrl(String rootPath) {
        if (rootPath.startsWith("file://")) {
            rootPath = rootPath.replace("file://", "");
            return org.apache.commons.lang3.StringUtils.appendIfMissing(rootPath, "/");
        } else {
            return org.apache.commons.lang3.StringUtils.appendIfMissing(rootPath, "/");

        }
    }
}
