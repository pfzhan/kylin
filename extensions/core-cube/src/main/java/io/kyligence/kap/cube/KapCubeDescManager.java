package io.kyligence.kap.cube;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.common.restclient.CaseInsensitiveStringCache;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.validation.CubeMetadataValidator;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class KapCubeDescManager {

    private static final Logger logger = LoggerFactory.getLogger(KapCubeDescManager.class);

    public static final Serializer<KapCubeDesc> CUBE_DESC_SERIALIZER = new JsonSerializer<KapCubeDesc>(KapCubeDesc.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, KapCubeDescManager> CACHE = new ConcurrentHashMap<KylinConfig, KapCubeDescManager>();

    public static KapCubeDescManager getInstance(KylinConfig config) {
        KapCubeDescManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (KapCubeDescManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new KapCubeDescManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init CubeDescManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;
    // name ==> CubeDesc
    private CaseInsensitiveStringCache<KapCubeDesc> cubeDescMap;

    private KapCubeDescManager(KylinConfig config) throws IOException {
        logger.info("Initializing CubeDescManager with config " + config);
        this.config = config;
        this.cubeDescMap = new CaseInsensitiveStringCache<KapCubeDesc>(config, Broadcaster.TYPE.CUBE_DESC);
        reloadAllCubeDesc();
    }

    public CubeDesc getCubeDesc(String name) {
        return cubeDescMap.get(name);
    }

    public List<CubeDesc> listAllDesc() {
        return new ArrayList<CubeDesc>(cubeDescMap.values());
    }

    /**
     * Reload CubeDesc from resource store It will be triggered by an desc
     * update event.
     *
     * @param name
     * @throws IOException
     */
    public CubeDesc reloadCubeDescLocal(String name) throws IOException {

        // Save Source
        String path = CubeDesc.concatResourcePath(name);

        // Reload the CubeDesc
        KapCubeDesc ndesc = loadCubeDesc(path);

        // Here replace the old one
        cubeDescMap.putLocal(ndesc.getName(), ndesc);
        Cuboid.reloadCache(name);
        return ndesc;
    }

    private KapCubeDesc loadCubeDesc(String path) throws IOException {
        ResourceStore store = getStore();
        KapCubeDesc ndesc = store.getResource(path, KapCubeDesc.class, CUBE_DESC_SERIALIZER);

        if (StringUtils.isBlank(ndesc.getName())) {
            throw new IllegalStateException("CubeDesc name must not be blank");
        }

        ndesc.init(config, getMetadataManager().getAllTablesMap());

        if (ndesc.getError().isEmpty() == false) {
            throw new IllegalStateException("Cube desc at " + path + " has issues: " + ndesc.getError());
        }

        return ndesc;
    }

    /**
     * Create a new CubeDesc
     *
     * @param cubeDesc
     * @return
     * @throws IOException
     */
    public CubeDesc createCubeDesc(CubeDesc cubeDesc) throws IOException {
        cubeDesc = new KapCubeDesc(cubeDesc);
        if (cubeDesc.getUuid() == null || cubeDesc.getName() == null)
            throw new IllegalArgumentException();
        if (cubeDescMap.containsKey(cubeDesc.getName()))
            throw new IllegalArgumentException("CubeDesc '" + cubeDesc.getName() + "' already exists");

        try {
            cubeDesc.init(config, getMetadataManager().getAllTablesMap());
        } catch (IllegalStateException e) {
            cubeDesc.addError(e.getMessage(), true);
        }
        // Check base validation
        if (!cubeDesc.getError().isEmpty()) {
            return cubeDesc;
        }
        // Semantic validation
        CubeMetadataValidator validator = new CubeMetadataValidator();
        ValidateContext context = validator.validate(cubeDesc, true);
        if (!context.ifPass()) {
            return cubeDesc;
        }

        cubeDesc.setSignature(cubeDesc.calculateSignature());

        String path = cubeDesc.getResourcePath();
        getStore().putResource(path, (KapCubeDesc) cubeDesc, CUBE_DESC_SERIALIZER);
        cubeDescMap.put(cubeDesc.getName(), (KapCubeDesc) cubeDesc);

        return cubeDesc;
    }

    // remove cubeDesc
    public void removeCubeDesc(CubeDesc cubeDesc) throws IOException {
        String path = cubeDesc.getResourcePath();
        getStore().deleteResource(path);
        cubeDescMap.remove(cubeDesc.getName());
        Cuboid.reloadCache(cubeDesc.getName());
    }

    // remove cubeDesc
    public void removeLocalCubeDesc(String name) throws IOException {
        cubeDescMap.removeLocal(name);
        Cuboid.reloadCache(name);
    }

    private void reloadAllCubeDesc() throws IOException {
        ResourceStore store = getStore();
        logger.info("Reloading Cube Metadata from folder " + store.getReadableResourcePath(ResourceStore.CUBE_DESC_RESOURCE_ROOT));

        cubeDescMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.CUBE_DESC_RESOURCE_ROOT, MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            KapCubeDesc desc;
            try {
                desc = loadCubeDesc(path);
            } catch (Exception e) {
                logger.error("Error loading cube desc " + path, e);
                continue;
            }
            if (path.equals(desc.getResourcePath()) == false) {
                logger.error("Skip suspicious desc at " + path + ", " + desc + " should be at " + desc.getResourcePath());
                continue;
            }
            if (cubeDescMap.containsKey(desc.getName())) {
                logger.error("Dup CubeDesc name '" + desc.getName() + "' on path " + path);
                continue;
            }

            cubeDescMap.putLocal(desc.getName(), desc);
        }

        logger.debug("Loaded " + cubeDescMap.size() + " Cube(s)");
    }

    /**
     * Update CubeDesc with the input. Broadcast the event into cluster
     *
     * @param desc
     * @return
     * @throws IOException
     */
    public CubeDesc updateCubeDesc(CubeDesc desc) throws IOException {
        // Validate CubeDesc
        desc = new KapCubeDesc(desc);
        if (desc.getUuid() == null || desc.getName() == null) {
            throw new IllegalArgumentException();
        }
        String name = desc.getName();
        if (!cubeDescMap.containsKey(name)) {
            throw new IllegalArgumentException("CubeDesc '" + name + "' does not exist.");
        }

        try {
            desc.init(config, getMetadataManager().getAllTablesMap());
        } catch (IllegalStateException e) {
            desc.addError(e.getMessage(), true);
            return desc;
        } catch (IllegalArgumentException e) {
            desc.addError(e.getMessage(), true);
            return desc;
        }

        // Semantic validation
        CubeMetadataValidator validator = new CubeMetadataValidator();
        ValidateContext context = validator.validate(desc, true);
        if (!context.ifPass()) {
            return desc;
        }

        desc.setSignature(desc.calculateSignature());

        // Save Source
        String path = desc.getResourcePath();
        getStore().putResource(path, (KapCubeDesc) desc, CUBE_DESC_SERIALIZER);

        // Reload the CubeDesc
        CubeDesc ndesc = loadCubeDesc(path);
        // Here replace the old one
        cubeDescMap.put(ndesc.getName(), (KapCubeDesc) desc);

        return ndesc;
    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(config);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

}
