package io.kyligence.kap.cube.raw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.common.restclient.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawTableInstanceManager {
    private static final Logger logger = LoggerFactory.getLogger(RawTableInstanceManager.class);

    public static final Serializer<RawTableInstance> INSTANCE_SERIALIZER = new JsonSerializer<RawTableInstance>(RawTableInstance.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, RawTableInstanceManager> CACHE = new ConcurrentHashMap<>();

    public static RawTableInstanceManager getInstance(KylinConfig config) {
        RawTableInstanceManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (RawTableInstanceManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new RawTableInstanceManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init RawTableInstanceManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ==========================================================

    private KylinConfig config;
    // name ==> RawTableDesc
    private CaseInsensitiveStringCache<RawTableInstance> rawTableInstanceMap;

    private RawTableInstanceManager(KylinConfig config) throws IOException {
        logger.info("Initializing RawTableDescManager with config " + config);
        this.config = config;
        this.rawTableInstanceMap = new CaseInsensitiveStringCache<RawTableInstance>(config, Broadcaster.TYPE.INVERTED_INDEX_DESC); // FIXME: broadcast raw table meta changes
        reloadAllRawTableInstance();
    }

    public RawTableInstance getRawTableInstance(String name) {
        return rawTableInstanceMap.get(name);
    }

    public List<RawTableInstance> listAllDesc() {
        return new ArrayList<RawTableInstance>(rawTableInstanceMap.values());
    }

    /**
     * Reload RawTableInstance from resource store. Triggered by an instance update event.
     */
    public RawTableInstance reloadRawTableInstanceLocal(String name) throws IOException {

        // Save Source
        String path = RawTableInstance.concatResourcePath(name);

        // Reload the RawTableInstance
        RawTableInstance instance = loadRawTableInstance(path);

        // Here replace the old one
        rawTableInstanceMap.putLocal(instance.getName(), instance);
        return instance;
    }

    private RawTableInstance loadRawTableInstance(String path) throws IOException {
        ResourceStore store = getStore();
        RawTableInstance instance = store.getResource(path, RawTableInstance.class, INSTANCE_SERIALIZER);

        instance.init(config);
        if (StringUtils.isBlank(instance.getName())) {
            throw new IllegalStateException("RawTableDesc name must not be blank");
        }

        return instance;
    }

    /**
     * Create a new RawTableInstance
     */
    public RawTableInstance createRawTableInstance(RawTableInstance rawTableInstance) throws IOException {
        if (rawTableInstance.getUuid() == null || rawTableInstance.getName() == null)
            throw new IllegalArgumentException();
        if (rawTableInstanceMap.containsKey(rawTableInstance.getName()))
            throw new IllegalArgumentException("RawTableInstance " + rawTableInstance.getName() + "' already exists");

        rawTableInstance.init(config);

        String path = rawTableInstance.getResourcePath();
        getStore().putResource(path, rawTableInstance, INSTANCE_SERIALIZER);
        rawTableInstanceMap.put(rawTableInstance.getName(), rawTableInstance);

        return rawTableInstance;
    }

    public void removeRawTableInstance(RawTableInstance rawTableInstance) throws IOException {
        String path = rawTableInstance.getResourcePath();
        getStore().deleteResource(path);
        rawTableInstanceMap.remove(rawTableInstance.getName());
    }

    public void removeRawTableInstanceLocal(String name) throws IOException {
        rawTableInstanceMap.removeLocal(name);
    }

    void reloadAllRawTableInstance() throws IOException {
        ResourceStore store = getStore();
        logger.info("Reloading RawTableInstance from folder " + store.getReadableResourcePath(RawTableInstance.RAW_TABLE_INSTANCE_RESOURCE_ROOT));

        rawTableInstanceMap.clear();

        List<String> paths = store.collectResourceRecursively(RawTableInstance.RAW_TABLE_INSTANCE_RESOURCE_ROOT, MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            RawTableInstance instance;
            try {
                instance = loadRawTableInstance(path);
            } catch (Exception e) {
                logger.error("Error loading RawTableInstance " + path, e);
                continue;
            }
            if (path.equals(instance.getResourcePath()) == false) {
                logger.error("Skip suspicious instance at " + path + ", " + instance + " should be at " + instance.getResourcePath());
                continue;
            }
            if (rawTableInstanceMap.containsKey(instance.getName())) {
                logger.error("Dup RawTableInstance name '" + instance.getName() + "' on path " + path);
                continue;
            }
            rawTableInstanceMap.putLocal(instance.getName(), instance);
        }

        logger.debug("Loaded " + rawTableInstanceMap.size() + " RawTableInstance(s)");
    }

    /**
     * Update RawTableDesc with the input. Broadcast the event into cluster
     */
    public RawTableInstance updateRawTableInstance(RawTableInstance instance) throws IOException {
        // Validate RawTableInstance
        if (instance.getUuid() == null || instance.getName() == null) {
            throw new IllegalArgumentException();
        }
        String name = instance.getName();
        if (!rawTableInstanceMap.containsKey(name)) {
            throw new IllegalArgumentException("RawTableDesc '" + name + "' does not exist.");
        }

        instance.init(config);

        // Save Source
        String path = instance.getResourcePath();
        getStore().putResource(path, instance, INSTANCE_SERIALIZER);

        // Reload the RawTableInstance
        RawTableInstance ninstance = loadRawTableInstance(path);
        // Here replace the old one
        rawTableInstanceMap.put(ninstance.getName(), instance);

        return ninstance;
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }
}
