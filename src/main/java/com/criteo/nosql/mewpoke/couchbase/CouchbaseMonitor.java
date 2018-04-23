package com.criteo.nosql.mewpoke.couchbase;

import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.RequestHandler;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.config.RestApiResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.api.ClusterApiClient;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.util.NodeLocatorHelper;
import com.criteo.nosql.mewpoke.config.Config;
import com.criteo.nosql.mewpoke.discovery.Service;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


public class CouchbaseMonitor implements AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(CouchbaseMonitor.class);
    private static AtomicReference<CouchbaseEnvironment> couchbaseEnv = new AtomicReference<>(null);

    private final String serviceName;

    private final Cluster client;
    private final Bucket bucket;
    private final int httpDirectPort;
    private final long timeoutInMs;
    private final ClusterManager clusterManager;
    private final List<String> bucketStatsNames;
    private final List<String> xdcrStatsNames;
    private final List<String> queryStatsNames;

    // To avoid too many allocation at each iteration we allocate buffers upfront
    private final Map<InetSocketAddress, Long> setLatencies;
    private final Map<InetSocketAddress, Boolean> availability;
    private final Map<InetSocketAddress, String> nodesMembership;
    private final Map<InetSocketAddress, Map<String, Double>> nodesApiStats;
    private final Map<String, Map<String, Double>> xdcrStats;
    private final Map<String, Double> queryStats;

    private final RequestHandler requestHandler;
    private final Field nodesGetter;
    private final ArrayList<JsonDocument> docs;

    private CouchbaseMonitor(String serviceName, Cluster client, Bucket bucket, int httpDirectPort, long timeoutInMs, String username, String password, List<String> bucketStatsNames, List<String> xdcrStatsNames, List<String> queryStatsNames) throws NoSuchFieldException, IllegalAccessException {
        this.serviceName = serviceName;
        this.client = client;
        this.bucket = bucket;
        this.httpDirectPort = httpDirectPort;
        this.timeoutInMs = timeoutInMs;
        this.clusterManager = client.clusterManager(username, password);
        this.bucketStatsNames = bucketStatsNames;
        this.xdcrStatsNames = xdcrStatsNames;
        this.queryStatsNames = queryStatsNames;

        final CouchbaseCore c = ((CouchbaseCore) bucket.core());
        final Field f = c.getClass().getDeclaredField("requestHandler");
        f.setAccessible(true);
        this.requestHandler = (RequestHandler) f.get(c);
        this.nodesGetter = requestHandler.getClass().getDeclaredField("nodes");
        nodesGetter.setAccessible(true);

        this.setLatencies = new HashMap<>(getNodes().size());
        this.availability = new HashMap<>(getNodes().size());
        this.nodesMembership = new HashMap<>(getNodes().size());
        this.nodesApiStats = new HashMap<>(getNodes().size());
        this.xdcrStats = new HashMap<>();
        this.queryStats = new HashMap<>();

        // Generate requests that will spread on every nodes
        final NodeLocatorHelper locator = NodeLocatorHelper.create(bucket);
        final Map<InetAddress, String> keysHolder = new HashMap<>(locator.nodes().size());
        for (int i = 0; keysHolder.size() < locator.nodes().size() && i < 2000; i++) {
            final String key = "mewpoke_" + i;
            keysHolder.put(locator.activeNodeForId(key), key);
        }
        this.docs = new ArrayList<>(keysHolder.size());
        for (String key : keysHolder.values()) {
            this.docs.add(JsonDocument.create(key, null, -1));
        }
    }

    private InetSocketAddress nodeToInetAddress(Node n) {
        return new InetSocketAddress(n.hostname().hostname(), httpDirectPort);
    }

    public static Optional<CouchbaseMonitor> fromNodes(final Service service, Set<InetSocketAddress> endPoints, Config config) {
        if (endPoints.isEmpty()) {
            return Optional.empty();
        }

        final long timeoutInMs = config.getService().getTimeoutInSec() * 1000L;
        final String username = config.getService().getUsername();
        final String password = config.getService().getPassword();
        final String bucketName = service.getBucketName();
        final List<String> bucketStatsNames = (config.getCouchbaseStats() != null ? config.getCouchbaseStats().getBucket()
            : null);
        final List<String> xdcrStatsNames = (config.getCouchbaseStats() != null ? config.getCouchbaseStats().getXdcr()
            : null);
        final List<String> queryStatsNames = (config.getCouchbaseStats() != null ? config.getCouchbaseStats().getQuery()
            : null);

        Cluster client = null;
        Bucket bucket = null;
        try {
            final CouchbaseEnvironment env = couchbaseEnv.updateAndGet(e -> e == null ? DefaultCouchbaseEnvironment.builder().retryStrategy(FailFastRetryStrategy.INSTANCE).build() : e);
            final int httpDirectPort = env.bootstrapHttpDirectPort();
            client = CouchbaseCluster.create(env, endPoints.stream().map(e -> e.getHostString()).collect(Collectors.toList()));
            client.authenticate(username, password);
            bucket = client.openBucket(bucketName);
            return Optional.of(new CouchbaseMonitor(bucketName, client, bucket, httpDirectPort, timeoutInMs, username, password, bucketStatsNames, xdcrStatsNames, queryStatsNames));
        } catch (Exception e) {
            logger.error("Cannot create couchbase client for {}", bucketName, e);
            if (client != null) client.disconnect();
            if (bucket != null) bucket.close();
            return Optional.empty();
        }
    }

    private CopyOnWriteArrayList<Node> getNodes() {
        try {
            return (CopyOnWriteArrayList<Node>) nodesGetter.get(requestHandler);
        } catch (IllegalAccessException e) {
            return new CopyOnWriteArrayList<>();
        }
    }

    public boolean collectRebalanceOps() {
        try {
            return this.clusterManager.info().raw().getString("rebalanceStatus").equalsIgnoreCase("running");
        } catch (Exception e) {
            logger.error("Got an invalid JSON from the couchbase API for {} on rebalanceOps", serviceName, e);
            return false;
        }
    }

    public Map<InetSocketAddress, String> collectMembership() {
        try {
            final JsonArray clusterNodesStats = this.clusterManager.info().raw().getArray("nodes");
            clusterNodesStats.forEach(n -> {
                final JsonObject node = ((JsonObject) n);
                String hostname = node.getString("hostname");
                nodesMembership.put(new InetSocketAddress(hostname.substring(0, hostname.indexOf(':')), 8091), node.getString("clusterMembership"));
            });

            return Collections.unmodifiableMap(nodesMembership);
        } catch (Exception e) {
            logger.error("Got an invalid JSON from the couchbase API for {} on membership", serviceName, e);
            return Collections.emptyMap();
        }
    }

    private JsonNode getFromApi(final String uri) {
        final ClusterApiClient api = this.clusterManager.apiClient();
        final ObjectMapper objectMapper = new ObjectMapper();
        try {
            final RestApiResponse response = api.get(uri).execute();
            final int statusCode = response.httpStatus().code();
            if (statusCode / 100 == 2) {
                return objectMapper.readTree(response.body());
            } else {
                final String reasonPhrase = response.httpStatus().reasonPhrase();
                logger.error("Couchbase REST API request was not successful. URI '{}' returns {}. Reason is '{}'", uri, statusCode, reasonPhrase);
                return NullNode.instance;
            }
        } catch (Exception e) {
            logger.error("Couchbase REST API request failed. URI was '{}'.", uri, e);
            return NullNode.instance;
        }
    }

    public Map<InetSocketAddress, Map<String, Double>> collectApiStatsBucket() {

        for (Node n : getNodes()) {
            final String hostPort = nodeToInetAddress(n).toString().split("/")[1];
            final String uri = "/pools/default/buckets/" + this.bucket.name() + "/nodes/" + hostPort + "/stats";
            final JsonNode jsonBucketStatsNode = getFromApi(uri);

            final Map<String, Double> statsMap = new HashMap<>();

            final JsonNode hotKeys = jsonBucketStatsNode.findValue("hot_keys");
            if (hotKeys != null) {
                final int hotKeysCount = Math.min(hotKeys.size(), 3);
                for (int i = 0; i < hotKeysCount; i++) {
                    try {
                        final String key = "hot_keys." + i;
                        final double value = hotKeys.get(i).get("ops").asDouble();
                        statsMap.put(key, value);
                    } catch (Exception e) {
                        logger.error("Failed to get hot keys for {}.", hostPort, e);
                    }
                }
            }

            // We extract only configured stats. If undefined, we extract ALL stats.
            final JsonNode samples = jsonBucketStatsNode.findValue("samples");
            if (samples != null) {
                final Iterator<Map.Entry<String, JsonNode>> fields = samples.fields();
                while (fields.hasNext()) {
                    final Map.Entry<String, JsonNode> field = fields.next();
                    final String statName = field.getKey();
                    // TODO: I would prefer empty list as default. And allow pattern matching to configure ALL easily (**)
                    if (bucketStatsNames == null || bucketStatsNames.contains(statName)) {
                        try {
                            final double value = field.getValue().get(0).asDouble();
                            statsMap.put(statName, value);
                        } catch (Exception e) {
                            logger.error("Cannot fetch stat {} for bucket {} and node {}.", statName, this.bucket.name(), hostPort, e);
                        }
                    }
                }
            }

            nodesApiStats.put(nodeToInetAddress(n), statsMap);
        }
        return Collections.unmodifiableMap(nodesApiStats);
    }

    public Map<String, Map<String, Double>> collectApiStatsBucketXdcr() {

        final String uri = "/pools/default/buckets/@xdcr-" + this.bucket.name() + "/stats";
        final JsonNode samples = getFromApi(uri).findValue("samples");
        if (samples == null) {
            return Collections.emptyMap();
        }

        final JsonNode remoteClusters = getFromApi("/pools/default/remoteClusters");
        final Map<String, String> remoteClusterNames = new HashMap<>();
        for (JsonNode remoteCluster : remoteClusters) {
            final String name = remoteCluster.findValue("name").asText();
            final String uuid = remoteCluster.findValue("uuid").asText();
            remoteClusterNames.put(uuid, name);
        }

        // If a destination was removed, we clear XDCR stats
        xdcrStats.entrySet().removeIf(entry -> !remoteClusters.findValues("name").contains(entry.getKey()));

        final JsonNode tasks = getFromApi("/pools/default/tasks");

        for (JsonNode task : tasks) {
            if ( !task.findValue("type").asText().equals("xdcr"))
                continue;

            final String id = task.findValue("id").asText();
            final String[] idspl = id.split("/");
            final String remoteClusterName = remoteClusterNames.get(idspl[0]);
            final String dstbucket = idspl[2];
            final String prefix = "replications/" + id + "/";

            final Map<String, Double> statsMap = new HashMap<>();

            final Iterator<Map.Entry<String, JsonNode>> fields = samples.fields();
            while (fields.hasNext()) {
                final Map.Entry<String, JsonNode> field = fields.next();
                final String key = field.getKey();
                if (key.startsWith(prefix)) {
                    final String statName = key.substring(prefix.length());

                    // We extract only configured stats. If undefined, we extract ALL stats.
                    // TODO: I would prefer empty list as default. And allow pattern matching to configure ALL easily (**)
                    if (xdcrStatsNames == null || xdcrStatsNames.contains(statName)) {
                        try {
                            final double value = field.getValue().get(0).asDouble();
                            statsMap.put(statName, value);
                        } catch (Exception e) {
                            logger.error("Cannot fetch XDCR stat {} for replication {}.", field.getKey(), id, e);
                        }
                    }
                }
            }

            xdcrStats.put(remoteClusterName + " " +  dstbucket, statsMap);
        }

        return Collections.unmodifiableMap(xdcrStats);
    }

    public Map<String, Double> collectApiStatsQuery() {
        logger.info("collect query stat N1ql.");
        final String uri = "/pools/default/buckets/@query/stats";
        final JsonNode qstat = getFromApi(uri).findValue("samples");

        final Iterator<Map.Entry<String, JsonNode>> fields = qstat.fields();

        while(fields.hasNext()) {
            final Map.Entry<String, JsonNode> field = fields.next();
            final String statName = field.getKey();
            if (!statName.equals("timestamp") && (queryStatsNames == null || queryStatsNames.contains(statName))) {
                final double value = field.getValue().get(0).asDouble();
                queryStats.put(statName, value);
            }
        }

        return Collections.unmodifiableMap(queryStats);
    }

    public Map<InetSocketAddress, Boolean> collectAvailability() {
        for (Node n : getNodes()) {
            availability.put(nodeToInetAddress(n), n.state() == LifecycleState.CONNECTED);
        }
        return Collections.unmodifiableMap(availability);
    }

    public Map<InetSocketAddress, Long> collectLatencies(PersistTo persistTo, ReplicateTo replicateTo) {
        Observable
                .from(docs)
                .flatMap(doc -> {
                    final long start = System.nanoTime();
                    Tuple2<Observable<Document>, UpsertRequest> ret = bucket.async().upsertWithRequest(doc, persistTo, replicateTo);
                    return ret.value1()
                            .timeout(timeoutInMs, TimeUnit.MILLISECONDS)
                            .onErrorReturn(e -> null)
                            .doOnError(e -> setLatencies.put(nodeToInetAddress(ret.value2().node), timeoutInMs))
                            .doOnCompleted(() -> {
                                long stop = System.nanoTime();
                                setLatencies.put(nodeToInetAddress(ret.value2().node), (stop - start) / 1000L);
                            });
                })
                .last()
                .toBlocking()
                .firstOrDefault(null);

        return Collections.unmodifiableMap(setLatencies);
    }

    public Map<InetSocketAddress, Long> collectPersistToDiskLatencies() {
        return collectLatencies(PersistTo.MASTER, ReplicateTo.NONE);
    }

    @Override
    public void close() {
        try {
            bucket.close();
        } catch (Exception e) {
            logger.error("Cannot close bucket properly for {} ", serviceName, e);
        }
        try {
            client.disconnect();
        } catch (Exception e) {
            logger.error("Cannot disconnect couchbase client properly for {}", serviceName, e);
        }
    }
}
