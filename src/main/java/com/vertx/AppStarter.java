package com.vertx;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.vertx.core.VertxOptions;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

import java.net.*;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.vertx.core.impl.Arguments.require;
import static java.util.Collections.list;

/**
 * Instructions :
 * <p>
 * 1 - Start this program from IDE on node one with hosts "node-one-IP", "node-two-IP"
 * 2 - Start same program from IDE on node two with host "node-two-IP" - Node one will automatically detect it.
 * 2 - Start same program from IDE on node three with host "node-two-IP", "node-three-IP" - Node one will automatically detect it.
 * <p>
 * Only first node in cluster will put data in cache.
 * Access given below URL from node-2 or node-3.
 * <p>
 * http://localhost:8080/mars
 * http://localhost:8080/alien
 * <p>
 * <p>
 * When you shut down a node, Hazelcast seem to take some time to update its registry, so give it like 10 seconds
 * before you read the cache again by hitting URL on browser. Generally it shouldn't take that much time.
 */
public class AppStarter {

    public static final String CACHE_MAP_NAME = "myMap";

    // TODO - Here you define the list of IP.
    private static final List<String> clusterHosts = Arrays.asList("10.84.131.214", "10.84.131.115");

    public static void main(final String... args) throws SocketException, UnknownHostException {
        String host = getHost();
        HazelcastClusterManager clusterManager = getClusterManager();
        VertxOptions options = new VertxOptions()
                .setClusterManager(clusterManager)
                .setClustered(true)

                // If there are multiple n/w interfaces, tell Vertx which one to use.
                .setClusterHost(host)

                // Vertx start a separate HTTP process on this port for event-bus communication
                // via TCP, and Vertx use this port to send event-bus communication via TCP.
                .setClusterPort(41232);

        Vertx.rxClusteredVertx(options).subscribe(vertx -> {
            HazelcastInstance hazelcastInstance = clusterManager.getHazelcastInstance();
            require(hazelcastInstance != null, "Hazelcast started successfully!");
            initializeCache();
            startHttpServer(vertx, host);
            registerVertxEventBusHandler(vertx);
        }, ex -> ex.printStackTrace());
    }

    private static String getHost() throws SocketException {
        // Identify which n/w interface should be used by Vertx.
        return list(NetworkInterface.getNetworkInterfaces()).stream()
                .flatMap(ni -> list(ni.getInetAddresses()).stream())
                .filter(address -> !address.isAnyLocalAddress())
                .filter(address -> !address.isMulticastAddress())
                .filter(address -> !address.isLoopbackAddress())
                .filter(address -> !(address instanceof Inet6Address))
                .map(InetAddress::getHostAddress)
                .filter(host -> host.startsWith("10.84.131"))
                .collect(Collectors.toList())
                .get(0);
    }

    private static void startHttpServer(final Vertx vertx, final String currentHost) {
        Router router = Router.router(vertx);

        // Failure handler.
        router.route().failureHandler(context -> {
            context.getDelegate().failure().printStackTrace();
            context.response()
                    .setChunked(true)
                    .write("Failure::" + context.getDelegate().failure().getMessage())
                    .end();
        });

        // HTTP request handler.
        router.get("/:key").handler(context -> {

            // Get the query parameters.
            String key = context.request().getParam("key");

            // Demo of sending messages on event-bus.
            vertx.eventBus().send("someHandler", key + ":: Host = " + currentHost);

            // Write HTTP response back to browser.
            context
                    .response()
                    .setChunked(true)
                    .write(readFromCache(key))
                    .end();
        });
        vertx.createHttpServer()
                .requestHandler(router::accept)
                .listen(8080, "0.0.0.0");
    }

    private static HazelcastClusterManager getClusterManager() throws SocketException {
        HazelcastClusterManager clusterManager = new HazelcastClusterManager();
        Config config = new Config()
                .setNetworkConfig(new NetworkConfig()
                        .setPort(5702)
                        .setPortAutoIncrement(false)
                        .setJoin(createJoinConfig()));
        config.getMapConfigs().put(CACHE_MAP_NAME, createMapConfig(CACHE_MAP_NAME));
        clusterManager.setConfig(config);
        return clusterManager;
    }

    private static JoinConfig createJoinConfig() throws SocketException {
        TcpIpConfig tcpipConfig = new TcpIpConfig()
                .setEnabled(true)
                .setConnectionTimeoutSeconds(5);
        clusterHosts.forEach(tcpipConfig::addMember);
        return new JoinConfig()
                .setTcpIpConfig(tcpipConfig)
                .setMulticastConfig(
                        new MulticastConfig()
                                .setEnabled(false));
    }

    private static void initializeCache() {
        HazelcastInstance hazelcastInstance = getHazelcastInstance();
        // Write data in cache from node-1. Any new node added in cluster won't add any data in cache.
        if (hazelcastInstance.getCluster().getMembers().size() == 1) {
            Person person1 = new Person().setName("alien").setAge(30);
            Person person2 = new Person().setName("mars").setAge(38);
            hazelcastInstance.getMap(CACHE_MAP_NAME).set(person1.getName(), person1);
            hazelcastInstance.getMap(CACHE_MAP_NAME).set(person2.getName(), person2);
        }
    }

    private static String readFromCache(final String cacheKey) {
        try {
            HazelcastInstance hazelcastInstance = getHazelcastInstance();
            return new ObjectMapper().writeValueAsString(hazelcastInstance.getMap(CACHE_MAP_NAME).get(cacheKey));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error reading cache for key " + cacheKey);
        }
    }

    private static HazelcastInstance getHazelcastInstance() {
        // You can register with Spring instead of doing it every time.
        return Hazelcast.getAllHazelcastInstances().iterator().next();
    }

    private static MapConfig createMapConfig(final String cacheMapName) {
        return new MapConfig()
                .setName(cacheMapName)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setBackupCount(0)
                .setAsyncBackupCount(1)
                .setReadBackupData(false)
                .setEvictionPolicy(EvictionPolicy.NONE)
                .setTimeToLiveSeconds(0)
                .setMaxIdleSeconds(0)
                .setStatisticsEnabled(false)
                .setMaxSizeConfig(new MaxSizeConfig(0, MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE));
    }

    private static void registerVertxEventBusHandler(final Vertx vertx) {
        vertx.eventBus().consumer("someHandler", message -> {
            System.out.println("Hello from :: " + message.body());
        });
    }
}