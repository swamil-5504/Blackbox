
package org.blackbox.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

public class ClusterConfig {

    private final String clusterName;
    private final Map<String, BrokerAddress> brokers;

    private ClusterConfig(
            String clusterName,
            Map<String, BrokerAddress> brokers
    ) {
        this.clusterName = clusterName;
        this.brokers = Map.copyOf(brokers);
    }

    public static ClusterConfig load(String resourcePath)
            throws IOException {

        Properties properties = new Properties();

        try (InputStream input =
                     ClusterConfig.class.getClassLoader()
                             .getResourceAsStream(resourcePath)) {

            if (input == null) {
                throw new IOException(
                        "Config file not found: " + resourcePath
                );
            }

            properties.load(input);
        }

        String clusterName = properties.getProperty("cluster.name");

        if (clusterName == null || clusterName.isBlank()) {
            throw new IllegalArgumentException(
                    "Missing cluster.name"
            );
        }

        Map<String, BrokerAddress> brokers = new TreeMap<>();

        for (String key : properties.stringPropertyNames()) {

            if (!key.endsWith(".host")) {
                continue;
            }

            String brokerId = key.substring(
                    0, key.length() - ".host".length()
            );

            String host = properties.getProperty(key);

            String portValue = properties.getProperty(
                    brokerId + ".port"
            );

            String internalPortValue = properties.getProperty(
                    brokerId + ".internal-port"
            );

            if (host == null || host.isBlank()
                    || portValue == null
                    || internalPortValue == null) {
                throw new IllegalArgumentException(
                        "Invalid configuration for " + brokerId
                );
            }

            int port;
            int internalPort;

            try {
                port = Integer.parseInt(portValue);
                internalPort = Integer.parseInt(internalPortValue);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid port configuration for " + brokerId,
                        e
                );
            }

            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException(
                        "Port out of range for " + brokerId
                );
            }

            if (internalPort < 1 || internalPort > 65535) {
                throw new IllegalArgumentException(
                        "Internal port out of range for " + brokerId
                );
            }

            if (port == internalPort) {
                throw new IllegalArgumentException(
                        "Client and internal ports must be different for "
                                + brokerId
                );
            }

            brokers.put(
                    brokerId,
                    new BrokerAddress(host, port, internalPort)
            );
        }

        if (brokers.isEmpty()) {
            throw new IllegalArgumentException(
                    "No brokers found in cluster configuration"
            );
        }

        return new ClusterConfig(clusterName, brokers);
    }

    public String getClusterName() {
        return clusterName;
    }

    public Map<String, BrokerAddress> getBrokers() {
        return brokers;
    }

    public BrokerAddress getBroker(String brokerId) {
        BrokerAddress address = brokers.get(brokerId);

        if (address == null) {
            throw new IllegalArgumentException(
                    "Unknown broker: " + brokerId
            );
        }

        return address;
    }

    public record BrokerAddress(
            String host,
            int port,
            int internalPort
    ) {
    }
}
