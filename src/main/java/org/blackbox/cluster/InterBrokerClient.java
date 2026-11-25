
package org.blackbox.cluster;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InterBrokerClient implements AutoCloseable {

    private final String brokerId;
    private final String clusterName;

    private final Map<String, PeerConnection> connections =
            new ConcurrentHashMap<>();

    public InterBrokerClient(
            String brokerId,
            String clusterName
    ) {
        this.brokerId = brokerId;
        this.clusterName = clusterName;
    }

    public void connectWithRetry(
            String remoteBrokerId,
            String host,
            int port,
            int maxAttempts,
            long retryDelayMillis
    ) throws InterruptedException {

        if (connections.containsKey(remoteBrokerId)) {
            return;
        }

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {

            try {

                PeerConnection connection =
                        connectToBroker(remoteBrokerId, host, port);

                PeerConnection existing =
                        connections.putIfAbsent(
                                remoteBrokerId,
                                connection
                        );

                if (existing != null) {
                    connection.close();
                }

                System.out.println(
                        "Persistent connection established with "
                                + remoteBrokerId
                                + " at " + host + ":" + port
                );

                return;

            } catch (Exception e) {

                if (attempt == maxAttempts) {

                    System.err.println(
                            "Could not connect to " + remoteBrokerId
                                    + " after " + maxAttempts
                                    + " attempts: " + e.getMessage()
                    );

                    return;
                }

                System.out.println(
                        "Connection to " + remoteBrokerId
                                + " failed. Retry " + (attempt + 1)
                                + "/" + maxAttempts
                                + " in " + retryDelayMillis + " ms"
                );

                Thread.sleep(retryDelayMillis);
            }
        }
    }

    private PeerConnection connectToBroker(
            String remoteBrokerId,
            String host,
            int port
    ) throws Exception {

        Socket socket = new Socket();

        try {

            socket.connect(
                    new InetSocketAddress(host, port),
                    3000
            );

            socket.setKeepAlive(true);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(5000);

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(
                            socket.getInputStream(),
                            StandardCharsets.UTF_8
                    )
            );

            PrintWriter writer = new PrintWriter(
                    socket.getOutputStream(),
                    true
            );


            writer.println(
                    "HANDSHAKE|" + brokerId + "|" + clusterName
            );

            if (writer.checkError()) {
                throw new IllegalStateException(
                        "Failed to send handshake"
                );
            }

            // Read handshake response.
            String response = reader.readLine();

            if (response == null) {
                throw new IllegalStateException(
                        "No response from broker"
                );
            }

            String[] parts = response.split("\\|", -1);

            if (parts.length != 3
                    || !parts[0].equals("HANDSHAKE_OK")
                    || !parts[1].equals(remoteBrokerId)
                    || !parts[2].equals(clusterName)) {

                throw new IllegalStateException(
                        "Invalid handshake response: " + response
                );
            }


            return new PeerConnection(
                    remoteBrokerId,
                    socket,
                    reader,
                    writer
            );

        } catch (Exception e) {

            try {
                socket.close();
            } catch (Exception ignored) {
            }

            throw e;
        }
    }

    public void ping(String remoteBrokerId) throws Exception {

        PeerConnection connection =
                connections.get(remoteBrokerId);

        if (connection == null) {
            throw new IllegalStateException(
                    "No connection to " + remoteBrokerId
            );
        }

        connection.ping();
    }

    @Override
    public void close() {

        for (PeerConnection connection : connections.values()) {
            connection.close();
        }

        connections.clear();
    }

    private static class PeerConnection {

        private final String remoteBrokerId;
        private final Socket socket;
        private final BufferedReader reader;
        private final PrintWriter writer;

        private PeerConnection(
                String remoteBrokerId,
                Socket socket,
                BufferedReader reader,
                PrintWriter writer
        ) {
            this.remoteBrokerId = remoteBrokerId;
            this.socket = socket;
            this.reader = reader;
            this.writer = writer;
        }

        public synchronized void ping() throws Exception {

            if (socket.isClosed() || !socket.isConnected()) {
                throw new IllegalStateException(
                        "Connection to " + remoteBrokerId
                                + " is closed"
                );
            }

            writer.println("PING");

            if (writer.checkError()) {
                throw new IllegalStateException(
                        "Failed to send PING to " + remoteBrokerId
                );
            }

            String response = reader.readLine();

            if (!("PONG|" + remoteBrokerId).equals(response)) {
                throw new IllegalStateException(
                        "Invalid PING response from "
                                + remoteBrokerId + ": " + response
                );
            }

            System.out.println(
                    "PING successful: " + remoteBrokerId
            );
        }

        public synchronized void close() {

            try {
                socket.close();
            } catch (Exception e) {
                System.err.println(
                        "Error closing connection to "
                                + remoteBrokerId + ": "
                                + e.getMessage()
                );
            }
        }
    }
}
