
package org.blackbox.cluster;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class InterBrokerServer {

    private final String brokerId;
    private final String clusterName;
    private final int port;

    public InterBrokerServer(
            String brokerId,
            String clusterName,
            int port
    ) {
        this.brokerId = brokerId;
        this.clusterName = clusterName;
        this.port = port;
    }

    public void start() throws Exception {

        try (ServerSocket serverSocket = new ServerSocket(port)) {

            System.out.println(
                    "Inter-broker server listening on port " + port
            );

            while (true) {

                Socket socket = serverSocket.accept();

                Thread clientThread = new Thread(
                        () -> handleConnection(socket)
                );

                clientThread.setName(
                        "inter-broker-client-" + socket.getPort()
                );

                clientThread.start();
            }
        }
    }

    private void handleConnection(Socket socket) {

        try (
                socket;
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(
                                socket.getInputStream(),
                                StandardCharsets.UTF_8
                        )
                );
                PrintWriter writer = new PrintWriter(
                        socket.getOutputStream(),
                        true
                )
        ) {

            socket.setKeepAlive(true);
            socket.setTcpNoDelay(true);


            String request = reader.readLine();

            if (request == null) {
                return;
            }

            String[] parts = request.split("\\|", -1);

            if (parts.length != 3
                    || !parts[0].equals("HANDSHAKE")) {

                writer.println("ERROR|INVALID_HANDSHAKE");
                return;
            }

            String remoteBrokerId = parts[1];
            String remoteClusterName = parts[2];


            if (!clusterName.equals(remoteClusterName)) {
                writer.println("ERROR|CLUSTER_MISMATCH");
                return;
            }


            if (brokerId.equals(remoteBrokerId)) {
                writer.println("ERROR|SELF_CONNECTION");
                return;
            }

            System.out.println(
                    "Broker " + remoteBrokerId
                            + " established persistent connection to "
                            + brokerId
            );


            writer.println(
                    "HANDSHAKE_OK|" + brokerId + "|" + clusterName
            );

            // Step 5: Keep processing requests on the same socket.
            String message;

            while ((message = reader.readLine()) != null) {

                System.out.println(
                        "Received from " + remoteBrokerId
                                + ": " + message
                );

                if (message.equals("PING")) {

                    writer.println("PONG|" + brokerId);

                } else {

                    writer.println("ERROR|UNKNOWN_COMMAND");
                }
            }

            System.out.println(
                    "Broker " + remoteBrokerId
                            + " disconnected from " + brokerId
            );

        } catch (Exception e) {

            System.err.println(
                    "Inter-broker connection error: "
                            + e.getMessage()
            );
        }
    }
}
