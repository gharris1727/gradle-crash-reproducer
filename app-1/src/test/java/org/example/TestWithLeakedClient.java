/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class TestWithLeakedClient {

    public static String LOCALHOST = "localhost";
    public static int MAX_WAIT_MS = 60_000;
    public static int PORT_SKIP_LIMIT = 100;
    public static int SOCKET_TIMEOUT_MS = 50;

    @Test
    public void test() {
        connectToUsedSockets();
    }

    // Detect some other processes ports by repeatedly binding our own socket and watching for skipped ports
    // Only works when the OS allocates consecutive ports when binding port 0.
    // Faster than calling lsof
    public static Set<Integer> findUsedPorts(long deadline) {
        int expectedNextPort = 0;
        Set<Integer> usedPorts = new HashSet<>();
        int largePortSkips = 0;
        while (System.currentTimeMillis() < deadline) {
            try (Socket socket = new Socket()) {
                socket.bind(new InetSocketAddress(0));
                int nextPort = socket.getLocalPort();
                if (expectedNextPort != 0 && expectedNextPort != 65536 && nextPort > expectedNextPort) {
                    if (nextPort - expectedNextPort < PORT_SKIP_LIMIT) {
                        for (int i = expectedNextPort; i <= nextPort; i++) {
                            // As soon as we see a repeated port, that means we checked the whole space
                            if (usedPorts.contains(i)) {
                                return usedPorts;
                            }
                            usedPorts.add(i);
                        }
                    } else {
                        System.out.println("Skipped from " + expectedNextPort + " to " + nextPort);
                        if (largePortSkips++ > 10) {
                            throw new RuntimeException("Too many ports skipped, probably using nonconsecutive ports");
                        }
                    }
                }
                expectedNextPort = nextPort + 1;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        throw new RuntimeException("Unable to find port skip before timing out");
    }

    public static void connectToUsedSockets() {
        long deadline = System.currentTimeMillis() + MAX_WAIT_MS;
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            while (System.currentTimeMillis() < deadline) {
                Set<Integer> usedPorts = findUsedPorts(deadline);
                System.out.println(usedPorts);
                List<? extends Future<?>> connections = usedPorts.stream()
                    .map(TestWithLeakedClient::connectToPort)
                    .map(executorService::submit)
                    .collect(Collectors.toList());
                for (Future<?> connection : connections) {
                    connection.get();
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            executorService.shutdown();
        }
    }

    private static Runnable connectToPort(int usedPort) {
        return () -> {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(LOCALHOST, usedPort), SOCKET_TIMEOUT_MS);
            } catch (IOException e) {
                if (!(e instanceof SocketTimeoutException) && !(e instanceof ConnectException)) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
