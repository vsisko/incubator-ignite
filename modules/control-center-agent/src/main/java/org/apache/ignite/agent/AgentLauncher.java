/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent;

import com.beust.jcommander.*;
import org.apache.ignite.agent.handlers.*;
import org.apache.ignite.agent.testdrive.*;
import org.eclipse.jetty.util.ssl.*;
import org.eclipse.jetty.websocket.client.*;

import java.io.*;
import java.net.*;
import java.util.logging.*;

/**
 * Control Center Agent launcher.
 */
public class AgentLauncher {
    /** Static initializer. */
    static {
        AgentLoggingConfigurator.configure();
    }

    /** */
    private static final Logger log = Logger.getLogger(AgentLauncher.class.getName());

    /** */
    private static final int RECONNECT_INTERVAL = 3000;

    /**
     * @param args Args.
     */
    @SuppressWarnings("BusyWait")
    public static void main(String[] args) throws Exception {
        log.log(Level.INFO, "Starting Apache Ignite Control Center Agent...");

        AgentConfiguration cfg = new AgentConfiguration();

        AgentConfiguration cmdCfg = new AgentConfiguration();

        JCommander jCommander = new JCommander(cmdCfg, args);

        if (cmdCfg.help()) {
            jCommander.usage();

            return;
        }

        if (cmdCfg.configPath() != null)
            cfg.load(new File(cmdCfg.configPath()).toURI().toURL());

        cfg.merge(cmdCfg);

        if (cfg.login() == null) {
            System.out.print("Login: ");

            cfg.login(System.console().readLine().trim());
        }

        if (cfg.password() == null) {
            System.out.print("Password: ");

            cfg.password(new String(System.console().readPassword()));
        }

        RestExecutor restExecutor = new RestExecutor(cfg);

        restExecutor.start();

        try {
            SslContextFactory sslCtxFactory = new SslContextFactory();

            // TODO IGNITE-843 Fix issue with trust all: if (Boolean.TRUE.equals(Boolean.getBoolean("trust.all")))
            sslCtxFactory.setTrustAll(true);

            WebSocketClient client = new WebSocketClient(sslCtxFactory);

            client.setMaxIdleTimeout(Long.MAX_VALUE);

            client.start();

            try {
                while (!Thread.interrupted()) {
                    AgentSocket agentSock = new AgentSocket(cfg, restExecutor);

                    log.log(Level.INFO, "Connecting to: " + cfg.serverUri());

                    client.connect(agentSock, URI.create(cfg.serverUri()));

                    agentSock.waitForClose();

                    Thread.sleep(RECONNECT_INTERVAL);
                }
            }
            finally {
                client.stop();
            }
        }
        finally {
            restExecutor.stop();
        }
    }
}
