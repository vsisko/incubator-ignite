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

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Agent configuration.
 */
public class AgentConfiguration {
    /** Default server URI. */
    private static final String DFLT_SERVER_URI = "wss://localhost:3001";

    /** Default Ignite node HTTP URI. */
    private static final String DFLT_NODE_URI = "http://localhost:8080";

    /** */
    @Parameter(names = {"-l", "--login"}, description = "User's login (email) on Ignite Web Control Center")
    private String login;

    /** */
    @Parameter(names = {"-p", "--password"}, description = "User's password")
    private String pwd;

    /** */
    @Parameter(names = {"-s", "--server-uri"}, description = "URI for connect to Ignite Web Control Center via " +
        "web-socket protocol, for example: wss://control-center.my-company.com:3001")
    private String srvUri;

    /** */
    @Parameter(names = {"-n", "--node-uri"},
        description = "URI for connect to Ignite REST server, for example: http://localhost:8080")
    private String nodeUri = "http://localhost:8080";

    /** */
    @Parameter(names = {"-c", "--config"}, description = "Path to configuration file")
    private String cfgPath;

    /** */
    @Parameter(names = {"-drv", "--driver-folder"},
        description = "Path to folder with JDBC drivers, for example /home/user/jdbc-drivers")
    private String driversFolder;

    /** */
    @Parameter(names = { "-tm", "--test-metadata" },
        description = "Start H2 database with sample tables in same process.")
    private boolean meta;

    /** */
    @Parameter(names = { "-ts", "--test-sql" },
        description = "Create cache and populate it with sample data for use in query.")
    private boolean sql;

    /** */
    @Parameter(names = { "-h", "--help" }, description = "Print this help message")
    private boolean help;

    /**
     * @return Login.
     */
    public String getLogin() {
        return login;
    }

    /**
     * @param login Login.
     */
    public void setLogin(String login) {
        this.login = login;
    }

    /**
     * @return Password.
     */
    public String getPassword() {
        return pwd;
    }

    /**
     * @param pwd Password.
     */
    public void setPassword(String pwd) {
        this.pwd = pwd;
    }

    /**
     * @return Server URI.
     */
    public String getServerUri() {
        return srvUri;
    }

    /**
     * @param srvUri URI.
     */
    public void setServerUri(String srvUri) {
        this.srvUri = srvUri;
    }

    /**
     * @return Node URI.
     */
    public String getNodeUri() {
        return nodeUri;
    }

    /**
     * @param nodeUri Node URI.
     */
    public void setNodeUri(String nodeUri) {
        this.nodeUri = nodeUri;
    }

    /**
     * @return Configuration path.
     */
    public String getConfigPath() {
        return cfgPath;
    }

    /**
     * @param cfgPath Config path.
     */
    public void setConfigPath(String cfgPath) {
        this.cfgPath = cfgPath;
    }

    /**
     * @return Configured drivers folder.
     */
    public String getDriversFolder() {
        return driversFolder;
    }

    /**
     * @param driversFolder Driver folder.
     */
    public void setDriversFolder(String driversFolder) {
        this.driversFolder = driversFolder;
    }

    /**
     * @param cfgUrl URL.
     */
    public void load(URL cfgUrl) throws IOException {
        Properties props = new Properties();

        try (Reader reader = new InputStreamReader(cfgUrl.openStream())) {
            props.load(reader);
        }

        String val = (String)props.remove("login");

        if (val != null)
            setLogin(val);

        val = (String)props.remove("password");

        if (val != null)
            setPassword(val);

        val = (String)props.remove("serverURI");

        if (val != null)
            setServerUri(val);

        val = (String)props.remove("nodeURI");

        if (val != null)
            setNodeUri(val);

        val = (String)props.remove("driverFolder");

        if (val != null)
            setDriversFolder(val);
    }

    /**
     * @param cmd Command.
     */
    public void merge(AgentConfiguration cmd) {
        if (cmd.getLogin() != null)
            setLogin(cmd.getLogin());

        if (cmd.getPassword() != null)
            setPassword(cmd.getPassword());

        if (cmd.getServerUri() != null)
            setServerUri(cmd.getServerUri());

        if (srvUri == null)
            setServerUri(DFLT_SERVER_URI);

        if (cmd.getNodeUri() != null)
            setNodeUri(cmd.getNodeUri());

        if (nodeUri == null)
            setNodeUri(DFLT_NODE_URI);

        if (cmd.getDriversFolder() != null)
            setDriversFolder(cmd.getDriversFolder());
    }

    /**
     * @return {@code true} If agent options usage should be printed.
     */
    public boolean help() {
        return help;
    }
}
