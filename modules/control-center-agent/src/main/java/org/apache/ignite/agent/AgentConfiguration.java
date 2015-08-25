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
    private String nodeUri;

    /** */
    @Parameter(names = {"-c", "--config"}, description = "Path to configuration file")
    private String cfgPath;

    /** */
    @Parameter(names = {"-drv", "--driver-folder"},
        description = "Path to folder with JDBC drivers, for example: /home/user/jdbc-drivers")
    private String driversFolder;

    /** */
    @Parameter(names = { "-tm", "--test-drive-metadata" },
        description = "Start H2 database with sample tables in same process. " +
            "JDBC URL for connect to sample database: jdbc:h2:mem:test-drive-db")
    private Boolean meta;

    /** */
    @Parameter(names = { "-ts", "--test-drive-sql" },
        description = "Create cache and populate it with sample data for use in query.")
    private Boolean sql;

    /** */
    @Parameter(names = { "-h", "--help" }, help = true, description = "Print this help message")
    private Boolean help;

    /**
     * @return Login.
     */
    public String login() {
        return login;
    }

    /**
     * @param login Login.
     */
    public void login(String login) {
        this.login = login;
    }

    /**
     * @return Password.
     */
    public String password() {
        return pwd;
    }

    /**
     * @param pwd Password.
     */
    public void password(String pwd) {
        this.pwd = pwd;
    }

    /**
     * @return Server URI.
     */
    public String serverUri() {
        return srvUri;
    }

    /**
     * @param srvUri URI.
     */
    public void serverUri(String srvUri) {
        this.srvUri = srvUri;
    }

    /**
     * @return Node URI.
     */
    public String nodeUri() {
        return nodeUri;
    }

    /**
     * @param nodeUri Node URI.
     */
    public void nodeUri(String nodeUri) {
        this.nodeUri = nodeUri;
    }

    /**
     * @return Configuration path.
     */
    public String configPath() {
        return cfgPath;
    }

    /**
     * @return Configured drivers folder.
     */
    public String driversFolder() {
        return driversFolder;
    }

    /**
     * @param driversFolder Driver folder.
     */
    public void driversFolder(String driversFolder) {
        this.driversFolder = driversFolder;
    }

    /**
     * @return {@code true} If metadata test drive should be started.
     */
    public Boolean testDriveMetadata() {
        return meta != null ? meta : false;
    }

    /**
     * @param meta Set to {@code true} if metadata test drive should be started.
     */
    public void testDriveMetadata(Boolean meta) {
        this.meta = meta;
    }

    /**
     * @return {@code true} If SQL test drive should be started.
     */
    public Boolean testDriveSql() {
        return sql != null ? sql : false;
    }

    /**
     * @param sql Set to {@code true} if SQL test drive should be started.
     */
    public void testDriveSql(Boolean sql) {
        this.sql = sql;
    }

    /**
     * @return {@code true} If agent options usage should be printed.
     */
    public Boolean help() {
        return help != null ? help : false;
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
            login(val);

        val = (String)props.remove("password");

        if (val != null)
            password(val);

        val = (String)props.remove("serverURI");

        if (val != null)
            serverUri(val);

        val = (String)props.remove("nodeURI");

        if (val != null)
            nodeUri(val);

        val = (String)props.remove("driverFolder");

        if (val != null)
            driversFolder(val);
    }

    /**
     * @param cmd Command.
     */
    public void merge(AgentConfiguration cmd) {
        if (cmd.login() != null)
            login(cmd.login());

        if (cmd.password() != null)
            password(cmd.password());

        if (cmd.serverUri() != null)
            serverUri(cmd.serverUri());

        if (srvUri == null)
            serverUri(DFLT_SERVER_URI);

        if (cmd.nodeUri() != null)
            nodeUri(cmd.nodeUri());

        if (nodeUri == null)
            nodeUri(DFLT_NODE_URI);

        if (cmd.driversFolder() != null)
            driversFolder(cmd.driversFolder());

        if (cmd.testDriveMetadata())
            testDriveMetadata(true);

        if (cmd.testDriveSql())
            testDriveSql(true);
    }
}
