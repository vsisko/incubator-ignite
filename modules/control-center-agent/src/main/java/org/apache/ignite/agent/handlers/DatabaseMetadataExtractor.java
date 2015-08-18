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

package org.apache.ignite.agent.handlers;

import org.apache.ignite.agent.*;
import org.apache.ignite.agent.remote.*;
import org.apache.ignite.schema.parser.*;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.logging.*;

/**
 * Remote API to extract database metadata.
 */
public class DatabaseMetadataExtractor {
    /** */
    private static final Logger log = Logger.getLogger(DatabaseMetadataExtractor.class.getName());

    /** */
    private final String driversFolder;

    /**
     * @param cfg Config.
     */
    public DatabaseMetadataExtractor(AgentConfiguration cfg) {
        String driversFolder = cfg.getDriversFolder();

        if (driversFolder == null) {
            File agentHome = AgentUtils.getAgentHome();

            if (agentHome != null)
                driversFolder = agentHome + "/jdbc-drivers";
        }

        this.driversFolder = driversFolder;
    }

    /**
     * @param jdbcDriverJarPath JDBC driver JAR path.
     * @param jdbcDriverCls JDBC driver class.
     * @param jdbcUrl JDBC URL.
     * @param jdbcInfo Properties to connect to database.
     * @return Collection of tables.
     */
    @Remote
    public Collection<DbTable> extractMetadata(String jdbcDriverJarPath, String jdbcDriverCls, String jdbcUrl,
        Properties jdbcInfo, boolean tblsOnly) throws SQLException {
        log.log(Level.INFO, "Collecting database metadata...");

        if (!new File(jdbcDriverJarPath).isAbsolute() && driversFolder != null)
            jdbcDriverJarPath = new File(driversFolder, jdbcDriverJarPath).getPath();

        Connection conn = DbMetadataReader.getInstance().connect(jdbcDriverJarPath, jdbcDriverCls, jdbcUrl, jdbcInfo);

        Collection<DbTable> metadata = DbMetadataReader.getInstance().extractMetadata(conn, tblsOnly);

        log.log(Level.INFO, "Collected: " + metadata.size());

        return metadata;
    }

    /**
     * Wrapper class for later to be transformed to JSON and send to Web Control Center.
     */
    private static class JdbcDriver {
        /** */
        private final String jdbcDriverClass;
        /** */
        private final String jdbcDriverJar;

        /**
         * @param jdbcDriverClass Optional JDBC driver class.
         * @param jdbcDriverJar File name of driver jar file.
         */
        public JdbcDriver(String jdbcDriverClass, String jdbcDriverJar) {
            this.jdbcDriverClass = jdbcDriverClass;
            this.jdbcDriverJar = jdbcDriverJar;
        }
    }

    /**
     * @param path Path to normalize.
     * @return Normalized file path.
     */
    private String normalizePath(String path) {
        return path != null ? path.replace('\\', '/') : null;
    }

    /**
     * @return Drivers in drivers folder
     * @see AgentConfiguration#driversFolder
     */
    @Remote
    public List<JdbcDriver> availableDrivers() {
        String drvFolder = normalizePath(driversFolder);

        log.log(Level.INFO, "Collecting JDBC drivers in folder: " + drvFolder);

        if (drvFolder == null) {
            log.log(Level.INFO, "JDBC drivers folder not specified, returning empty list");

            return Collections.emptyList();
        }

        String[] list = new File(drvFolder).list();

        if (list == null) {
            log.log(Level.INFO, "JDBC drivers folder has no files, returning empty list");

            return Collections.emptyList();
        }

        List<JdbcDriver> res = new ArrayList<>();

        for (String fileName : list) {
            if (fileName.endsWith(".jar")) {
                try {
                    String spec = normalizePath("jar:file:/" + drvFolder + '/' + fileName +
                        "!/META-INF/services/java.sql.Driver");

                    URL url = new URL(spec);

                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
                        String jdbcDriverClass = reader.readLine();

                        res.add(new JdbcDriver(jdbcDriverClass, fileName));

                        log.log(Level.INFO, "Found: [driver=" + fileName + ", class=" + jdbcDriverClass + "]");
                    }
                }
                catch (IOException e) {
                    res.add(new JdbcDriver(null, fileName));

                    log.log(Level.INFO, "Found: [driver=" + fileName + "]");
                    log.log(Level.INFO, "Failed to detect driver class: " + e.getMessage());
                }
            }
        }

        return res;
    }
}
