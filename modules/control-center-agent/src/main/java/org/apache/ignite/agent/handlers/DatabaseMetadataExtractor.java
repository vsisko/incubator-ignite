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

/**
 * Remote API to extract database metadata.
 */
public class DatabaseMetadataExtractor {
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
     *
     * @return Collection of tables.
     */
    @Remote
    public Collection<DbTable> extractMetadata(String jdbcDriverJarPath, String jdbcDriverCls, String jdbcUrl,
        Properties jdbcInfo, boolean tblsOnly) throws SQLException {
        if (!new File(jdbcDriverJarPath).isAbsolute() && driversFolder != null)
            jdbcDriverJarPath = new File(driversFolder, jdbcDriverJarPath).getPath();

        Connection conn = DbMetadataReader.getInstance().connect(jdbcDriverJarPath, jdbcDriverCls, jdbcUrl, jdbcInfo);

        return DbMetadataReader.getInstance().extractMetadata(conn, tblsOnly);
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
     * @return Drivers in drivers folder
     * @see AgentConfiguration#driversFolder
     */
    @Remote
    public List<JdbcDriver> availableDrivers() {
        if (driversFolder == null)
            return Collections.emptyList();

        String[] list = new File(driversFolder).list();

        if (list == null)
            return Collections.emptyList();

        List<JdbcDriver> res = new ArrayList<>();

        for (String fileName : list) {
            if (fileName.endsWith(".jar")) {
                try {
                    String spec = "jar:file:/" + driversFolder + '/' + fileName + "!/META-INF/services/java.sql.Driver";

                    URL url = new URL(spec.replace('\\', '/'));

                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
                        res.add(new JdbcDriver(reader.readLine(), fileName));
                    }
                } catch (IOException ignored) {
                    res.add(new JdbcDriver(null, fileName));
                }
            }
        }

        return res;
    }
}
