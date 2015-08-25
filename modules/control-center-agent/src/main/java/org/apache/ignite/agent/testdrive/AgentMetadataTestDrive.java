package org.apache.ignite.agent.testdrive;

import org.apache.ignite.agent.*;
import org.h2.tools.*;

import java.io.*;
import java.sql.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * Test drive for metadata load from database.
 *
 * H2 database will be started and several tables will be created.
 */
public class AgentMetadataTestDrive {
    /** */
    private static final Logger log = Logger.getLogger(AgentMetadataTestDrive.class.getName());

    /** */
    private static final AtomicBoolean initLatch = new AtomicBoolean();

    /**
     * Execute query.
     *
     * @param conn Connection to database.
     * @param qry Statement to execute.
     */
    private static void query(Connection conn, String qry) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(qry)) {
            ps.executeUpdate();
        }
    }

    /**
     * Start H2 database and populate it with several tables.
     */
    public static void testDrive() {
        if (initLatch.compareAndSet(false, true)) {
            log.log(Level.INFO, "TEST-DRIVE: Prepare in-memory H2 database...");

            try {
                Connection conn = DriverManager.getConnection("jdbc:h2:mem:test-drive-db;DB_CLOSE_DELAY=-1", "sa", "");

                File agentHome = AgentUtils.getAgentHome();

                File sqlScript = new File((agentHome != null) ? new File(agentHome, "test-drive") : new File("test-drive"),
                    "test-drive.sql");

                RunScript.execute(conn, new FileReader(sqlScript));
                log.log(Level.INFO, "TEST-DRIVE: Sample tables created.");

                conn.close();

                Server.createTcpServer("-tcpDaemon").start();

                log.log(Level.INFO, "TEST-DRIVE: TcpServer stared.");

                log.log(Level.INFO, "TEST-DRIVE: JDBC URL for test drive metadata load: jdbc:h2:mem:test-drive-db");
            }
            catch (Exception e) {
                log.log(Level.SEVERE, "TEST-DRIVE: Failed to start test drive for metadata!", e);
            }
        }
    }
}
