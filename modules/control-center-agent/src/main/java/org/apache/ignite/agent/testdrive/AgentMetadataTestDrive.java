package org.apache.ignite.agent.testdrive;

import org.h2.tools.*;

import java.sql.*;
import java.util.logging.*;

/**
 * Test drive for metadata load from database.
 *
 * H2 database will be started and several tables will be created.
 */
public class AgentMetadataTestDrive {
    /** */
    private static final Logger log = Logger.getLogger(AgentMetadataTestDrive.class.getName());

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
        log.log(Level.INFO, "Starting in-memory H2 database for metadata test drive...");

        try {
            Connection conn = DriverManager.getConnection("jdbc:h2:mem:test-driver-db;DB_CLOSE_DELAY=-1", "sa", "");

            query(conn, "CREATE TABLE COUNTRY(ID INTEGER NOT NULL PRIMARY KEY, COUNTRY_NAME VARCHAR(100))");
            log.log(Level.INFO, "Table COUNTRY created.");

            query(conn, "CREATE TABLE DEPARTMENT(" +
                " DEPARTMENT_ID INTEGER  NOT NULL PRIMARY KEY," +
                " DEPARTMENT_NAME VARCHAR(50) NOT NULL," +
                " COUNTRY_ID INTEGER," +
                " MANAGER_ID INTEGER)");
            log.log(Level.INFO, "Table DEPARTMENT created.");

            query(conn, "CREATE TABLE EMPLOYEE(" +
                " EMPLOYEE_ID INTEGER NOT NULL PRIMARY KEY," +
                " FIRST_NAME VARCHAR(20) NOT NULL," +
                " LAST_NAME VARCHAR(30) NOT NULL," +
                " EMAIL VARCHAR(25) NOT NULL," +
                " PHONE_NUMBER VARCHAR(20)," +
                " HIRE_DATE DATE NOT NULL," +
                " JOB VARCHAR(50) NOT NULL," +
                " SALARY DOUBLE," +
                " MANAGER_ID INTEGER," +
                " DEPARTMENT_ID INTEGER)");
            log.log(Level.INFO, "Table EMPLOYEE created.");

            query(conn, "CREATE INDEX EMP_SALARY_A ON EMPLOYEE(SALARY ASC)");
            query(conn, "CREATE INDEX EMP_SALARY_B ON EMPLOYEE(SALARY DESC)");
            query(conn, "CREATE INDEX EMP_NAMES ON EMPLOYEE(FIRST_NAME ASC, LAST_NAME  ASC)");
            log.log(Level.INFO, "Indexes for table EMPLOYEE created.");

            conn.close();

            Server.createTcpServer("-tcpDaemon").start();

            log.log(Level.INFO, "TcpServer stared.");
        } catch (SQLException e) {
            log.log(Level.SEVERE, "Failed to start test drive for metadata!", e);
        }
    }
}
