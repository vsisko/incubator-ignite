package org.apache.ignite.agent.testdrive;

import org.apache.ignite.*;
import org.apache.ignite.agent.testdrive.model.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * Test drive for SQL.
 *
 * Cache will be created and populated with data to query.
 */
public class AgentSqlTestDrive {
    /** */
    private static final Logger log = Logger.getLogger(AgentMetadataTestDrive.class.getName());

    /** */
    private static final AtomicBoolean initLatch = new AtomicBoolean();

    private static final String CACHE_NAME = "test-drive-sql";

    private static final Random rnd = new Random();

    /** Countries count. */
    private static final int CNTR_CNT = 10;

    /** Departments count */
    private static final int DEP_CNT = 100;

    /** Employees count. */
    private static final int EMPL_CNT = 1000;

    /**
     * Configure cache.
     *
     * @param name Cache name.
     */
    private static <K, V> CacheConfiguration<K, V> cache(String name) {
        CacheConfiguration<K, V> ccfg = new CacheConfiguration<>(name);

        // Configure cache types.
        Collection<CacheTypeMetadata> meta = new ArrayList<>();

        // CAR.
        CacheTypeMetadata type = new CacheTypeMetadata();

        meta.add(type);

        type.setKeyType(CarKey.class.getName());
        type.setValueType(Car.class.getName());

        // Query fields for CAR.
        Map<String, Class<?>> qryFlds = new LinkedHashMap<>();

        qryFlds.put("carId", int.class);
        qryFlds.put("parkingId", int.class);
        qryFlds.put("carName", String.class);

        type.setQueryFields(qryFlds);

        // Ascending fields for CAR.
        Map<String, Class<?>> ascFlds = new LinkedHashMap<>();

        ascFlds.put("carId", int.class);

        type.setAscendingFields(ascFlds);

        ccfg.setTypeMetadata(meta);

        // PARKING.
        type = new CacheTypeMetadata();

        meta.add(type);

        type.setKeyType(ParkingKey.class.getName());
        type.setValueType(Parking.class.getName());

        // Query fields for PARKING.
        qryFlds = new LinkedHashMap<>();

        qryFlds.put("parkingId", int.class);
        qryFlds.put("parkingName", String.class);

        type.setQueryFields(qryFlds);

        // Ascending fields for PARKING.
        ascFlds = new LinkedHashMap<>();

        ascFlds.put("parkingId", int.class);

        type.setAscendingFields(ascFlds);

        ccfg.setTypeMetadata(meta);

        // COUNTRY.
        type = new CacheTypeMetadata();

        meta.add(type);

        type.setKeyType(CountryKey.class.getName());
        type.setValueType(Country.class.getName());

        // Query fields for COUNTRY.
        qryFlds = new LinkedHashMap<>();

        qryFlds.put("id", int.class);
        qryFlds.put("countryName", String.class);

        type.setQueryFields(qryFlds);

        // Ascending fields for COUNTRY.
        ascFlds = new LinkedHashMap<>();

        ascFlds.put("id", int.class);

        type.setAscendingFields(ascFlds);

        ccfg.setTypeMetadata(meta);

        // DEPARTMENT.
        type = new CacheTypeMetadata();

        meta.add(type);

        type.setKeyType(DepartmentKey.class.getName());
        type.setValueType(Department.class.getName());

        // Query fields for DEPARTMENT.
        qryFlds = new LinkedHashMap<>();

        qryFlds.put("departmentId", int.class);
        qryFlds.put("departmentName", String.class);
        qryFlds.put("countryId", Integer.class);
        qryFlds.put("managerId", Integer.class);

        type.setQueryFields(qryFlds);

        // Ascending fields for DEPARTMENT.
        ascFlds = new LinkedHashMap<>();

        ascFlds.put("departmentId", int.class);

        type.setAscendingFields(ascFlds);

        ccfg.setTypeMetadata(meta);

        // EMPLOYEE.
        type = new CacheTypeMetadata();

        meta.add(type);

        type.setKeyType(EmployeeKey.class.getName());
        type.setValueType(Employee.class.getName());

        // Query fields for EMPLOYEE.
        qryFlds = new LinkedHashMap<>();

        qryFlds.put("employeeId", int.class);
        qryFlds.put("firstName", String.class);
        qryFlds.put("lastName", String.class);
        qryFlds.put("email", String.class);
        qryFlds.put("phoneNumber", String.class);
        qryFlds.put("hireDate", java.sql.Date.class);
        qryFlds.put("job", String.class);
        qryFlds.put("salary", Double.class);
        qryFlds.put("managerId", Integer.class);
        qryFlds.put("departmentId", Integer.class);

        type.setQueryFields(qryFlds);

        // Ascending fields for EMPLOYEE.
        ascFlds = new LinkedHashMap<>();

        ascFlds.put("employeeId", int.class);
        ascFlds.put("salary", Double.class);

        type.setAscendingFields(ascFlds);

        // Groups for EMPLOYEE.
        Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> grps = new LinkedHashMap<>();

        LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> grpItems = new LinkedHashMap<>();

        grpItems.put("firstName", new IgniteBiTuple<Class<?>, Boolean>(String.class, false));
        grpItems.put("lastName", new IgniteBiTuple<Class<?>, Boolean>(String.class, false));

        grps.put("EMP_NAMES", grpItems);

        type.setGroups(grps);

        ccfg.setTypeMetadata(meta);

        return ccfg;
    }

    public static double round(double value, int places) {
        if (places < 0)
            throw new IllegalArgumentException();

        long factor = (long) Math.pow(10, places);

        value *= factor;

        long tmp = Math.round(value);

        return (double) tmp / factor;
    }

    /**
     * @param ignite Ignite.
     * @param name Cache name.
     */
    private static void populateCache(Ignite ignite, String name) {
        log.log(Level.INFO, "TEST-DRIVE: Start population '" + name + "' cache with data...");

        IgniteCache<CountryKey, Country> cacheCountry = ignite.cache(name);

        for (int i = 0; i < CNTR_CNT; i++)
            cacheCountry.put(new CountryKey(i), new Country(i, "State " + (i + 1)));

        IgniteCache<DepartmentKey, Department> cacheDepartment = ignite.cache(name);

        for (int i = 0; i < DEP_CNT; i++) {
            Integer managerId = (i == 0 || rnd.nextBoolean()) ? null : rnd.nextInt(i);

            cacheDepartment.put(new DepartmentKey(i),
                new Department(i, "Department " + (i + 1), rnd.nextInt(CNTR_CNT), managerId));
        }

        IgniteCache<EmployeeKey, Employee> cacheEmployee = ignite.cache(name);

        long offset = java.sql.Date.valueOf("2007-01-01").getTime();

        long end = java.sql.Date.valueOf("2016-01-01").getTime();

        long diff = end - offset + 1;

        for (int i = 0; i < EMPL_CNT; i++) {
            Integer managerId = (i == 0 || rnd.nextBoolean()) ? null : rnd.nextInt(i);

            double r = rnd.nextDouble();

            cacheEmployee.put(new EmployeeKey(i),
                new Employee(i, "first name " + (i + 1), "last name " + (i + 1), "email " + (i + 1),
                    "phone number " + (i + 1), new java.sql.Date(offset + (long)(r * diff)), "job " + (i + 1),
                    round(r * 5000, 2) , managerId, rnd.nextInt(DEP_CNT)));
        }

        log.log(Level.INFO, "TEST-DRIVE: Finished population '" + name + "' cache with data.");
    }

    /**
     * Start ignite node with cache and populate it with data.
     */
    public static void testDrive() {
        if (initLatch.compareAndSet(false, true)) {
            log.log(Level.INFO, "TEST-DRIVE: Prepare node configuration...");

            try {
                IgniteConfiguration cfg = new IgniteConfiguration();

                cfg.setMetricsLogFrequency(0);

                cfg.setCacheConfiguration(cache(CACHE_NAME));

                log.log(Level.INFO, "TEST-DRIVE: Start embedded node with indexed enabled cache...");

                Ignite ignite = Ignition.start(cfg);

                log.log(Level.INFO, "TEST-DRIVE: Embedded node started");

                populateCache(ignite, CACHE_NAME);
            }
            catch (Exception e) {
                log.log(Level.SEVERE, "TEST-DRIVE: Failed to start test drive for sql!", e);
            }
        }
    }
}
