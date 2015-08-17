/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.atomic.*;

/**
 *
 */
public class GridCacheMassiveRebalancingSyncSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    private static int TEST_SIZE = 1_024_000;

    /** cache name. */
    protected static String CACHE_NAME_DHT = "cache";

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>();

        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setForceServerMode(true);

        if (getTestGridName(10).equals(gridName))
            iCfg.setClientMode(true);

        cacheCfg.setName(CACHE_NAME_DHT);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        //cacheCfg.setRebalanceBatchSize(1024);
        //cacheCfg.setRebalanceBatchesCount(1);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheCfg.setRebalanceThreadPoolSize(4);
        //cacheCfg.setRebalanceTimeout(1000000);
        cacheCfg.setBackups(1);

        iCfg.setCacheConfiguration(cacheCfg);
        return iCfg;
    }

    /**
     * @param ignite Ignite.
     */
    protected void generateData(Ignite ignite) {
        try (IgniteDataStreamer<Integer, Integer> stmr = ignite.dataStreamer(CACHE_NAME_DHT)) {
            for (int i = 0; i < TEST_SIZE; i++) {
                if (i % 1_000_000 == 0)
                    log.info("Prepared " + i / 1_000_000 + "m entries.");

                stmr.addData(i, i);
            }
        }
    }

    /**
     * @param ignite Ignite.
     * @throws IgniteCheckedException
     */
    protected void checkData(Ignite ignite) throws IgniteCheckedException {
        for (int i = 0; i < TEST_SIZE; i++) {
            if (i % 1_000_000 == 0)
                log.info("Checked " + i / 1_000_000 + "m entries.");

            assert ignite.cache(CACHE_NAME_DHT).get(i).equals(i) : "keys " + i + " does not match";
        }
    }

    /**
     * @throws Exception
     */
    public void testSimpleRebalancing() throws Exception {
        Ignite ignite = startGrid(0);

        generateData(ignite);

        log.info("Preloading started.");

        long start = System.currentTimeMillis();

        startGrid(1);

        IgniteInternalFuture f1 = ((GridCacheAdapter)grid(1).context().cache().internalCache(CACHE_NAME_DHT)).preloader().syncFuture();

        f1.get();

        long spend = (System.currentTimeMillis() - start) / 1000;

        stopGrid(0);

        checkData(grid(1));

        log.info("Spend " + spend + " seconds to preload entries.");

        stopAllGrids();
    }

    /**
     * @throws Exception
     */
    public void testComplexRebalancing() throws Exception {
        Ignite ignite = startGrid(0);

        generateData(ignite);

        log.info("Preloading started.");

        long start = System.currentTimeMillis();

        //will be started simultaneously in case of ASYNC mode
        startGrid(1);
        startGrid(2);
        startGrid(3);
        startGrid(4);

        GridCachePreloader p1 = ((GridCacheAdapter)grid(1).context().cache().internalCache(CACHE_NAME_DHT)).preloader();
        GridCachePreloader p2 = ((GridCacheAdapter)grid(2).context().cache().internalCache(CACHE_NAME_DHT)).preloader();
        GridCachePreloader p3 = ((GridCacheAdapter)grid(3).context().cache().internalCache(CACHE_NAME_DHT)).preloader();
        GridCachePreloader p4 = ((GridCacheAdapter)grid(4).context().cache().internalCache(CACHE_NAME_DHT)).preloader();

        IgniteInternalFuture f4 = p4.syncFuture();
        f4.get();

        AffinityTopologyVersion f4Top = ((GridDhtPartitionDemander.SyncFuture)f4).topologyVersion();

        IgniteInternalFuture f1 = p1.syncFuture();
        IgniteInternalFuture f2 = p2.syncFuture();
        IgniteInternalFuture f3 = p3.syncFuture();

        while (!((GridDhtPartitionDemander.SyncFuture)f1).topologyVersion().equals(f4Top) ||
            !((GridDhtPartitionDemander.SyncFuture)f2).topologyVersion().equals(f4Top) ||
            !((GridDhtPartitionDemander.SyncFuture)f3).topologyVersion().equals(f4Top)) {
            U.sleep(100);

            f1 = p1.syncFuture();
            f2 = p2.syncFuture();
            f3 = p3.syncFuture();
        }
        f1.get();
        f2.get();
        f3.get();

        long spend = (System.currentTimeMillis() - start) / 1000;

        f1 = p1.syncFuture();
        f2 = p2.syncFuture();
        f3 = p3.syncFuture();
        f4 = p4.syncFuture();

        stopGrid(0);

        while (f1 == p1.syncFuture() || f2 == p2.syncFuture() || f3 == p3.syncFuture() || f4 == p4.syncFuture())
            U.sleep(100);

        p1.syncFuture().get();
        p2.syncFuture().get();
        p3.syncFuture().get();
        p4.syncFuture().get();

        f2 = p2.syncFuture();
        f3 = p3.syncFuture();
        f4 = p4.syncFuture();

        stopGrid(1);

        while (f2 == p2.syncFuture() || f3 == p3.syncFuture() || f4 == p4.syncFuture())
            U.sleep(100);

        p2.syncFuture().get();
        p3.syncFuture().get();
        p4.syncFuture().get();

        f3 = p3.syncFuture();
        f4 = p4.syncFuture();

        stopGrid(2);

        while (f3 == p3.syncFuture() || f4 == p4.syncFuture())
            U.sleep(100);

        p3.syncFuture().get();
        p4.syncFuture().get();

        stopGrid(3);

        checkData(grid(4));

        log.info("Spend " + spend + " seconds to preload entries.");

        stopAllGrids();
    }

    /**
     * @throws Exception
     */
    public void _testOpPerSecRebalancingTest() throws Exception {
        startGrid(0);

        final AtomicBoolean cancelled = new AtomicBoolean(false);

        generateData(grid(0));

        startGrid(1);
        startGrid(2);
        startGrid(10);

        Thread t = new Thread(new Runnable() {
            @Override public void run() {

                long spend = 0;

                long ops = 0;

                while (!cancelled.get()) {
                    try {
                        long start = System.currentTimeMillis();

                        int size = 1000;

                        for (int i = 0; i < size; i++)
                            grid(10).cachex(CACHE_NAME_DHT).remove(i);

                        for (int i = 0; i < size; i++)
                            grid(10).cachex(CACHE_NAME_DHT).put(i, i);

                        spend += System.currentTimeMillis() - start;

                        ops += size * 2;
                    }
                    catch (IgniteCheckedException e) {
                        e.printStackTrace();
                    }

                    log.info("Ops. per ms: " + ops / spend);
                }
            }
        });
        t.start();

        stopGrid(0);
        startGrid(0);

        stopGrid(0);
        startGrid(0);

        stopGrid(0);
        startGrid(0);

        cancelled.set(true);
        t.join();

        checkData(grid(10));

        //stopAllGrids();
    }
}