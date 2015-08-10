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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.worker.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.thread.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.GridTopic.*;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.*;
import static org.apache.ignite.internal.processors.dr.GridDrType.*;

/**
 * Thread pool for requesting partitions from other nodes
 * and populating local cache.
 */
@SuppressWarnings("NonConstantFieldWithUpperCaseName")
public class GridDhtPartitionDemander {
    /** */
    private final GridCacheContext<?, ?> cctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final ReadWriteLock busyLock;

    /** */
    @GridToStringInclude
    private final Collection<DemandWorker> dmdWorkers;

    /** Preload predicate. */
    private IgnitePredicate<GridCacheEntryInfo> preloadPred;

    /** Future for preload mode {@link CacheRebalanceMode#SYNC}. */
    @GridToStringInclude
    private SyncFuture syncFut;

    /** Preload timeout. */
    private final AtomicLong timeout;

    /** Allows demand threads to synchronize their step. */
    private CyclicBarrier barrier;

    /** Demand lock. */
    private final ReadWriteLock demandLock = new ReentrantReadWriteLock();

    /** */
    private int poolSize;

    /** Last timeout object. */
    private AtomicReference<GridTimeoutObject> lastTimeoutObj = new AtomicReference<>();

    /** Last exchange future. */
    private volatile GridDhtPartitionsExchangeFuture lastExchangeFut;

    /**
     * @param cctx Cache context.
     * @param busyLock Shutdown lock.
     */
    public GridDhtPartitionDemander(GridCacheContext<?, ?> cctx, ReadWriteLock busyLock) {
        assert cctx != null;
        assert busyLock != null;

        this.cctx = cctx;
        this.busyLock = busyLock;

        log = cctx.logger(getClass());

        boolean enabled = cctx.rebalanceEnabled() && !cctx.kernalContext().clientNode();

        poolSize = enabled ? cctx.config().getRebalanceThreadPoolSize() : 0;

        if (enabled) {
            barrier = new CyclicBarrier(poolSize);

            dmdWorkers = new ArrayList<>(poolSize);

            for (int i = 0; i < poolSize; i++)
                dmdWorkers.add(new DemandWorker(i));

            syncFut = new SyncFuture(dmdWorkers);
        }
        else {
            dmdWorkers = Collections.emptyList();

            syncFut = new SyncFuture(dmdWorkers);

            // Calling onDone() immediately since preloading is disabled.
            syncFut.onDone();
        }

        timeout = new AtomicLong(cctx.config().getRebalanceTimeout());
    }

    /**
     *
     */
    void start() {
        if (poolSize > 0) {
            for (DemandWorker w : dmdWorkers)
                new IgniteThread(cctx.gridName(), "preloader-demand-worker", w).start();
        }
    }

    /**
     *
     */
    void stop() {
        U.cancel(dmdWorkers);

        if (log.isDebugEnabled())
            log.debug("Before joining on demand workers: " + dmdWorkers);

        U.join(dmdWorkers, log);

        if (log.isDebugEnabled())
            log.debug("After joining on demand workers: " + dmdWorkers);

        lastExchangeFut = null;

        lastTimeoutObj.set(null);
    }

    /**
     * @return Future for {@link CacheRebalanceMode#SYNC} mode.
     */
    IgniteInternalFuture<?> syncFuture() {
        return syncFut;
    }

    /**
     * Sets preload predicate for demand pool.
     *
     * @param preloadPred Preload predicate.
     */
    void preloadPredicate(IgnitePredicate<GridCacheEntryInfo> preloadPred) {
        this.preloadPred = preloadPred;
    }

    /**
     * @return Size of this thread pool.
     */
    int poolSize() {
        return poolSize;
    }

    /**
     * Force preload.
     */
    void forcePreload() {
        GridTimeoutObject obj = lastTimeoutObj.getAndSet(null);

        if (obj != null)
            cctx.time().removeTimeoutObject(obj);

        final GridDhtPartitionsExchangeFuture exchFut = lastExchangeFut;

        if (exchFut != null) {
            if (log.isDebugEnabled())
                log.debug("Forcing rebalance event for future: " + exchFut);

            exchFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> t) {
                    cctx.shared().exchange().forcePreloadExchange(exchFut);
                }
            });
        }
        else if (log.isDebugEnabled())
            log.debug("Ignoring force rebalance request (no topology event happened yet).");
    }

    /**
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        if (busyLock.readLock().tryLock())
            return true;

        if (log.isDebugEnabled())
            log.debug("Failed to enter to busy state (demander is stopping): " + cctx.nodeId());

        return false;
    }

    /**
     * @param idx
     * @return topic
     */
    static Object topic(int idx, int cacheId, UUID nodeId) {
        return TOPIC_CACHE.topic("DemandPool", nodeId, cacheId, idx);//Todo: remove nodeId
    }

    /**
     *
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /**
     * @param type Type.
     * @param discoEvt Discovery event.
     */
    private void preloadEvent(int type, DiscoveryEvent discoEvt) {
        preloadEvent(-1, type, discoEvt);
    }

    /**
     * @param part Partition.
     * @param type Type.
     * @param discoEvt Discovery event.
     */
    private void preloadEvent(int part, int type, DiscoveryEvent discoEvt) {
        assert discoEvt != null;

        cctx.events().addPreloadEvent(part, type, discoEvt.eventNode(), discoEvt.type(), discoEvt.timestamp());
    }

    /**
     * @param deque Deque to poll from.
     * @param time Time to wait.
     * @param w Worker.
     * @return Polled item.
     * @throws InterruptedException If interrupted.
     */
    @Nullable private <T> T poll(BlockingQueue<T> deque, long time, GridWorker w) throws InterruptedException {
        assert w != null;

        // There is currently a case where {@code interrupted}
        // flag on a thread gets flipped during stop which causes the pool to hang.  This check
        // will always make sure that interrupted flag gets reset before going into wait conditions.
        // The true fix should actually make sure that interrupted flag does not get reset or that
        // interrupted exception gets propagated. Until we find a real fix, this method should
        // always work to make sure that there is no hanging during stop.
        if (w.isCancelled())
            Thread.currentThread().interrupt();

        return deque.poll(time, MILLISECONDS);
    }

    /**
     * @param p Partition.
     * @param topVer Topology version.
     * @return Picked owners.
     */
    private Collection<ClusterNode> pickedOwners(int p, AffinityTopologyVersion topVer) {
        Collection<ClusterNode> affNodes = cctx.affinity().nodes(p, topVer);

        int affCnt = affNodes.size();

        Collection<ClusterNode> rmts = remoteOwners(p, topVer);

        int rmtCnt = rmts.size();

        if (rmtCnt <= affCnt)
            return rmts;

        List<ClusterNode> sorted = new ArrayList<>(rmts);

        // Sort in descending order, so nodes with higher order will be first.
        Collections.sort(sorted, CU.nodeComparator(false));

        // Pick newest nodes.
        return sorted.subList(0, affCnt);
    }

    /**
     * @param p Partition.
     * @param topVer Topology version.
     * @return Nodes owning this partition.
     */
    private Collection<ClusterNode> remoteOwners(int p, AffinityTopologyVersion topVer) {
        return F.view(cctx.dht().topology().owners(p, topVer), F.remoteNodes(cctx.nodeId()));
    }

    /**
     * @param assigns Assignments.
     * @param force {@code True} if dummy reassign.
     */
    void addAssignments(final GridDhtPreloaderAssignments assigns, boolean force) {
        if (log.isDebugEnabled())
            log.debug("Adding partition assignments: " + assigns);

        long delay = cctx.config().getRebalanceDelay();

        if (delay == 0 || force) {
            assert assigns != null;

            synchronized (dmdWorkers) {
                for (DemandWorker w : dmdWorkers)
                    w.addAssignments(assigns);
            }
        }
        else if (delay > 0) {
            assert !force;

            GridTimeoutObject obj = lastTimeoutObj.get();

            if (obj != null)
                cctx.time().removeTimeoutObject(obj);

            final GridDhtPartitionsExchangeFuture exchFut = lastExchangeFut;

            assert exchFut != null : "Delaying rebalance process without topology event.";

            obj = new GridTimeoutObjectAdapter(delay) {
                @Override public void onTimeout() {
                    exchFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> f) {
                            cctx.shared().exchange().forcePreloadExchange(exchFut);
                        }
                    });
                }
            };

            lastTimeoutObj.set(obj);

            cctx.time().addTimeoutObject(obj);
        }
    }

    /**
     *
     */
    void unwindUndeploys() {
        demandLock.writeLock().lock();

        try {
            cctx.deploy().unwind(cctx);
        }
        finally {
            demandLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionDemander.class, this);
    }

    /**
     *
     */
    private class DemandWorker extends GridWorker {
        /** Worker ID. */
        private int id;

        /** Partition-to-node assignments. */
        private final LinkedBlockingDeque<GridDhtPreloaderAssignments> assignQ = new LinkedBlockingDeque<>();

        /** Hide worker logger and use cache logger instead. */
        private IgniteLogger log = GridDhtPartitionDemander.this.log;

        /**
         * @param id Worker ID.
         */
        private DemandWorker(int id) {
            super(cctx.gridName(), "preloader-demand-worker", GridDhtPartitionDemander.this.log);

            assert id >= 0;

            this.id = id;
        }

        /**
         * @param assigns Assignments.
         */
        void addAssignments(GridDhtPreloaderAssignments assigns) {
            assert assigns != null;

            assignQ.offer(assigns);

            if (log.isDebugEnabled())
                log.debug("Added assignments to worker: " + this);
        }

        /**
         * @return {@code True} if topology changed.
         */
        private boolean topologyChanged() {
            return !assignQ.isEmpty() || cctx.shared().exchange().topologyChanged();
        }

        /**
         * @param pick Node picked for preloading.
         * @param p Partition.
         * @param entry Preloaded entry.
         * @param topVer Topology version.
         * @return {@code False} if partition has become invalid during preloading.
         * @throws IgniteInterruptedCheckedException If interrupted.
         */
        private boolean preloadEntry(
            ClusterNode pick,
            int p,
            GridCacheEntryInfo entry,
            AffinityTopologyVersion topVer
        ) throws IgniteCheckedException {
            try {
                GridCacheEntryEx cached = null;

                try {
                    cached = cctx.dht().entryEx(entry.key());

                    if (log.isDebugEnabled())
                        log.debug("Rebalancing key [key=" + entry.key() + ", part=" + p + ", node=" + pick.id() + ']');

                    if (cctx.dht().isIgfsDataCache() &&
                        cctx.dht().igfsDataSpaceUsed() > cctx.dht().igfsDataSpaceMax()) {
                        LT.error(log, null, "Failed to rebalance IGFS data cache (IGFS space size exceeded maximum " +
                            "value, will ignore rebalance entries): " + name());

                        if (cached.markObsoleteIfEmpty(null))
                            cached.context().cache().removeIfObsolete(cached.key());

                        return true;
                    }

                    if (preloadPred == null || preloadPred.apply(entry)) {
                        if (cached.initialValue(
                            entry.value(),
                            entry.version(),
                            entry.ttl(),
                            entry.expireTime(),
                            true,
                            topVer,
                            cctx.isDrEnabled() ? DR_PRELOAD : DR_NONE
                        )) {
                            cctx.evicts().touch(cached, topVer); // Start tracking.

                            if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_LOADED) && !cached.isInternal())
                                cctx.events().addEvent(cached.partition(), cached.key(), cctx.localNodeId(),
                                    (IgniteUuid)null, null, EVT_CACHE_REBALANCE_OBJECT_LOADED, entry.value(), true, null,
                                    false, null, null, null);
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Rebalancing entry is already in cache (will ignore) [key=" + cached.key() +
                                ", part=" + p + ']');
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Rebalance predicate evaluated to false for entry (will ignore): " + entry);
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Entry has been concurrently removed while rebalancing (will ignore) [key=" +
                            cached.key() + ", part=" + p + ']');
                }
                catch (GridDhtInvalidPartitionException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Partition became invalid during rebalancing (will ignore): " + p);

                    return false;
                }
            }
            catch (IgniteInterruptedCheckedException e) {
                throw e;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteCheckedException("Failed to cache rebalanced entry (will stop rebalancing) [local=" +
                    cctx.nodeId() + ", node=" + pick.id() + ", key=" + entry.key() + ", part=" + p + ']', e);
            }

            return true;
        }

        /**
         * @param node Node to demand from.
         * @param topVer Topology version.
         * @param d Demand message.
         * @param exchFut Exchange future.
         * @return Missed partitions.
         * @throws InterruptedException If interrupted.
         * @throws ClusterTopologyCheckedException If node left.
         * @throws IgniteCheckedException If failed to send message.
         */
        private Set<Integer> demandFromNode(
            final ClusterNode node,
            final AffinityTopologyVersion topVer,
            final GridDhtPartitionDemandMessage d,
            final GridDhtPartitionsExchangeFuture exchFut
        ) throws InterruptedException, IgniteCheckedException {
            final GridDhtPartitionTopology top = cctx.dht().topology();

            long timeout = GridDhtPartitionDemander.this.timeout.get();

            d.timeout(timeout);
            d.workerId(id);

            final Set<Integer> missed = new HashSet<>();

            final ConcurrentHashMap8<Integer, Boolean> remaining = new ConcurrentHashMap8<>();

            for (int p : d.partitions())
                remaining.put(p, false);

            if (isCancelled() || topologyChanged())
                return missed;

            int threadCnt = cctx.config().getRebalanceThreadPoolSize(); //todo = getRebalanceThreadPoolSize / assigns.count

            List<Set<Integer>> sParts = new ArrayList<>(threadCnt);

            int cnt = 0;

            while (cnt < threadCnt) {
                sParts.add(new HashSet<Integer>());

                final int idx = cnt;

                cctx.io().addOrderedHandler(topic(cnt, cctx.cacheId(), node.id()), new CI2<UUID, GridDhtPartitionSupplyMessage>() {
                    @Override public void apply(UUID id, GridDhtPartitionSupplyMessage m) {
                        enterBusy();

                        try {
                            handleSupplyMessage(idx, new SupplyMessage(id, m), node, topVer, top,
                                exchFut, missed, d, remaining);
                        }finally{
                            leaveBusy();
                        }
                    }
                });

                cnt++;
            }

            Iterator<Integer> it = d.partitions().iterator();

            cnt = 0;

            while (it.hasNext()) {
                sParts.get(cnt % threadCnt).add(it.next());

                cnt++;
            }

            try {
                cnt = 0;

                while (cnt < threadCnt) {

                    // Create copy.
                    GridDhtPartitionDemandMessage initD = new GridDhtPartitionDemandMessage(d, sParts.get(cnt));

                    initD.topic(topic(cnt, cctx.cacheId(),node.id()));

                    try {
                        if (logg && cctx.name().equals("cache"))
                        System.out.println("D "+cnt + " initial Demand "+" "+cctx.localNode().id());

                        cctx.io().sendOrderedMessage(node, GridDhtPartitionSupplier.topic(cnt, cctx.cacheId()), initD, cctx.ioPolicy(), d.timeout());
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to send partition demand message to local node", e);
                    }

                    cnt++;
                }

                do {
                    U.sleep(1000);//Todo: improve
                }
                while (!isCancelled() && !topologyChanged() && !remaining.isEmpty());

                return missed;
            }
            finally {
                cnt = 0;

                while (cnt < threadCnt) {
                    cctx.io().removeOrderedHandler(topic(cnt,cctx.cacheId(), node.id()));

                    cnt++;
                }
            }
        }

        boolean logg = false;

        /**
         * @param s Supply message.
         * @param node Node.
         * @param topVer Topology version.
         * @param top Topology.
         * @param exchFut Exchange future.
         * @param missed Missed.
         * @param d initial DemandMessage.
         */
        private void handleSupplyMessage(
            int idx,
            SupplyMessage s,
            ClusterNode node,
            AffinityTopologyVersion topVer,
            GridDhtPartitionTopology top,
            GridDhtPartitionsExchangeFuture exchFut,
            Set<Integer> missed,
            GridDhtPartitionDemandMessage d,
            ConcurrentHashMap8 remaining) {

            if (logg && cctx.name().equals("cache"))
            System.out.println("D "+idx + " handled supply message "+ cctx.localNode().id());

            // Check that message was received from expected node.
            if (!s.senderId().equals(node.id())) {
                U.warn(log, "Received supply message from unexpected node [expectedId=" + node.id() +
                    ", rcvdId=" + s.senderId() + ", msg=" + s + ']');

                return;
            }

            if (topologyChanged())
                return;

            if (log.isDebugEnabled())
                log.debug("Received supply message: " + s);

            GridDhtPartitionSupplyMessage supply = s.supply();

            // Check whether there were class loading errors on unmarshal
            if (supply.classError() != null) {
                if (log.isDebugEnabled())
                    log.debug("Class got undeployed during preloading: " + supply.classError());

                return;
            }

            // Preload.
            for (Map.Entry<Integer, CacheEntryInfoCollection> e : supply.infos().entrySet()) {
                int p = e.getKey();

                if (cctx.affinity().localNode(p, topVer)) {
                    GridDhtLocalPartition part = top.localPartition(p, topVer, true);

                    assert part != null;

                    if (part.state() == MOVING) {
                        boolean reserved = part.reserve();

                        assert reserved : "Failed to reserve partition [gridName=" +
                            cctx.gridName() + ", cacheName=" + cctx.namex() + ", part=" + part + ']';

                        part.lock();

                        try {
                            // Loop through all received entries and try to preload them.
                            for (GridCacheEntryInfo entry : e.getValue().infos()) {
                                if (!part.preloadingPermitted(entry.key(), entry.version())) {
                                    if (log.isDebugEnabled())
                                        log.debug("Preloading is not permitted for entry due to " +
                                            "evictions [key=" + entry.key() +
                                            ", ver=" + entry.version() + ']');

                                    continue;
                                }
                                try {
                                    if (!preloadEntry(node, p, entry, topVer)) {
                                        if (log.isDebugEnabled())
                                            log.debug("Got entries for invalid partition during " +
                                                "preloading (will skip) [p=" + p + ", entry=" + entry + ']');

                                        break;
                                    }
                                }
                                catch (IgniteCheckedException ex) {
                                    cancel();

                                    return;
                                }
                            }

                            boolean last = supply.last().contains(p);

                            // If message was last for this partition,
                            // then we take ownership.
                            if (last) {
                                top.own(part);//todo: close future?

//                                if (logg && cctx.name().equals("cache"))
//                                    System.out.println("D "+idx + " last "+ p +" "+ cctx.localNode().id());

                                remaining.remove(p);

                                if (log.isDebugEnabled())
                                    log.debug("Finished rebalancing partition: " + part);

                                if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_PART_LOADED))
                                    preloadEvent(p, EVT_CACHE_REBALANCE_PART_LOADED,
                                        exchFut.discoveryEvent());
                            }
                        }
                        finally {
                            part.unlock();
                            part.release();
                        }
                    }
                    else {
                        remaining.remove(p);

                        if (log.isDebugEnabled())
                            log.debug("Skipping rebalancing partition (state is not MOVING): " + part);
                    }
                }
                else {
                    remaining.remove(p);

                    if (log.isDebugEnabled())
                        log.debug("Skipping rebalancing partition (it does not belong on current node): " + p);
                }
            }

            for (Integer miss : s.supply().missed())
                remaining.remove(miss);

            // Only request partitions based on latest topology version.
            for (Integer miss : s.supply().missed())
                if (cctx.affinity().localNode(miss, topVer))
                    missed.add(miss);

            if (!remaining.isEmpty()) {
                try {
                    // Create copy.
                    GridDhtPartitionDemandMessage nextD =
                        new GridDhtPartitionDemandMessage(d, Collections.<Integer>emptySet());

                    nextD.topic(topic(idx, cctx.cacheId(), node.id()));

                    // Send demand message.
                    cctx.io().sendOrderedMessage(node, GridDhtPartitionSupplier.topic(idx, cctx.cacheId()),
                        nextD, cctx.ioPolicy(), d.timeout());

                    if (logg && cctx.name().equals("cache"))
                        System.out.println("D " + idx + " ack  " + cctx.localNode().id());
                }
                catch (IgniteCheckedException ex) {
                    U.error(log, "Failed to receive partitions from node (rebalancing will not " +
                        "fully finish) [node=" + node.id() + ", msg=" + d + ']', ex);

                    cancel();
                }
            }
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            try {
                int rebalanceOrder = cctx.config().getRebalanceOrder();

                if (!CU.isMarshallerCache(cctx.name())) {
                    if (log.isDebugEnabled())
                        log.debug("Waiting for marshaller cache preload [cacheName=" + cctx.name() + ']');

                    try {
                        cctx.kernalContext().cache().marshallerCache().preloader().syncFuture().get();
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to wait for marshaller cache preload future (grid is stopping): " +
                                "[cacheName=" + cctx.name() + ']');

                        return;
                    }
                    catch (IgniteCheckedException e) {
                        throw new Error("Ordered preload future should never fail: " + e.getMessage(), e);
                    }
                }

                if (rebalanceOrder > 0) {
                    IgniteInternalFuture<?> fut = cctx.kernalContext().cache().orderedPreloadFuture(rebalanceOrder);

                    try {
                        if (fut != null) {
                            if (log.isDebugEnabled())
                                log.debug("Waiting for dependant caches rebalance [cacheName=" + cctx.name() +
                                    ", rebalanceOrder=" + rebalanceOrder + ']');

                            fut.get();
                        }
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to wait for ordered rebalance future (grid is stopping): " +
                                "[cacheName=" + cctx.name() + ", rebalanceOrder=" + rebalanceOrder + ']');

                        return;
                    }
                    catch (IgniteCheckedException e) {
                        throw new Error("Ordered rebalance future should never fail: " + e.getMessage(), e);
                    }
                }

                GridDhtPartitionsExchangeFuture exchFut = null;

                boolean stopEvtFired = false;

                while (!isCancelled()) {
                    try {
                        barrier.await();

                        if (id == 0 && exchFut != null && !exchFut.dummy() &&
                            cctx.events().isRecordable(EVT_CACHE_REBALANCE_STOPPED)) {

                            if (!cctx.isReplicated() || !stopEvtFired) {
                                preloadEvent(EVT_CACHE_REBALANCE_STOPPED, exchFut.discoveryEvent());

                                stopEvtFired = true;
                            }
                        }
                    }
                    catch (BrokenBarrierException ignore) {
                        throw new InterruptedException("Demand worker stopped.");
                    }

                    // Sync up all demand threads at this step.
                    GridDhtPreloaderAssignments assigns = null;

                    while (assigns == null)
                        assigns = poll(assignQ, cctx.gridConfig().getNetworkTimeout(), this);

                    demandLock.readLock().lock();

                    try {
                        exchFut = assigns.exchangeFuture();

                        // Assignments are empty if preloading is disabled.
                        if (assigns.isEmpty())
                            continue;

                        boolean resync = false;

                        // While.
                        // =====
                        while (!isCancelled() && !topologyChanged() && !resync) {
                            Collection<Integer> missed = new HashSet<>();

                            // For.
                            // ===
                            for (ClusterNode node : assigns.keySet()) {
                                if (topologyChanged() || isCancelled())
                                    break; // For.

                                GridDhtPartitionDemandMessage d = assigns.remove(node);

                                // If another thread is already processing this message,
                                // move to the next node.
                                if (d == null)
                                    continue; // For.

                                try {
                                    Set<Integer> set = demandFromNode(node, assigns.topologyVersion(), d, exchFut);

                                    if (!set.isEmpty()) {
                                        if (log.isDebugEnabled())
                                            log.debug("Missed partitions from node [nodeId=" + node.id() + ", missed=" +
                                                set + ']');

                                        missed.addAll(set);
                                    }
                                }
                                catch (IgniteInterruptedCheckedException e) {
                                    throw e;
                                }
                                catch (ClusterTopologyCheckedException e) {
                                    if (log.isDebugEnabled())
                                        log.debug("Node left during rebalancing (will retry) [node=" + node.id() +
                                            ", msg=" + e.getMessage() + ']');

                                    resync = true;

                                    break; // For.
                                }
                                catch (IgniteCheckedException e) {
                                    U.error(log, "Failed to receive partitions from node (rebalancing will not " +
                                        "fully finish) [node=" + node.id() + ", msg=" + d + ']', e);
                                }
                            }

                            // Processed missed entries.
                            if (!missed.isEmpty()) {
                                if (log.isDebugEnabled())
                                    log.debug("Reassigning partitions that were missed: " + missed);

                                assert exchFut.exchangeId() != null;

                                cctx.shared().exchange().forceDummyExchange(true, exchFut);
                            }
                            else
                                break; // While.
                        }
                    }
                    finally {
                        demandLock.readLock().unlock();

                        syncFut.onWorkerDone(this);
                    }

                    cctx.shared().exchange().scheduleResendPartitions();
                }
            }
            finally {
                // Safety.
                syncFut.onWorkerDone(this);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DemandWorker.class, this, "assignQ", assignQ, "super", super.toString());
        }
    }

    /**
     * Sets last exchange future.
     *
     * @param lastFut Last future to set.
     */
    void updateLastExchangeFuture(GridDhtPartitionsExchangeFuture lastFut) {
        lastExchangeFut = lastFut;
    }

    /**
     * @param exchFut Exchange future.
     * @return Assignments of partitions to nodes.
     */
    GridDhtPreloaderAssignments assign(GridDhtPartitionsExchangeFuture exchFut) {
        // No assignments for disabled preloader.
        GridDhtPartitionTopology top = cctx.dht().topology();

        if (!cctx.rebalanceEnabled())
            return new GridDhtPreloaderAssignments(exchFut, top.topologyVersion());

        int partCnt = cctx.affinity().partitions();

        assert exchFut.forcePreload() || exchFut.dummyReassign() ||
            exchFut.exchangeId().topologyVersion().equals(top.topologyVersion()) :
            "Topology version mismatch [exchId=" + exchFut.exchangeId() +
                ", topVer=" + top.topologyVersion() + ']';

        GridDhtPreloaderAssignments assigns = new GridDhtPreloaderAssignments(exchFut, top.topologyVersion());

        AffinityTopologyVersion topVer = assigns.topologyVersion();

        for (int p = 0; p < partCnt; p++) {
            if (cctx.shared().exchange().hasPendingExchange()) {
                if (log.isDebugEnabled())
                    log.debug("Skipping assignments creation, exchange worker has pending assignments: " +
                        exchFut.exchangeId());

                break;
            }

            // If partition belongs to local node.
            if (cctx.affinity().localNode(p, topVer)) {
                GridDhtLocalPartition part = top.localPartition(p, topVer, true);

                assert part != null;
                assert part.id() == p;

                if (part.state() != MOVING) {
                    if (log.isDebugEnabled())
                        log.debug("Skipping partition assignment (state is not MOVING): " + part);

                    continue; // For.
                }

                Collection<ClusterNode> picked = pickedOwners(p, topVer);

                if (picked.isEmpty()) {
                    top.own(part);

                    if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_PART_DATA_LOST)) {
                        DiscoveryEvent discoEvt = exchFut.discoveryEvent();

                        cctx.events().addPreloadEvent(p,
                            EVT_CACHE_REBALANCE_PART_DATA_LOST, discoEvt.eventNode(),
                            discoEvt.type(), discoEvt.timestamp());
                    }

                    if (log.isDebugEnabled())
                        log.debug("Owning partition as there are no other owners: " + part);
                }
                else {
                    ClusterNode n = F.first(picked);

                    GridDhtPartitionDemandMessage msg = assigns.get(n);

                    if (msg == null) {
                        assigns.put(n, msg = new GridDhtPartitionDemandMessage(
                            top.updateSequence(),
                            exchFut.exchangeId().topologyVersion(),
                            cctx.cacheId()));
                    }

                    msg.addPartition(p);
                }
            }
        }

        return assigns;
    }

    /**
     *
     */
    private class SyncFuture extends GridFutureAdapter<Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Remaining workers. */
        private Collection<DemandWorker> remaining;

        /**
         * @param workers List of workers.
         */
        private SyncFuture(Collection<DemandWorker> workers) {
            assert workers.size() == poolSize();

            remaining = Collections.synchronizedList(new LinkedList<>(workers));
        }

        /**
         * @param w Worker who iterated through all partitions.
         */
        void onWorkerDone(DemandWorker w) {
            if (isDone())
                return;

            if (remaining.remove(w))
                if (log.isDebugEnabled())
                    log.debug("Completed full partition iteration for worker [worker=" + w + ']');

            if (remaining.isEmpty()) {
                if (log.isDebugEnabled())
                    log.debug("Completed sync future.");

                onDone();
            }
        }
    }

    /**
     * Supply message wrapper.
     */
    private static class SupplyMessage {
        /** Sender ID. */
        private UUID sndId;

        /** Supply message. */
        private GridDhtPartitionSupplyMessage supply;

        /**
         * Dummy constructor.
         */
        private SupplyMessage() {
            // No-op.
        }

        /**
         * @param sndId Sender ID.
         * @param supply Supply message.
         */
        SupplyMessage(UUID sndId, GridDhtPartitionSupplyMessage supply) {
            this.sndId = sndId;
            this.supply = supply;
        }

        /**
         * @return Sender ID.
         */
        UUID senderId() {
            return sndId;
        }

        /**
         * @return Message.
         */
        GridDhtPartitionSupplyMessage supply() {
            return supply;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SupplyMessage.class, this);
        }
    }
}
