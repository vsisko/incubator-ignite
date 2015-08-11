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
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jsr166.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.GridTopic.*;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.*;
import static org.apache.ignite.internal.processors.dr.GridDrType.*;

/**
 * Thread pool for requesting partitions from other nodes and populating local cache.
 */
@SuppressWarnings("NonConstantFieldWithUpperCaseName")
public class GridDhtPartitionDemander {
    /** */
    private final GridCacheContext<?, ?> cctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final ReadWriteLock busyLock;

    /** Preload predicate. */
    private IgnitePredicate<GridCacheEntryInfo> preloadPred;

    /** Future for preload mode {@link CacheRebalanceMode#SYNC}. */
    @GridToStringInclude
    private volatile SyncFuture syncFut;

    /** Last timeout object. */
    private AtomicReference<GridTimeoutObject> lastTimeoutObj = new AtomicReference<>();

    /** Last exchange future. */
    private volatile GridDhtPartitionsExchangeFuture lastExchangeFut;

    /** Assignments. */
    private volatile GridDhtPreloaderAssignments assigns;

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

        if (enabled) {

            for (int cnt = 0; cnt < cctx.config().getRebalanceThreadPoolSize(); cnt++) {
                final int idx = cnt;

                cctx.io().addOrderedHandler(topic(cnt, cctx.cacheId()), new CI2<UUID, GridDhtPartitionSupplyMessage>() {
                    @Override public void apply(final UUID id, final GridDhtPartitionSupplyMessage m) {
                        enterBusy();

                        try {
                            handleSupplyMessage(idx, id, m);
                        }
                        finally {
                            leaveBusy();
                        }
                    }
                });
            }
        }

        syncFut = new SyncFuture();

        if (!enabled)
            // Calling onDone() immediately since preloading is disabled.
            syncFut.onDone();
    }

    /**
     *
     */
    void start() {
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
    }

    /**
     *
     */
    void stop() {
        if (cctx.rebalanceEnabled() && !cctx.kernalContext().clientNode()) {
            for (int cnt = 0; cnt < cctx.config().getRebalanceThreadPoolSize(); cnt++)
                cctx.io().removeOrderedHandler(topic(cnt, cctx.cacheId()));
        }

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
    static Object topic(int idx, int cacheId) {
        return TOPIC_CACHE.topic("Demander", cacheId, idx);
    }

    /**
     * @return {@code True} if topology changed.
     */
    private boolean topologyChanged(AffinityTopologyVersion topVer) {
        return !cctx.affinity().affinityTopologyVersion().equals(topVer);
    }

    /**
     *
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
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
     * @param assigns Assignments.
     * @param force {@code True} if dummy reassign.
     */

    void addAssignments(final GridDhtPreloaderAssignments assigns, boolean force) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Adding partition assignments: " + assigns);

        long delay = cctx.config().getRebalanceDelay();

        if (delay == 0 || force) {
            assert assigns != null;

            AffinityTopologyVersion topVer = cctx.affinity().affinityTopologyVersion();

            if (this.assigns != null) {
                syncFut.get();

                syncFut = new SyncFuture();
            }

            if (assigns.isEmpty() || topologyChanged(topVer)) {
                syncFut.onDone();

                return;
            }

            this.assigns = assigns;

            for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e : assigns.entrySet()) {
                GridDhtPartitionDemandMessage d = e.getValue();

                d.timeout(cctx.config().getRebalanceTimeout());
                d.workerId(0);//old api support.

                ClusterNode node = e.getKey();

                GridConcurrentHashSet<Integer> remainings = new GridConcurrentHashSet<>();

                remainings.addAll(d.partitions());

                syncFut.append(node.id(), remainings);

                int lsnrCnt = cctx.config().getRebalanceThreadPoolSize();

                List<Set<Integer>> sParts = new ArrayList<>(lsnrCnt);

                for (int cnt = 0; cnt < lsnrCnt; cnt++)
                    sParts.add(new HashSet<Integer>());

                Iterator<Integer> it = d.partitions().iterator();

                int cnt = 0;

                while (it.hasNext())
                    sParts.get(cnt++ % lsnrCnt).add(it.next());

                for (cnt = 0; cnt < lsnrCnt; cnt++) {

                    if (!sParts.get(cnt).isEmpty()) {

                        // Create copy.
                        GridDhtPartitionDemandMessage initD = new GridDhtPartitionDemandMessage(d, sParts.get(cnt));

                        initD.topic(topic(cnt, cctx.cacheId()));

                        try {
                            cctx.io().sendOrderedMessage(node, GridDhtPartitionSupplier.topic(cnt, cctx.cacheId()), initD, cctx.ioPolicy(), d.timeout());
                        }
                        catch (IgniteCheckedException ex) {
                            U.error(log, "Failed to send partition demand message to local node", ex);
                        }
                    }
                }

                if (log.isInfoEnabled() && !d.partitions().isEmpty()) {
                    LinkedList<Integer> s = new LinkedList<>(d.partitions());

                    Collections.sort(s);

                    StringBuilder sb = new StringBuilder();

                    int start = -1;

                    int prev = -1;

                    Iterator<Integer> sit = s.iterator();

                    while (sit.hasNext()) {
                        int p = sit.next();
                        if (start == -1) {
                            start = p;
                            prev = p;
                        }

                        if (prev < p - 1) {
                            sb.append(start);

                            if (start != prev)
                                sb.append("-").append(prev);

                            sb.append(", ");

                            start = p;
                        }

                        if (!sit.hasNext()) {
                            sb.append(start);

                            if (start != p)
                                sb.append("-").append(p);
                        }

                        prev = p;
                    }

                    log.info("Requested rebalancing [from node=" + node.id() + ", partitions=" + s.size() + " (" + sb.toString() + ")]");
                }
            }
        }
        else if (delay > 0) {
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
     * @param idx Index.
     * @param id Node id.
     * @param supply Supply.
     */
    private void handleSupplyMessage(
        int idx,
        final UUID id,
        final GridDhtPartitionSupplyMessage supply) {
        ClusterNode node = cctx.node(id);

        assert node != null;

        GridDhtPartitionDemandMessage d = assigns.get(node);

        AffinityTopologyVersion topVer = d.topologyVersion();

        if (topologyChanged(topVer)) {
            syncFut.cancel(id);

            return;
        }

        if (log.isDebugEnabled())
            log.debug("Received supply message: " + supply);

        // Check whether there were class loading errors on unmarshal
        if (supply.classError() != null) {
            if (log.isDebugEnabled())
                log.debug("Class got undeployed during preloading: " + supply.classError());

            syncFut.cancel(id);

            return;
        }

        final GridDhtPartitionTopology top = cctx.dht().topology();

        GridDhtPartitionsExchangeFuture exchFut = assigns.exchangeFuture();

        try {

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
                                if (!preloadEntry(node, p, entry, topVer)) {
                                    if (log.isDebugEnabled())
                                        log.debug("Got entries for invalid partition during " +
                                            "preloading (will skip) [p=" + p + ", entry=" + entry + ']');

                                    break;
                                }
                            }

                            boolean last = supply.last().contains(p);

                            // If message was last for this partition,
                            // then we take ownership.
                            if (last) {
                                top.own(part);

                                syncFut.onPartitionDone(id, p);

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
                        syncFut.onPartitionDone(id, p);

                        if (log.isDebugEnabled())
                            log.debug("Skipping rebalancing partition (state is not MOVING): " + part);
                    }
                }
                else {
                    syncFut.onPartitionDone(id, p);

                    if (log.isDebugEnabled())
                        log.debug("Skipping rebalancing partition (it does not belong on current node): " + p);
                }
            }

            // Only request partitions based on latest topology version.
            for (Integer miss : supply.missed())
                if (cctx.affinity().localNode(miss, topVer))
                    syncFut.onMissedPartition(id, miss);

            for (Integer miss : supply.missed())
                syncFut.onPartitionDone(id, miss);

            if (!syncFut.isDone()) {

                // Create copy.
                GridDhtPartitionDemandMessage nextD =
                    new GridDhtPartitionDemandMessage(d, Collections.<Integer>emptySet());

                nextD.topic(topic(idx, cctx.cacheId()));

                // Send demand message.
                cctx.io().sendOrderedMessage(node, GridDhtPartitionSupplier.topic(idx, cctx.cacheId()),
                    nextD, cctx.ioPolicy(), d.timeout());
            }
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Node left during rebalancing (will retry) [node=" + node.id() +
                    ", msg=" + e.getMessage() + ']');
            syncFut.cancel(id);
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to receive partitions from node (rebalancing will not " +
                "fully finish) [node=" + node.id() + ", msg=" + d + ']', ex);

            syncFut.cancel(id);
        }
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
                        "value, will ignore rebalance entries)");

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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionDemander.class, this);
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
 *
 */
private class SyncFuture extends GridFutureAdapter<Object> {
    /** */
    private static final long serialVersionUID = 1L;

    private ConcurrentHashMap8<UUID, Collection<Integer>> remaining = new ConcurrentHashMap8<>();

    private ConcurrentHashMap8<UUID, Collection<Integer>> missed = new ConcurrentHashMap8<>();

    public void append(UUID nodeId, Collection<Integer> parts) {
        remaining.put(nodeId, parts);

        missed.put(nodeId, new GridConcurrentHashSet<Integer>());
    }

    void cancel(UUID nodeId) {
        if (isDone())
            return;

        remaining.remove(nodeId);

        checkIsDone();
    }

    void onMissedPartition(UUID nodeId, int p) {
        if (missed.get(nodeId) == null)
            missed.put(nodeId, new GridConcurrentHashSet<Integer>());

        missed.get(nodeId).add(p);
   }

    void onPartitionDone(UUID nodeId, int p) {
        if (isDone())
            return;

        Collection<Integer> parts = remaining.get(nodeId);

        parts.remove(p);

        if (parts.isEmpty()) {
            remaining.remove(nodeId);

            if (log.isDebugEnabled())
                log.debug("Completed full partition iteration for node [nodeId=" + nodeId + ']');
        }

        checkIsDone();
    }

    private void checkIsDone() {
        if (remaining.isEmpty()) {
            if (log.isDebugEnabled())
                log.debug("Completed sync future.");

            Collection<Integer> m = new HashSet<>();

            for (Map.Entry<UUID, Collection<Integer>> e : missed.entrySet()) {
                if (e.getValue() != null && !e.getValue().isEmpty())
                    m.addAll(e.getValue());
            }

            if (!m.isEmpty()) {
                if (log.isDebugEnabled())
                    log.debug("Reassigning partitions that were missed: " + m);

                cctx.shared().exchange().forceDummyExchange(true, assigns.exchangeFuture());
            }

            missed.clear();

            onDone();
        }
    }
}
}
