/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.coordinator.helper;

import com.google.common.collect.Lists;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.BalancerSegmentHolder;
import org.apache.druid.server.coordinator.BalancerStrategy;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.LoadPeonCallback;
import org.apache.druid.server.coordinator.LoadQueuePeon;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 */
public class DruidCoordinatorBalancer implements DruidCoordinatorHelper
{

  protected static final EmittingLogger log = new EmittingLogger(DruidCoordinatorBalancer.class);

  protected final DruidCoordinator coordinator;

  protected final Map<String, ConcurrentHashMap<String, BalancerSegmentHolder>> currentlyMovingSegments =
      new HashMap<>();

  public DruidCoordinatorBalancer(
      DruidCoordinator coordinator
  )
  {
    this.coordinator = coordinator;
  }

  protected void reduceLifetimes(String tier)
  {
    for (BalancerSegmentHolder holder : currentlyMovingSegments.get(tier).values()) {
      holder.reduceLifetime();
      if (holder.getLifetime() <= 0) {
        log.makeAlert("[%s]: Balancer move segments queue has a segment stuck", tier)
           .addData("segment", holder.getSegment().getIdentifier())
           .addData("server", holder.getFromServer().getMetadata())
           .emit();
      }
    }
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final CoordinatorStats stats = new CoordinatorStats();
    params.getDruidCluster().getHistoricals().forEach((String tier, NavigableSet<ServerHolder> servers) -> {
      balanceTier(params, tier, servers, stats);
    });
    return params.buildFromExisting().withCoordinatorStats(stats).build();
  }

  private void balanceTier(
      DruidCoordinatorRuntimeParams params,
      String tier,
      SortedSet<ServerHolder> servers,
      CoordinatorStats stats
  )
  {

    if (params.getAvailableSegments().size() == 0) {
      log.info("Metadata segments are not available. Cannot balance.");
      return;
    }
    currentlyMovingSegments.computeIfAbsent(tier, t -> new ConcurrentHashMap<>());

    if (!currentlyMovingSegments.get(tier).isEmpty()) {
      reduceLifetimes(tier);
      log.info("[%s]: Still waiting on %,d segments to be moved", tier, currentlyMovingSegments.get(tier).size());
      return;
    }

    Map<Boolean, List<ServerHolder>> partitions = servers.stream()
                                                      .collect(Collectors.partitioningBy(ServerHolder::isMaintenance));
    final List<ServerHolder> maintenance = partitions.get(true);
    final List<ServerHolder> general = partitions.get(false);
    log.info("Found %d servers in maintenance, %d general servers", maintenance.size(), general.size());

    if (general.size() == 0) {
      log.info("[%s]: 0 general servers found.  Cannot balance.", tier);
      return;
    }

    int numSegments = 0;
    for (ServerHolder sourceHolder : servers) {
      numSegments += sourceHolder.getServer().getSegments().size();
    }

    if (numSegments == 0) {
      log.info("No segments found.  Cannot balance.");
      return;
    }

    final int maxSegmentsToMove = Math.min(params.getCoordinatorDynamicConfig().getMaxSegmentsToMove(), numSegments);
    int priority = params.getCoordinatorDynamicConfig().getMaintenanceModeSegmentsPriority();
    int maintenanceSegmentsToMove = (int) Math.ceil(maxSegmentsToMove * priority / 10.0);
    log.info("Balancing %d segments for servers in maintenance mode", maintenanceSegmentsToMove);
    Pair<Integer, Integer> maintenanceResult = balanceServers(params, maintenance, general, maintenanceSegmentsToMove);
    int generalSegmentsToMove = maxSegmentsToMove - maintenanceResult.lhs;
    log.info("Balancing %d segments for servers in general mode", generalSegmentsToMove);
    Pair<Integer, Integer> generalResult = balanceServers(params, general, general, generalSegmentsToMove);
    Pair<Integer, Integer> result = new Pair(
        generalResult.lhs + maintenanceResult.lhs,
        generalResult.rhs + maintenanceResult.rhs
    );

    int moved = result.lhs;
    int unmoved = result.rhs;
    if (unmoved == maxSegmentsToMove) {
      // Cluster should be alive and constantly adjusting
      log.info("No good moves found in tier [%s]", tier);
    }
    stats.addToTieredStat("unmovedCount", tier, unmoved);
    stats.addToTieredStat("movedCount", tier, moved);

    if (params.getCoordinatorDynamicConfig().emitBalancingStats()) {
      final BalancerStrategy strategy = params.getBalancerStrategy();
      strategy.emitStats(tier, stats, Lists.newArrayList(servers));
    }
    log.info(
        "[%s]: Segments Moved: [%d] Segments Let Alone: [%d]",
        tier,
        moved,
        unmoved
    );
  }

  private Pair<Integer, Integer> balanceServers(
      DruidCoordinatorRuntimeParams params,
      List<ServerHolder> toMoveFrom,
      List<ServerHolder> toMoveTo,
      int maxSegmentsToMove
  )
  {
    final BalancerStrategy strategy = params.getBalancerStrategy();
    final int maxIterations = 2 * maxSegmentsToMove;
    final int maxToLoad = params.getCoordinatorDynamicConfig().getMaxSegmentsInNodeLoadingQueue();
    int moved = 0, unmoved = 0;

    //noinspection ForLoopThatDoesntUseLoopVariable
    for (int iter = 0; (moved + unmoved) < maxSegmentsToMove; ++iter) {
      final BalancerSegmentHolder segmentToMoveHolder = strategy.pickSegmentToMove(toMoveFrom);
      if (segmentToMoveHolder != null && params.getAvailableSegments().contains(segmentToMoveHolder.getSegment())) {
        final DataSegment segmentToMove = segmentToMoveHolder.getSegment();
        final ImmutableDruidServer fromServer = segmentToMoveHolder.getFromServer();
        // we want to leave the server the segment is currently on in the list...
        // but filter out replicas that are already serving the segment, and servers with a full load queue
        final List<ServerHolder> toMoveToWithLoadQueueCapacityAndNotServingSegment =
            toMoveTo.stream()
                    .filter(s -> s.getServer().equals(fromServer) ||
                                 (!s.isServingSegment(segmentToMove) &&
                                  (maxToLoad <= 0 || s.getNumberOfSegmentsInQueue() < maxToLoad)))
                    .collect(Collectors.toList());

        if (toMoveToWithLoadQueueCapacityAndNotServingSegment.size() > 0) {
          final ServerHolder destinationHolder =
              strategy.findNewSegmentHomeBalancer(segmentToMove, toMoveToWithLoadQueueCapacityAndNotServingSegment);

          if (destinationHolder != null && !destinationHolder.getServer().equals(fromServer)) {
            if (moveSegment(segmentToMoveHolder, destinationHolder.getServer(), params)) {
              moved++;
            }
          } else {
            log.info("Segment [%s] is 'optimally' placed.", segmentToMove.getIdentifier());
            unmoved++;
          }
        } else {
          log.info(
              "No valid movement destinations for segment [%s].",
              segmentToMove.getIdentifier()
          );
          unmoved++;
        }
      }
      if (iter >= maxIterations) {
        log.info(
            "Unable to select %d remaining candidate segments out of %d total to balance after %d iterations, ending run.",
            (maxSegmentsToMove - moved - unmoved), maxSegmentsToMove, iter
        );
        break;
      }
    }
    return new Pair<>(moved, unmoved);
  }

  protected boolean moveSegment(
      final BalancerSegmentHolder segment,
      final ImmutableDruidServer toServer,
      final DruidCoordinatorRuntimeParams params
  )
  {
    final LoadQueuePeon toPeon = params.getLoadManagementPeons().get(toServer.getName());

    final ImmutableDruidServer fromServer = segment.getFromServer();
    final DataSegment segmentToMove = segment.getSegment();
    final String segmentName = segmentToMove.getIdentifier();

    if (!toPeon.getSegmentsToLoad().contains(segmentToMove) &&
        (toServer.getSegment(segmentName) == null) &&
        new ServerHolder(toServer, toPeon).getAvailableSize() > segmentToMove.getSize()) {
      log.info("Moving [%s] from [%s] to [%s]", segmentName, fromServer.getName(), toServer.getName());

      LoadPeonCallback callback = null;
      try {
        Map<String, BalancerSegmentHolder> movingSegments = currentlyMovingSegments.get(toServer.getTier());
        movingSegments.put(segmentName, segment);
        callback = () -> movingSegments.remove(segmentName);
        coordinator.moveSegment(
            fromServer,
            toServer,
            segmentToMove,
            callback
        );
        return true;
      }
      catch (Exception e) {
        log.makeAlert(e, StringUtils.format("[%s] : Moving exception", segmentName)).emit();
        if (callback != null) {
          callback.execute();
        }
      }
    }
    return false;
  }
}
