/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals.assignment;

import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingLong;
import static org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo.UNKNOWN_OFFSET_SUM;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.NodeAssignment;
import org.apache.kafka.streams.processor.assignment.NodeState;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;

public class NodeStateImpl implements NodeState {

  private static final Comparator<TopicPartition> TOPIC_PARTITION_COMPARATOR = comparing(TopicPartition::topic).thenComparing(TopicPartition::partition);

  private final Logger log;

  private final UUID processId;
  private final Map<String, String> clientTags;
  private final long now;
  private final HostInfo hostInfo;

  // Prefer sorted collections for debugging purposes
  private final Map<TaskId, Long> statefulTaskOffsetSums = new TreeMap<>();
  private final Map<TaskId, Long> statefulTaskLagSums = new TreeMap<>();

  private final Map<TopicPartition, String> ownedPartitionsToConsumer = new TreeMap<>(TOPIC_PARTITION_COMPARATOR);
  private final Map<String, Set<TaskId>> consumerToPreviousStatefulTasks = new TreeMap<>();
  private final PreviousTaskAssignment previousTaskAssignment = new PreviousTaskAssignment();

  private static class PreviousTaskAssignment {
    public final Set<TaskId> active = new TreeSet<>();
    public final Set<TaskId> standby = new TreeSet<>();

    boolean initialized() {
      return !(active.isEmpty() && standby.isEmpty());
    }

    void retainAll(final Collection<TaskId> tasks) {
      active.retainAll(tasks);
      standby.retainAll(tasks);
    }

    boolean contains(final TaskId task) {
      return active.contains(task) || standby.contains(task);
    }
  }

  public NodeStateImpl(final UUID processId,
                       final Map<String, String> clientTags,
                       final long now,
                       final String endPoint) {
    log = new LogContext(String.format("node [%s] ", processId)).logger(NodeStateImpl.class);
    hostInfo = HostInfo.buildFromEndpoint(endPoint);

    this.processId = processId;
    this.clientTags = clientTags;
    this.now = now;
  }

  // Returns a stub node for the version probing edge case where the group leader (and thus assignor)
  // is on the older version and cannot read the metadata state from a "future" node's subscription
  public static NodeStateImpl futureVersionNode(final UUID processId) {
    return new NodeStateImpl(processId, Collections.emptyMap(), 0L, null);
  }

  @Override
  public NodeAssignment newAssignmentForNode() {
    return new NodeAssignmentImpl(this);
  }

  @Override
  public UUID processId() {
    return processId;
  }

  @Override
  public int numStreamThreads() {
    return consumers().size();
  }

  @Override
  public SortedSet<String> consumers() {
    return consumerToPreviousStatefulTasks.keySet();
  }

  @Override
  public String previousOwnerForPartition(final TopicPartition topicPartition) {
    return ownedPartitionsToConsumer.get(topicPartition);
  }

  @Override
  public Map<TaskId, Long> statefulTasksToLagSums() {
    return statefulTaskLagSums;
  }

  @Override
  public Optional<HostInfo> hostInfo() {
    return Optional.ofNullable(hostInfo);
  }

  @Override
  public Map<String, String> clientTags() {
    return clientTags;
  }

  public void addConsumerState(final String consumer,
                               final Collection<TopicPartition> ownedPartitions,
                               final Map<TaskId, Long> taskOffsetSums) {
    ownedPartitions.forEach(tp -> ownedPartitionsToConsumer.put(tp, consumer));
    statefulTaskOffsetSums.putAll(taskOffsetSums);
    consumerToPreviousStatefulTasks.put(consumer, taskOffsetSums.keySet());
  }

  public void initializePrevTasks(final Map<TopicPartition, TaskId> taskForPartitionMap,
                                  final boolean hasNamedTopologies) {
    if (previousTaskAssignment.initialized()) {
      log.error("Node state was already initialized:\n"
                    + "previousActiveTasks=({})\n"
                    + "previousStandbyTasks=({})",
                previousTaskAssignment.active, previousTaskAssignment.standby);
      throw new IllegalStateException("Already initialized previous tasks of this node state.");
    }

    maybeFilterUnknownPrevTasksAndPartitions(taskForPartitionMap, hasNamedTopologies);
    initializePrevActiveTasksFromOwnedPartitions(taskForPartitionMap);
    initializeRemainingPrevTasksFromTaskOffsetSums();
  }

  private void maybeFilterUnknownPrevTasksAndPartitions(final Map<TopicPartition, TaskId> taskForPartitionMap,
                                                        final boolean hasNamedTopologies) {
    // If this application uses named topologies, then it's possible for members to report tasks
    // or partitions in their subscription that belong to a named topology that the group leader
    // doesn't currently recognize, eg because it was just removed
    if (hasNamedTopologies) {
      ownedPartitionsToConsumer.keySet().retainAll(taskForPartitionMap.keySet());
      previousTaskAssignment.retainAll(taskForPartitionMap.values());
    }
  }

  private void initializePrevActiveTasksFromOwnedPartitions(final Map<TopicPartition, TaskId> taskForPartitionMap) {
    // there are three cases where we need to construct some or all of the prevTasks from the ownedPartitions:
    // 1) COOPERATIVE clients on version 2.4-2.5 do not encode active tasks at all and rely on ownedPartitions
    // 2) future client during version probing, when we can't decode the future subscription info's prev tasks
    // 3) stateless tasks are not encoded in the task lags, and must be figured out from the ownedPartitions
    for (final Map.Entry<TopicPartition, String> partitionEntry : ownedPartitionsToConsumer.entrySet()) {
      final TopicPartition tp = partitionEntry.getKey();
      final TaskId task = taskForPartitionMap.get(tp);
      if (task != null) {
        previousTaskAssignment.active.add(task);
      } else {
        log.error("No task found for topic partition {}", tp);
      }
    }
  }


  private void initializeRemainingPrevTasksFromTaskOffsetSums() {
    if (!previousTaskAssignment.initialized() && !ownedPartitionsToConsumer.isEmpty()) {
      log.error("Tried to process tasks in offset sum map before initializing the previous assignment"
                    + " from the non-empty ownedPartitions = {}", ownedPartitionsToConsumer);
      throw new IllegalStateException("Must initialize previous from ownedPartitions before initializing remaining tasks.");
    }
    for (final Map.Entry<TaskId, Long> taskEntry : statefulTaskOffsetSums.entrySet()) {
      final TaskId task = taskEntry.getKey();
      if (!previousTaskAssignment.contains(task)) {
        final long offsetSum = taskEntry.getValue();
        if (offsetSum == Task.LATEST_OFFSET) {
          previousTaskAssignment.active.add(task);
        } else {
          previousTaskAssignment.standby.add(task);
        }
      }
    }
  }

  /**
   * Compute the lag for each stateful task, including tasks this client did not previously have.
   */
  public void computeTaskLags(final Map<TaskId, Long> allTaskEndOffsetSums) {
    for (final Map.Entry<TaskId, Long> taskEntry : allTaskEndOffsetSums.entrySet()) {
      final TaskId task = taskEntry.getKey();
      final Long endOffsetSum = taskEntry.getValue();
      final Long offsetSum = statefulTaskLagSums.getOrDefault(task, 0L);

      if (offsetSum == Task.LATEST_OFFSET) {
        statefulTaskLagSums.put(task, Task.LATEST_OFFSET);
      } else if (offsetSum == UNKNOWN_OFFSET_SUM) {
        statefulTaskLagSums.put(task, UNKNOWN_OFFSET_SUM);
      } else if (endOffsetSum < offsetSum) {
        log.warn("Task " + task + " had endOffsetSum=" + endOffsetSum + " smaller than offsetSum=" +
                     offsetSum + " on node " + processId + ". This probably means the task is corrupted," +
                     " which in turn indicates that it will need to restore from scratch if it gets assigned." +
                     " The assignor will de-prioritize returning this task to this member in the hopes that" +
                     " some other member may be able to re-use its state.");
        statefulTaskLagSums.put(task, endOffsetSum);
      } else {
        statefulTaskLagSums.put(task, endOffsetSum - offsetSum);
      }
    }
  }

  @Override
  public long lagFor(final TaskId task) {
    final Long totalLag = statefulTaskLagSums.get(task);
    if (totalLag == null) {
      throw new IllegalStateException("Tried to lookup lag for unknown task " + task);
    }
    return totalLag;
  }

  @Override
  public SortedSet<TaskId> prevTasksByLag(final String consumer) {
    final SortedSet<TaskId> prevTasksByLag = new TreeSet<>(comparingLong(this::lagFor).thenComparing(TaskId::compareTo));
    for (final TaskId task : prevOwnedStatefulTasksByConsumer(consumer)) {
      if (statefulTaskLagSums.containsKey(task)) {
        prevTasksByLag.add(task);
      } else {
        log.debug("Skipping previous task {} since it's not part of the current assignment", task);
      }
    }
    return prevTasksByLag;
  }

  public long now() {
    return now;
  }
}
