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
package org.apache.kafka.streams.processor.assignment;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.HostInfo;

public interface NodeState {

  /**
   * @return a new {@link NodeAssignment} that can be used to assign tasks for this node
   */
  NodeAssignment newAssignmentForNode();

  /**
   * @return the processId of the application instance running on this node
   */
  UUID processId();

  /**
   * Returns the number of StreamThreads on this node, which is equal to the number of main consumers
   * and represents its overall capacity.
   * <p>
   * NOTE: this is actually the "minimum capacity" of a node, or the minimum number of assigned
   * active tasks below which the node will have been over-provisioned and unable to give every
   * available StreamThread an active task assignment
   *
   * @return the number of consumers on this node
   */
  int numStreamThreads();

  /**
   * @return the set of consumer client ids for all StreamThreads on the given node
   */
  SortedSet<String> consumers();

  /**
   * @return the consumer on this node that previously owned this partition in the previous rebalance
   */
  String previousOwnerForPartition(final TopicPartition topicPartition);

  /**
   * Returns the total lag across all logged stores in the task. Equal to the end offset sum if this client
   * did not have any state for this task on disk.
   *
   * @return end offset sum - offset sum
   *          Task.LATEST_OFFSET if this was previously an active running task on this client
   */
  long lagFor(final TaskId task);

  /**
   * @return the previous tasks assigned to this consumer ordered by lag, filtered for any tasks that don't exist in this assignment
   */
  SortedSet<TaskId> prevTasksByLag(final String consumer);

  /**
   * Returns a collection containing all (and only) stateful tasks in the topology by {@link TaskId},
   * mapped to its "offset lag sum". This is computed as the difference between the changelog end offset
   * and the current offset, summed across all logged state stores in the task.
   *
   * @return a map from all stateful tasks to their lag sum
   */
  Map<TaskId, Long> statefulTasksToLagSums();

  /**
   * The {@link HostInfo} of this node, if set via the
   * {@link org.apache.kafka.streams.StreamsConfig#APPLICATION_SERVER_CONFIG application.server} config
   *
   * @return the host info for this node if configured, else {@code Optional.empty()}
   */
  Optional<HostInfo> hostInfo();

  /**
   * The client tags for this client node, if set any have been via configs using the
   * {@link org.apache.kafka.streams.StreamsConfig#clientTagPrefix}
   * <p>
   * Can be used however you want, or passed in to enable the rack-aware standby task assignor.
   *
   * @return all the client tags found in this node's {@link org.apache.kafka.streams.StreamsConfig}
   */
  Map<String, String> clientTags();

}
