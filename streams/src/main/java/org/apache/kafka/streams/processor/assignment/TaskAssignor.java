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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Set;

/**
 * TODO(KIP-924):
 *   -extract computation of NodeState task lag/end offsets fetch
 *   -consider moving thread-level assignment to TaskAssignor
 *   -provide handle on StandbyTaskAssignors (move StandbyTaskAssignorFactory to public assignmnent package)
 *   -integrate with new rack-aware (active) task assignment
 */
public interface TaskAssignor extends Configurable {
  TaskAssignment assign(final ApplicationMetadata applicationMetadata);


  /**
   * Wrapper class for the final assignment of active and standbys tasks to individual Streams nodes
   */
  class TaskAssignment {
    private final Collection<NodeAssignment> nodeAssignments;

    /**
     * @param nodeAssignments the complete collection of all nodes and their desired task assignments
     */
    public TaskAssignment(final Collection<NodeAssignment> nodeAssignments) {
      this.nodeAssignments = nodeAssignments;
    }

    public Collection<NodeAssignment> assignment() {
      return nodeAssignments;
    }

    /**
     * @return the number of Streams client nodes to which tasks were assigned
     */
    public int numNodes() {
      return nodeAssignments.size();
    }

    /**
     * @return a String representation of the returned assignment, in processId order
     */
    @Override
    public String toString() {
      return assignment().stream()
          .sorted(Comparator.comparing(NodeAssignment::processId))
          .map(NodeAssignment::print)
          .collect(Collectors.joining(Utils.NL));
    }
  }
}
