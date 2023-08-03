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

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import org.apache.kafka.streams.processor.TaskId;

public interface ApplicationMetadata {

    /**
     * @param computeTaskLags whether to compute task lags for each {@link NodeState}.
     *                        NOTE: if true, a remote call will be made to fetch changelog end offsets
     * @return a map from the {@code processId} to {@link NodeState} for all Streams client nodes in this app
     */
    Map<UUID, NodeState> nodeStates(final boolean computeTaskLags);

    /**
     * @return a simple container class with the Streams configs relevant to assignment
     */
    TaskAssignorConfigs taskAssignorConfigs();

    /**
     * @return the set of all tasks in this topology which must be assigned to a node
     */
    Set<TaskId> allTasks();

    /**
     *
     * @return the set of stateful and changelogged tasks in this topology
     */
    Set<TaskId> statefulTasks();

    /**
     * Assign standby tasks to nodes according to the default logic.
     * <p>
     * If rack-aware client tags are configured, the rack-aware standby task assignor will be used
     *
     * @param nodeAssignments the current assignment of tasks to nodes
     */
    void defaultStandbyTaskAssignment(final Map<UUID, NodeAssignment> nodeAssignments);

    /**
     * Optimize the active task assignment for rack-awareness
     *
     * @param nodeAssignments the current assignment of tasks to nodes
     * @param tasks the set of tasks to reassign if possible. Must already be assigned to a node
     */
    void optimizeRackAwareActiveTasks(final Map<UUID, NodeAssignment> nodeAssignments, final SortedSet<TaskId> tasks);

    /**
     * Optimize the standby task assignment for rack-awareness
     *
     * @param nodeAssignments the current assignment of tasks to nodes
     */
    void optimizeRackawareStandbyTasks(final Map<UUID, NodeAssignment> nodeAssignments);

}
