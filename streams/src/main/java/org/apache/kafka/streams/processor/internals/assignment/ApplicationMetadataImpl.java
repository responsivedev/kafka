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

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ApplicationMetadata;
import org.apache.kafka.streams.processor.assignment.NodeAssignment;
import org.apache.kafka.streams.processor.assignment.NodeState;
import org.apache.kafka.streams.processor.assignment.TaskAssignorConfigs;

public class ApplicationMetadataImpl implements ApplicationMetadata {

    private final Map<UUID, NodeState> nodeStates;
    private final Set<TaskId> allTasks;
    private final Set<TaskId> statefulTasks;
    private final TaskAssignorConfigs taskAssignorConfigs;

    private final Admin adminClient;
    private final RackAwareTaskAssignor rackAwareTaskAssignor;
    private final StandbyTaskAssignor standbyTaskAssignor;

    public ApplicationMetadataImpl(final Map<UUID, NodeState> nodeStates,
                                   final Set<TaskId> allTaskIds,
                                   final Set<TaskId> statefulTaskIds,
                                   final TaskAssignorConfigs taskAssignorConfigs,
                                   final RackAwareTaskAssignor rackAwareTaskAssignor,
                                   final Admin adminClient) {
        this.nodeStates = nodeStates;
        this.allTasks = allTaskIds;
        this.statefulTasks = statefulTaskIds;
        this.taskAssignorConfigs = taskAssignorConfigs;
        this.adminClient = adminClient;
        this.rackAwareTaskAssignor = rackAwareTaskAssignor;
        this.standbyTaskAssignor = StandbyTaskAssignorFactory.create(taskAssignorConfigs, rackAwareTaskAssignor);
    }

    @Override
    public Map<UUID, NodeState> nodeStates(final boolean computeTaskLags) {
        if (computeTaskLags) {
            // TODO(KIP-924): extract end offset fetch and lag computation to here
        }
        return nodeStates;
    }

    @Override
    public TaskAssignorConfigs taskAssignorConfigs() {
        return taskAssignorConfigs;
    }

    @Override
    public Set<TaskId> allTasks() {
        return allTasks;
    }

    @Override
    public Set<TaskId> statefulTasks() {
        return statefulTasks;
    }


    @Override
    public void defaultStandbyTaskAssignment(final Map<UUID, NodeAssignment> nodeAssignments) {
        standbyTaskAssignor.assign(nodeAssignments, allTasks(), statefulTasks(), taskAssignorConfigs);
    }

    /**
     * @param nodeAssignments the initial assignment of tasks to nodes
     * @param tasks the set of tasks to reassign if possible. Must already be assigned to a node
     */
    @Override
    public void optimizeRackAwareActiveTasks(final Map<UUID, NodeAssignment> nodeAssignments,
                                             final SortedSet<TaskId> tasks) {
        rackAwareTaskAssignor.optimizeActiveTasks(
            tasks,
            nodeAssignments,
            taskAssignorConfigs.trafficCost,
            taskAssignorConfigs.nonOverlapCost
        );
    }

    @Override
    public void optimizeRackawareStandbyTasks(final Map<UUID, NodeAssignment> nodeAssignments) {
        rackAwareTaskAssignor.optimizeStandbyTasks(
            nodeAssignments,
            taskAssignorConfigs.trafficCost,
            taskAssignorConfigs.nonOverlapCost,
            standbyTaskAssignor::isAllowedTaskMovement
        );
    }
}
