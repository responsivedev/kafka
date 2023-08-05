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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.function.Function;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ApplicationMetadata;
import org.apache.kafka.streams.processor.assignment.NodeAssignment;
import org.apache.kafka.streams.processor.assignment.NodeState;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;

public class ApplicationMetadataImpl implements ApplicationMetadata {

    private final Map<UUID, NodeStateImpl> nodeStates;
    private final Set<TaskId> allTasks;
    private final Set<TaskId> statefulTasks;
    private final AssignmentConfigs assignmentConfigs;

    private final Function<Collection<NodeStateImpl>, Boolean> computeTaskLags;
    private final RackAwareTaskAssignor rackAwareTaskAssignor;
    private final StandbyTaskAssignor standbyTaskAssignor;

    public ApplicationMetadataImpl(final Map<UUID, NodeStateImpl> nodeStates,
                                   final Set<TaskId> allTaskIds,
                                   final Set<TaskId> statefulTaskIds,
                                   final AssignmentConfigs assignmentConfigs,
                                   final RackAwareTaskAssignor rackAwareTaskAssignor,
                                   final Function<Collection<NodeStateImpl>, Boolean> computeTaskLags) {
        this.nodeStates = nodeStates;
        this.allTasks = allTaskIds;
        this.statefulTasks = statefulTaskIds;
        this.assignmentConfigs = assignmentConfigs;
        this.rackAwareTaskAssignor = rackAwareTaskAssignor;
        this.computeTaskLags = computeTaskLags;
        this.standbyTaskAssignor = StandbyTaskAssignorFactory.create(assignmentConfigs, rackAwareTaskAssignor);
    }

    @Override
    public Map<UUID, ? extends NodeState> nodeStates() {
        return nodeStates;
    }

    @Override
    public boolean computeTaskLags() {
        return computeTaskLags.apply(nodeStates.values());
    }

    @Override
    public AssignmentConfigs assignmentConfigs() {
        return assignmentConfigs;
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
        standbyTaskAssignor.assign(nodeAssignments, allTasks(), statefulTasks(), assignmentConfigs);
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
            assignmentConfigs.trafficCost(),
            assignmentConfigs.nonOverlapCost()
        );
    }

    @Override
    public void optimizeRackAwareStandbyTasks(final Map<UUID, NodeAssignment> nodeAssignments) {
        rackAwareTaskAssignor.optimizeStandbyTasks(
            nodeAssignments,
            assignmentConfigs.trafficCost(),
            assignmentConfigs.nonOverlapCost(),
            standbyTaskAssignor::isAllowedTaskMovement
        );
    }
}
