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

import static org.apache.kafka.streams.internals.ApiUtils.validateMillisecondDuration;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.NodeAssignment;

public class NodeAssignmentImpl implements NodeAssignment {

  private final UUID processId;
  private final long now;
  private long followupRebalanceDeadline;

  private final Set<TaskId> activeTasks = new HashSet<>();
  private final Set<TaskId> standbyTasks = new HashSet<>();

  public NodeAssignmentImpl(final UUID processId, final long now) {
    this.processId = processId;
    this.now = now;
  }

  @Override
  public UUID processId() {
    return processId;
  }

  @Override
  public void assignActive(final TaskId task) {
    activeTasks.add(task);
  }

  @Override
  public void assignStandby(final TaskId task) {
    standbyTasks.add(task);
  }

  @Override
  public void removeActive(final TaskId task) {
    activeTasks.remove(task);
  }

  @Override
  public void removeStandby(final TaskId task) {
    standbyTasks.remove(task);
  }

  @Override
  public Set<TaskId> activeAssignment() {
    return activeTasks;
  }

  @Override
  public Set<TaskId> standbyAssignment() {
    return standbyTasks;
  }

  @Override
  public void requestFollowupRebalance(final Duration rebalanceInterval) {
    followupRebalanceDeadline = now + validateMillisecondDuration(rebalanceInterval, "rebalanceInterval");
  }

  @Override
  public long followupRebalanceDeadline() {
    return followupRebalanceDeadline;
  }


}

