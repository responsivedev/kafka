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

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StreamThread;

public interface NodeAssignment {

  UUID processId();

  void assignActive(final TaskId task);

  void assignStandby(final TaskId task);

  void removeActive(final TaskId task);

  void removeStandby(final TaskId task);

  Set<TaskId> activeAssignment();

  Set<TaskId> activeStatefulAssignment();

  Set<TaskId> activeStatelessAssignment();

  Set<TaskId> standbyAssignment();

  /**
   * Request a followup rebalance to be triggered by one of the consumers on this node after the
   * given interval has elapsed. The {@link StreamThread} selected for this will be chosen at random.
   * Only a single StreamThread consumer will be informed when a followup rebalance is requested, and
   * if invoked multiple times the rebalance will be scheduled according to the shortest delay/nearest
   * deadline.
   * <p>
   * NOTE: A best effort will be made to enforce a rebalance according to the requested schedule,
   * but there is no guarantee that another rebalance will not occur before this time has elapsed.
   * Similarly, there is no guarantee the followup rebalance will occur, and must be re-requested
   * if, for example, the requesting consumer crashes or drops out of the group. Such an event
   * is, however, guaranteed to trigger a new rebalance itself, at which point the assignor
   * can re-evaluate whether to request an additional rebalance or not.
   *
   * @param followupRebalanceDelay how long this node should wait before initiating a new rebalance
   */
  void requestFollowupRebalance(final Duration followupRebalanceDelay);

  /**
   * @return the actual deadline in objective time, using ms since the epoch, after which the
   * followup rebalance will be attempted. Equivalent to {@code 'now + followupRebalanceDelay'}
   */
  long followupRebalanceDeadline();

}
