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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;

public class AssignmentConfigs {
  private final long acceptableRecoveryLag;
  private final int maxWarmupReplicas;
  private final int numStandbyReplicas;
  private final long probingRebalanceIntervalMs;
  private final List<String> rackAwareAssignmentStandbyTags;
  private final int trafficCost;
  private final int nonOverlapCost;

  public AssignmentConfigs(final StreamsConfig configs) {
    acceptableRecoveryLag = configs.getLong(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG);
    maxWarmupReplicas = configs.getInt(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG);
    numStandbyReplicas = configs.getInt(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG);
    probingRebalanceIntervalMs = configs.getLong(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG);
    rackAwareAssignmentStandbyTags = configs.getList(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG);
    trafficCost = configs.getInt(StreamsConfig.RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG);
    nonOverlapCost = configs.getInt(StreamsConfig.RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG);
  }

  public AssignmentConfigs(final Long acceptableRecoveryLag,
                           final Integer maxWarmupReplicas,
                           final Integer numStandbyReplicas,
                           final Long probingRebalanceIntervalMs,
                           final List<String> rackAwareAssignmentStandbyTags,
                           final int trafficCost,
                           final int nonOverlapCost) {
    this.acceptableRecoveryLag = validated(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, acceptableRecoveryLag);
    this.maxWarmupReplicas = validated(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, maxWarmupReplicas);
    this.numStandbyReplicas = validated(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, numStandbyReplicas);
    this.probingRebalanceIntervalMs = validated(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, probingRebalanceIntervalMs);
    this.rackAwareAssignmentStandbyTags = validated(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, rackAwareAssignmentStandbyTags);
    this.trafficCost = validated(StreamsConfig.RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG, trafficCost);
    this.nonOverlapCost = validated(StreamsConfig.RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG, nonOverlapCost);
  }

  public long acceptableRecoveryLag() {
    return acceptableRecoveryLag;
  }

  public int maxWarmupReplicas() {
    return maxWarmupReplicas;
  }

  public int numStandbyReplicas() {
    return numStandbyReplicas;
  }

  public long probingRebalanceIntervalMs() {
    return probingRebalanceIntervalMs;
  }

  public List<String> rackAwareAssignmentStandbyTags() {
    return rackAwareAssignmentStandbyTags;
  }

  public int trafficCost() {
    return trafficCost;
  }

  public int nonOverlapCost() {
    return nonOverlapCost;
  }

  private static <T> T validated(final String configKey, final T value) {
    final ConfigDef.Validator validator = StreamsConfig.configDef().configKeys().get(configKey).validator;
    if (validator != null) {
      validator.ensureValid(configKey, value);
    }
    return value;
  }

  @Override
  public String toString() {
    return "AssignmentConfigs{" +
        "\n  acceptableRecoveryLag=" + acceptableRecoveryLag +
        "\n  maxWarmupReplicas=" + maxWarmupReplicas +
        "\n  numStandbyReplicas=" + numStandbyReplicas +
        "\n  probingRebalanceIntervalMs=" + probingRebalanceIntervalMs +
        "\n  rackAwareAssignmentStandbyTags=" + rackAwareAssignmentStandbyTags +
        "\n  trafficCost=" + trafficCost +
        "\n  nonOverlapCost=" + nonOverlapCost +
        "\n}";
  }
}