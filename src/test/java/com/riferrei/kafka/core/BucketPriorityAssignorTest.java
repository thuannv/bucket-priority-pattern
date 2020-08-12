/**

    Copyright © 2020 Ricardo Ferreira (riferrei@riferrei.com)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

*/

package com.riferrei.kafka.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BucketPriorityAssignorTest {

    @Test
    public void checkMissingConfiguration() {
        final Map<String, String> configs = new HashMap<>();
        final BucketPriorityAssignor assignor = new BucketPriorityAssignor();
        // Check if the topic configuration is missing
        assertThrows(ConfigException.class, () -> {
            assignor.configure(configs);
        });
        // Check if the buckets configuration is missing
        assertThrows(ConfigException.class, () -> {
            configs.put(BucketPriorityConfig.TOPIC_CONFIG, "test");
            assignor.configure(configs);
        });
        // Check if the allocation configuration is missing
        assertThrows(ConfigException.class, () -> {
            configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
            assignor.configure(configs);
        });
        // Check if complete configuration is gonna be enough
        assertDoesNotThrow(() -> {
            configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70, 30");
            assignor.configure(configs);
        });
    }

    @Test
    public void checkMatchingBucketConfiguration() {
        final Map<String, String> configs = new HashMap<>();
        final BucketPriorityAssignor assignor = new BucketPriorityAssignor();
        assertThrows(InvalidConfigurationException.class, () -> {
            configs.put(BucketPriorityConfig.TOPIC_CONFIG, "test");
            configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
            configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70");
            assignor.configure(configs);
        });
        assertDoesNotThrow(() -> {
            configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70, 30");
            assignor.configure(configs);
        });
    }

    @Test
    public void checkAllocationPercentageConfiguration() {
        final Map<String, String> configs = new HashMap<>();
        final BucketPriorityAssignor assignor = new BucketPriorityAssignor();
        assertThrows(InvalidConfigurationException.class, () -> {
            configs.put(BucketPriorityConfig.TOPIC_CONFIG, "test");
            configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
            configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70, 20");
            assignor.configure(configs);
        });
        assertDoesNotThrow(() -> {
            configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70, 30");
            assignor.configure(configs);
        });
    }
    
}
