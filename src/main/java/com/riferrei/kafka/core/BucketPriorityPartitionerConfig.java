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

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class BucketPriorityPartitionerConfig extends AbstractConfig {

    public BucketPriorityPartitionerConfig(Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    public String topic() {
        return getString(TOPIC_CONFIG);
    }

    public String delimiter() {
        return getString(DELIMITER_CONFIG);
    }

    public List<String> buckets() {
        return getList(BUCKETS_CONFIG);
    }

    public List<String> allocation() {
        return getList(ALLOCATION_CONFIG);
    }

    public FallbackAction fallbackAction() {
        String value = getString(FALLBACK_ACTION_CONFIG);
        return FallbackAction.valueOf(value.toUpperCase());
    }

    private static final ConfigDef CONFIG;
    
    public static final String TOPIC_CONFIG = "bucket.priority.topic";
    public static final String TOPIC_CONFIG_DOC = "Which topic should have its partitions mapped to buckets.";
    public static final String BUCKETS_CONFIG = "bucket.priority.buckets";
    public static final String BUCKETS_CONFIG_DOC = "List of the bucket names.";
    public static final String DELIMITER_CONFIG = "bucket.priority.delimiter";
    public static final String DELIMITER_CONFIG_DOC = "Delimiter used to look up the bucket name in the key.";
    public static final String DELIMITER_CONFIG_DEFAULT = "-";
    public static final String ALLOCATION_CONFIG = "bucket.priority.allocation";
    public static final String ALLOCATION_CONFIG_DOC = "Allocation in percentage for each bucket.";
    public static final String FALLBACK_ACTION_CONFIG = "bucket.priority.fallback.action";
    public static final String FALLBACK_ACTION_CONFIG_DOC = "What to do when there is no bucket information.";
    public static final String FALLBACK_ACTION_CONFIG_DEFAULT = FallbackAction.DEFAULT.name();

    public enum FallbackAction {
        DEFAULT, ROUNDROBIN, DISCARD
    }

    static {
        CONFIG = new ConfigDef()
            .define(TOPIC_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                TOPIC_CONFIG_DOC)
            .define(DELIMITER_CONFIG,
                ConfigDef.Type.STRING,
                DELIMITER_CONFIG_DEFAULT,
                ConfigDef.Importance.LOW,
                DELIMITER_CONFIG_DOC)
            .define(BUCKETS_CONFIG,
                ConfigDef.Type.LIST,
                ConfigDef.Importance.HIGH,
                BUCKETS_CONFIG_DOC)
            .define(ALLOCATION_CONFIG,
                ConfigDef.Type.LIST,
                ConfigDef.Importance.HIGH,
                ALLOCATION_CONFIG_DOC)
            .define(FALLBACK_ACTION_CONFIG,
                ConfigDef.Type.STRING,
                FALLBACK_ACTION_CONFIG_DEFAULT,
                ConfigDef.Importance.LOW,
                FALLBACK_ACTION_CONFIG_DOC);
    }

}
