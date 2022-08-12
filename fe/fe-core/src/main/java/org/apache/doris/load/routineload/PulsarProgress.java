// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.load.routineload;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TPulsarRLTaskProgress;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PulsarProgress extends RoutineLoadProgress {
    private static final Logger LOG = LogManager.getLogger(PulsarProgress.class);

    public static final String MESSAGEID_BEGINNING = "MESSAGEID_BEGINNING";
    public static final String MESSAGEID_END = "MESSAGEID_END";
    public static final String MESSAGEID_ZERO = "MESSAGEID_ZERO";

    public static final String MESSAGEID_BEGINNING_VAL = "earliest";
    public static final String MESSAGEID_END_VAL = "latest";

    private Map<String, String> partitionNameToMessageId = Maps.newConcurrentMap();

    public PulsarProgress() {
        super(LoadDataSourceType.PULSAR);
    }

    public PulsarProgress(TPulsarRLTaskProgress tPulsarRLTaskProgress) {
        super(LoadDataSourceType.PULSAR);
        this.partitionNameToMessageId = tPulsarRLTaskProgress.getPartitionCmtMessageId();
    }

    public Map<String, String> getPartitionNameToMessageId(List<String> partitionNames) {
        Map<String, String> result = Maps.newHashMap();
        for (Map.Entry<String, String> entry : partitionNameToMessageId.entrySet()) {
            for (String partitionName : partitionNames) {
                if (entry.getKey().equals(partitionName)) {
                    result.put(partitionName, entry.getValue());
                }
            }
        }
        return result;
    }

    public void addPartitionMessageId(Pair<String, String> partitionMessageId) {
        partitionNameToMessageId.put(partitionMessageId.first, partitionMessageId.second);
    }

    public String getMessageIdByPartition(String pulsarPartition) {
        return partitionNameToMessageId.get(pulsarPartition);
    }

    public boolean containsPartition(String pulsarPartition) {
        return partitionNameToMessageId.containsKey(pulsarPartition);
    }

    public boolean hasPartition() {
        return !partitionNameToMessageId.isEmpty();
    }

    private void getReadableProgress(Map<String, String> showPartitionNameToMessageId) {
        for (Map.Entry<String, String> entry : partitionNameToMessageId.entrySet()) {
            // TODO:
            if (entry.getValue().equals("0")) {
                showPartitionNameToMessageId.put(entry.getKey(), MESSAGEID_ZERO);
            } else if (entry.getValue().equals("-1")) {
                showPartitionNameToMessageId.put(entry.getKey(), MESSAGEID_END);
            } else if (entry.getValue().equals("-2")) {
                showPartitionNameToMessageId.put(entry.getKey(), MESSAGEID_BEGINNING);
            } else {
                // TODO：kafka的offset（减一），pulsar的messageId（ledgerId:entryId:partitionIndex）  :batchIndex
                showPartitionNameToMessageId.put(entry.getKey(), "" + (Long.parseLong(entry.getValue()) - 1));
            }
        }
    }

    public void modifyMessageId(List<Pair<String, String>> pulsarPartitionMessageIds) throws DdlException {
        for (Pair<String, String> pair : pulsarPartitionMessageIds) {
            if (!partitionNameToMessageId.containsKey(pair.first)) {
                throw new DdlException("The specified partition " + pair.first + " is not in the consumed partitions");
            }
        }

        for (Pair<String, String> pair : pulsarPartitionMessageIds) {
            partitionNameToMessageId.put(pair.first, pair.second);
        }
    }

    public List<Pair<String, String>> getPartitionMessageIdPairs(boolean alreadyConsumed) {
        List<Pair<String, String>> pairs = Lists.newArrayList();
        for (Map.Entry<String, String> entry : partitionNameToMessageId.entrySet()) {
            if (entry.getValue().equals("0")) {
                pairs.add(Pair.create(entry.getKey(), MESSAGEID_ZERO));
            } else if (entry.getValue().equals("-1")) {
                pairs.add(Pair.create(entry.getKey(), MESSAGEID_END));
            } else if (entry.getValue().equals("-2")) {
                pairs.add(Pair.create(entry.getKey(), MESSAGEID_BEGINNING));
            } else {
                long messageId = Long.parseLong(entry.getValue());
                if (alreadyConsumed) {
                    messageId -= 1;
                }
                pairs.add(Pair.create(entry.getKey(), "" + messageId));
            }
        }
        return pairs;
    }

    public Map<String, String> getLag(Map<String, String> partitionNameWithLatestMessageIds) {
        Map<String, String> lagMap = Maps.newHashMap();
        for (Map.Entry<String, String> entry : partitionNameToMessageId.entrySet()) {
            if (partitionNameWithLatestMessageIds.containsKey(entry.getKey())) {
                long lag = Long.parseLong(partitionNameWithLatestMessageIds.get(entry.getKey()))
                        - Long.parseLong(entry.getValue());
                lagMap.put(entry.getKey(), String.valueOf(lag));
            } else {
                lagMap.put(entry.getKey(), "-1");
            }
        }
        return lagMap;
    }

    @Override
    public String toString() {
        Map<String, String> showPartitionNameToMessageId = Maps.newHashMap();
        getReadableProgress(showPartitionNameToMessageId);
        return "PulsarProgress [partitionNameToMessageId="
                + Joiner.on("|").withKeyValueSeparator("_").join(showPartitionNameToMessageId) + "]";
    }

    @Override
    String toJsonString() {
        Map<String, String> showPartitionNameToMessageId = Maps.newHashMap();
        getReadableProgress(showPartitionNameToMessageId);
        Gson gson = new Gson();
        return gson.toJson(showPartitionNameToMessageId);
    }

    @Override
    void update(RLTaskTxnCommitAttachment attachment) {
        PulsarProgress newProgress = (PulsarProgress) attachment.getProgress();
        newProgress.partitionNameToMessageId.entrySet().stream()
                .forEach(entity -> this.partitionNameToMessageId.put(entity.getKey(), entity.getValue() + 1));
        LOG.debug("update pulsar progress: {}, task: {}, job: {}",
                newProgress.toJsonString(), DebugUtil.printId(attachment.getTaskId()), attachment.getJobId());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(partitionNameToMessageId.size());
        for (Map.Entry<String, String> entry : partitionNameToMessageId.entrySet()) {
            out.writeBytes(entry.getKey());
            out.writeBytes(entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int size = in.readInt();
        partitionNameToMessageId = new HashMap<>();
        for (int i = 0; i < size; i++) {
            partitionNameToMessageId.put(String.valueOf(in.readByte()), String.valueOf(in.readByte()));
        }
    }
}
