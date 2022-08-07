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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TPulsarRLTaskProgress;

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

    public static final long MESSAGEID_BEGINNING_VAL = -2;
    public static final long MESSAGEID_END_VAL = -1;

    private Map<Integer, Long> partitionIdToMessageId = Maps.newConcurrentMap();

    public PulsarProgress() {
        super(LoadDataSourceType.PULSAR);
    }

    public PulsarProgress(TPulsarRLTaskProgress tPulsarRLTaskProgress) {
        super(LoadDataSourceType.PULSAR);
        this.partitionIdToMessageId = tPulsarRLTaskProgress.getPartitionCmtMessageId();
    }

    public Map<Integer, Long> getPartitionIdToMessageId(List<Integer> partitionIds) {
        Map<Integer, Long> result = Maps.newHashMap();
        for (Map.Entry<Integer, Long> entry : partitionIdToMessageId.entrySet()) {
            for (Integer partitionId : partitionIds) {
                if (entry.getKey().equals(partitionId)) {
                    result.put(partitionId, entry.getValue());
                }
            }
        }
        return result;
    }

    public void addPartitionMessageId(Pair<Integer, Long> partitionMessageId) {
        partitionIdToMessageId.put(partitionMessageId.first, partitionMessageId.second);
    }

    public Long getMessageIdByPartition(int pulsarPartition) {
        return partitionIdToMessageId.get(pulsarPartition);
    }

    public boolean containsPartition(Integer pulsarPartition) {
        return partitionIdToMessageId.containsKey(pulsarPartition);
    }

    public boolean hasPartition() {
        return !partitionIdToMessageId.isEmpty();
    }

    private void getReadableProgress(Map<Integer, String> showPartitionIdToMessageId) {
        for (Map.Entry<Integer, Long> entry : partitionIdToMessageId.entrySet()) {
            if (entry.getValue() == 0) {
                showPartitionIdToMessageId.put(entry.getKey(), MESSAGEID_ZERO);
            } else if (entry.getValue() == -1) {
                showPartitionIdToMessageId.put(entry.getKey(), MESSAGEID_END);
            } else if (entry.getValue() == -2) {
                showPartitionIdToMessageId.put(entry.getKey(), MESSAGEID_BEGINNING);
            } else {
                // The offset saved in partitionIdToOffset is the next offset to be consumed.
                // So here we minus 1 to return the "already consumed" offset.
                // TODO：kafka的offset（减一），pulsar的messageId（ledgerId:entryId:partitionIndex）  :batchIndex
                showPartitionIdToMessageId.put(entry.getKey(), "" + (entry.getValue() - 1));
            }
        }
    }

    public void modifyMessageId(List<Pair<Integer, Long>> pulsarPartitionMessageIds) throws DdlException {
        for (Pair<Integer, Long> pair : pulsarPartitionMessageIds) {
            if (!partitionIdToMessageId.containsKey(pair.first)) {
                throw new DdlException("The specified partition " + pair.first + " is not in the consumed partitions");
            }
        }

        for (Pair<Integer, Long> pair : pulsarPartitionMessageIds) {
            partitionIdToMessageId.put(pair.first, pair.second);
        }
    }

    public List<Pair<Integer, String>> getPartitionMessageIdPairs(boolean alreadyConsumed) {
        List<Pair<Integer, String>> pairs = Lists.newArrayList();
        for (Map.Entry<Integer, Long> entry : partitionIdToMessageId.entrySet()) {
            if (entry.getValue() == 0) {
                pairs.add(Pair.create(entry.getKey(), MESSAGEID_ZERO));
            } else if (entry.getValue() == -1) {
                pairs.add(Pair.create(entry.getKey(), MESSAGEID_END));
            } else if (entry.getValue() == -2) {
                pairs.add(Pair.create(entry.getKey(), MESSAGEID_BEGINNING));
            } else {
                long messageId = entry.getValue();
                if (alreadyConsumed) {
                    messageId -= 1;
                }
                pairs.add(Pair.create(entry.getKey(), "" + messageId));
            }
        }
        return pairs;
    }

    public Map<Integer, Long> getLag(Map<Integer, Long> partitionIdWithLatestMessageIds) {
        Map<Integer, Long> lagMap = Maps.newHashMap();
        for (Map.Entry<Integer, Long> entry : partitionIdToMessageId.entrySet()) {
            if (partitionIdWithLatestMessageIds.containsKey(entry.getKey())) {
                long lag = partitionIdWithLatestMessageIds.get(entry.getKey()) - entry.getValue();
                lagMap.put(entry.getKey(), lag);
            } else {
                lagMap.put(entry.getKey(), -1L);
            }
        }
        return lagMap;
    }

    @Override
    public String toString() {
        Map<Integer, String> showPartitionIdToMessageId = Maps.newHashMap();
        getReadableProgress(showPartitionIdToMessageId);
        return "PulsarProgress [partitionIdToMessageId="
                + Joiner.on("|").withKeyValueSeparator("_").join(showPartitionIdToMessageId) + "]";
    }

    @Override
    String toJsonString() {
        Map<Integer, String> showPartitionIdToMessageId = Maps.newHashMap();
        getReadableProgress(showPartitionIdToMessageId);
        Gson gson = new Gson();
        return gson.toJson(showPartitionIdToMessageId);
    }

    @Override
    void update(RLTaskTxnCommitAttachment attachment) {
        PulsarProgress newProgress = (PulsarProgress) attachment.getProgress();
        newProgress.partitionIdToMessageId.entrySet().stream()
                .forEach(entity -> this.partitionIdToMessageId.put(entity.getKey(), entity.getValue() + 1));
        LOG.debug("update pulsar progress: {}, task: {}, job: {}",
                newProgress.toJsonString(), DebugUtil.printId(attachment.getTaskId()), attachment.getJobId());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(partitionIdToMessageId.size());
        for (Map.Entry<Integer, Long> entry : partitionIdToMessageId.entrySet()) {
            out.writeInt((Integer) entry.getKey());
            out.writeLong((Long) entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int size = in.readInt();
        partitionIdToMessageId = new HashMap<>();
        for (int i = 0; i < size; i++) {
            partitionIdToMessageId.put(in.readInt(), in.readLong());
        }
    }
}
