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


import org.apache.doris.analysis.AlterRoutineLoadStmt;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.RoutineLoadDataSourceProperties;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.PulsarUtil;
import org.apache.doris.common.util.SmallFileMgr;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PulsarRoutineLoadJob extends RoutineLoadJob {
    private static final Logger LOG = LogManager.getLogger(PulsarRoutineLoadJob.class);

    public static final String PULSAR_FILE_CATALOG = "pulsar";

    private String serviceUrl;
    private String topic;
    private String subscriptionName;
    private List<String> customPulsarPartitions = Lists.newArrayList();
    private List<String> currentPulsarPartitions = Lists.newArrayList();
    private String pulsarDefaultMessageId = "";
    private Map<String, String> customProperties = Maps.newHashMap();
    private Map<String, String> convertedCustomProperties = Maps.newHashMap();
    private Map<String, String> cachedPartitionWithLatestMessageIds = Maps.newConcurrentMap();
    private List<String> newCurrentPulsarPartition = Lists.newArrayList();

    public PulsarRoutineLoadJob() {
        super(-1, LoadDataSourceType.PULSAR);
    }

    public PulsarRoutineLoadJob(Long id, String name, String clusterName,
                               long dbId, long tableId, String serviceUrl, String topic, String subscriptionName,
                               UserIdentity userIdentity) {
        super(id, name, clusterName, dbId, tableId, LoadDataSourceType.PULSAR, userIdentity);
        this.serviceUrl = serviceUrl;
        this.topic = topic;
        this.subscriptionName = subscriptionName;
        this.progress = new PulsarProgress();
    }

    public String getTopic() {
        return topic;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    public Map<String, String> getConvertedCustomProperties() {
        return convertedCustomProperties;
    }

    private String convertedDefaultMessageIdToString() {
        if (this.pulsarDefaultMessageId.isEmpty()) {
            return PulsarProgress.MESSAGEID_END_VAL;
        } else {
            if (this.pulsarDefaultMessageId.equalsIgnoreCase(PulsarProgress.MESSAGEID_BEGINNING)) {
                return PulsarProgress.MESSAGEID_BEGINNING_VAL;
            } else if (this.pulsarDefaultMessageId.equalsIgnoreCase(PulsarProgress.MESSAGEID_END)) {
                return PulsarProgress.MESSAGEID_END_VAL;
            } else {
                return PulsarProgress.MESSAGEID_END_VAL;
            }
        }
    }

    @Override
    public void prepare() throws UserException {
        super.prepare();
        convertCustomProperties(true);
    }

    private void convertCustomProperties(boolean rebuild) throws DdlException {
        if (customProperties.isEmpty()) {
            return;
        }

        if (!rebuild && !convertedCustomProperties.isEmpty()) {
            return;
        }

        if (rebuild) {
            convertedCustomProperties.clear();
        }

        SmallFileMgr smallFileMgr = Env.getCurrentEnv().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                // convert FILE:file_name -> FILE:file_id:md5
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                SmallFileMgr.SmallFile smallFile = smallFileMgr.getSmallFile(dbId, PULSAR_FILE_CATALOG, file, true);
                convertedCustomProperties.put(entry.getKey(), "FILE:" + smallFile.id + ":" + smallFile.md5);
            } else {
                convertedCustomProperties.put(entry.getKey(), entry.getValue());
            }
        }

        if (convertedCustomProperties.containsKey(CreateRoutineLoadStmt.PULSAR_DEFAULT_MESSAGEID)) {
            pulsarDefaultMessageId = convertedCustomProperties.remove(CreateRoutineLoadStmt.PULSAR_DEFAULT_MESSAGEID);
        }
    }

    @Override
    public void divideRoutineLoadJob(int currentConcurrentTaskNum) throws UserException {
        List<RoutineLoadTaskInfo> result = new ArrayList<>();
        writeLock();
        try {
            if (state == JobState.NEED_SCHEDULE) {
                for (int i = 0; i < currentConcurrentTaskNum; i++) {
                    Map<String, String> taskPulsarProgress = Maps.newHashMap();
                    for (int j = i; j < currentPulsarPartitions.size(); j = j + currentConcurrentTaskNum) {
                        String pulsarPartition = currentPulsarPartitions.get(j);
                        taskPulsarProgress.put(pulsarPartition,
                                ((PulsarProgress) progress).getMessageIdByPartition(pulsarPartition));
                    }
                    PulsarTaskInfo pulsarTaskInfo = new PulsarTaskInfo(UUID.randomUUID(), id, clusterName,
                            maxBatchIntervalS * 2 * 1000, taskPulsarProgress);
                    routineLoadTaskInfoList.add(pulsarTaskInfo);
                    result.add(pulsarTaskInfo);
                }
                // change job state to running
                if (result.size() != 0) {
                    unprotectUpdateState(JobState.RUNNING, null, false);
                }
            } else {
                LOG.debug("Ignore to divide routine load job while job state {}", state);
            }

            Env.getCurrentEnv().getRoutineLoadTaskScheduler().addTasksInQueue(result);
        } finally {
            writeUnlock();
        }
    }

    @Override
    public int calculateCurrentConcurrentTaskNum() {
        int partitionNum = currentPulsarPartitions.size();
        if (desireTaskConcurrentNum == 0) {
            desireTaskConcurrentNum = Config.max_routine_load_task_concurrent_num;
        }

        LOG.debug("current concurrent task number is min"
                        + "(partition num: {}, desire task concurrent num: {} config: {})",
                partitionNum, desireTaskConcurrentNum, Config.max_routine_load_task_concurrent_num);
        currentTaskConcurrentNum = Math.min(partitionNum, Math.min(desireTaskConcurrentNum,
                Config.max_routine_load_task_concurrent_num));
        return currentTaskConcurrentNum;
    }

    @Override
    protected boolean checkCommitInfo(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment, TransactionState txnState,
                                      TransactionState.TxnStatusChangeReason txnStatusChangeReason) {
        if (txnState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            return true;
        }
        LOG.debug("no need to update the progress of pulsar routine load. txn status: {}, "
                        + "txnStatusChangeReason: {}, task: {}, job: {}",
                txnState.getTransactionStatus(), txnStatusChangeReason,
                DebugUtil.printId(rlTaskTxnCommitAttachment.getTaskId()), id);
        return false;
    }

    @Override
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) throws UserException {
        super.updateProgress(attachment);
        this.progress.update(attachment);
    }

    @Override
    protected void replayUpdateProgress(RLTaskTxnCommitAttachment attachment) {
        super.replayUpdateProgress(attachment);
        this.progress.update(attachment);
    }

    @Override
    RoutineLoadTaskInfo unprotectRenewTask(RoutineLoadTaskInfo routineLoadTaskInfo) {
        PulsarTaskInfo oldPulsarTaskInfo = (PulsarTaskInfo) routineLoadTaskInfo;
        // add new task
        PulsarTaskInfo pulsarTaskInfo = new PulsarTaskInfo(oldPulsarTaskInfo,
                ((PulsarProgress) progress).getPartitionNameToMessageId(oldPulsarTaskInfo.getPartitions()));
        // remove old task
        routineLoadTaskInfoList.remove(routineLoadTaskInfo);
        // add new task
        routineLoadTaskInfoList.add(pulsarTaskInfo);
        return pulsarTaskInfo;
    }

    @Override
    protected void unprotectUpdateProgress() throws UserException {
        updateNewPartitionProgress();
    }

    @Override
    protected void preCheckNeedSchedule() throws UserException {
        if (this.state == JobState.RUNNING || this.state == JobState.NEED_SCHEDULE) {
            if (customPulsarPartitions != null && !customPulsarPartitions.isEmpty()) {
                return;
            }
            updatePulsarPartitions();
        }
    }

    private void updatePulsarPartitions() throws UserException {
        try {
            this.newCurrentPulsarPartition = getAllPulsarPartitions();
        } catch (Exception e) {
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                    .add("error_msg", "Job failed to fetch all current partition with error " + e.getMessage())
                    .build(), e);
            if (this.state == JobState.NEED_SCHEDULE) {
                unprotectUpdateState(JobState.PAUSED,
                        new ErrorReason(InternalErrorCode.PARTITIONS_ERR,
                                "Job failed to fetch all current partition with error " + e.getMessage()),
                        false /* not replay */);
            }
        }
    }

    @Override
    protected boolean unprotectNeedReschedule() throws UserException {
        if (this.state == JobState.RUNNING || this.state == JobState.NEED_SCHEDULE) {
            if (customPulsarPartitions != null && customPulsarPartitions.size() != 0) {
                currentPulsarPartitions = customPulsarPartitions;
                return false;
            } else {
                Preconditions.checkNotNull(this.newCurrentPulsarPartition);
                if (currentPulsarPartitions.containsAll(this.newCurrentPulsarPartition)) {
                    if (currentPulsarPartitions.size() > this.newCurrentPulsarPartition.size()) {
                        currentPulsarPartitions = this.newCurrentPulsarPartition;
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                    .add("current_pulsar_partitions", Joiner.on(",").join(currentPulsarPartitions))
                                    .add("msg", "current pulsar partitions has been change")
                                    .build());
                        }
                        return true;
                    } else {
                        for (String pulsarPartition : currentPulsarPartitions) {
                            if (!((PulsarProgress) progress).containsPartition(pulsarPartition)) {
                                return true;
                            }
                        }
                        return false;
                    }
                } else {
                    currentPulsarPartitions = this.newCurrentPulsarPartition;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                .add("current_pulsar_partitions", Joiner.on(",").join(currentPulsarPartitions))
                                .add("msg", "current pulsar partitions has been change")
                                .build());
                    }
                    return true;
                }
            }
        } else if (this.state == JobState.PAUSED) {
            return ScheduleRule.isNeedAutoSchedule(this);
        } else {
            return false;
        }
    }

    @Override
    protected String getStatistic() {
        Map<String, Object> summary = this.jobStatistic.summary();
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(summary);
    }

    private List<String> getAllPulsarPartitions() throws UserException {
        convertCustomProperties(false);
        return PulsarUtil.getAllPulsarPartitions(serviceUrl, topic, subscriptionName, convertedCustomProperties);
    }

    public static PulsarRoutineLoadJob fromCreateStmt(CreateRoutineLoadStmt stmt) throws UserException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(stmt.getDBName());
        OlapTable olapTable = db.getOlapTableOrDdlException(stmt.getTableName());
        checkMeta(olapTable, stmt.getRoutineLoadDesc());
        long tableId = olapTable.getId();

        long id = Env.getCurrentEnv().getNextId();
        PulsarRoutineLoadJob pulsarRoutineLoadJob = new PulsarRoutineLoadJob(id, stmt.getName(),
                db.getClusterName(), db.getId(), tableId,
                stmt.getPulsarServiceUrl(), stmt.getPulsarTopic(),
                stmt.getPulsarSubscriptionName(), stmt.getUserInfo());
        pulsarRoutineLoadJob.setOptional(stmt);
        pulsarRoutineLoadJob.checkCustomProperties();
        pulsarRoutineLoadJob.checkCustomPartition();

        return pulsarRoutineLoadJob;
    }

    private void checkCustomPartition() throws UserException {
        if (customPulsarPartitions.isEmpty()) {
            return;
        }
        List<String> allPulsarPartitions = getAllPulsarPartitions();
        for (String allPulsarPartition : allPulsarPartitions) {
            LOG.info(allPulsarPartition);
        }
        for (String customPartition : customPulsarPartitions) {
            LOG.info(customPartition);
            if (!allPulsarPartitions.contains(customPartition)) {
                throw new LoadException("there is a custom pulsar partition " + customPartition
                        + " which is invalid for topic " + topic);
            }
        }
    }

    private void checkCustomProperties() throws DdlException {
        SmallFileMgr smallFileMgr = Env.getCurrentEnv().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                if (!smallFileMgr.containsFile(dbId, PULSAR_FILE_CATALOG, file)) {
                    throw new DdlException("File " + file + " does not exist in db "
                            + dbId + " with catalog: " + PULSAR_FILE_CATALOG);
                }
            }
        }
    }

    private void updateNewPartitionProgress() throws UserException {
        try {
            for (String pulsarPartition : currentPulsarPartitions) {
                if (!((PulsarProgress) progress).containsPartition(pulsarPartition)) {
                    List<String> newPartitions = Lists.newArrayList();
                    newPartitions.add(pulsarPartition);
                    List<Pair<String, String>> newPartitionsMessageIds
                            = getNewPartitionMessageIdsFromDefaultMessageId(newPartitions);
                    Preconditions.checkState(newPartitionsMessageIds.size() == 1);
                    for (Pair<String, String> partitionMessageId : newPartitionsMessageIds) {
                        ((PulsarProgress) progress).addPartitionMessageId(partitionMessageId);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                    .add("pulsar_partition_id", partitionMessageId.first)
                                    .add("begin_messageid", partitionMessageId.second)
                                    .add("msg", "The new partition has been added in job"));
                        }
                    }
                }
            }
        } catch (UserException e) {
            unprotectUpdateState(JobState.PAUSED,
                    new ErrorReason(InternalErrorCode.PARTITIONS_ERR, e.getMessage()), false /* not replay */);
            throw e;
        }
    }

    private List<Pair<String, String>> getNewPartitionMessageIdsFromDefaultMessageId(List<String> newPartitions)
            throws UserException {
        List<Pair<String, String>> partitionMessageIds = Lists.newArrayList();
        String beginMessageId = convertedDefaultMessageIdToString();
        for (String pulsarPartition : newPartitions) {
            partitionMessageIds.add(Pair.create(pulsarPartition, beginMessageId));
        }
        return partitionMessageIds;
    }

    @Override
    protected void setOptional(CreateRoutineLoadStmt stmt) throws UserException {
        super.setOptional(stmt);

        if (!stmt.getPulsarPartitionMessageIds().isEmpty()) {
            setCustomPulsarPartitions(stmt);
        }
        if (!stmt.getCustomPulsarProperties().isEmpty()) {
            setCustomPulsarProperties(stmt.getCustomPulsarProperties());
        }
    }

    private void setCustomPulsarPartitions(CreateRoutineLoadStmt stmt) throws LoadException {
        List<Pair<String, String>> pulsarPartitionMessageIds = stmt.getPulsarPartitionMessageIds();
        for (Pair<String, String> partitionMessageId : pulsarPartitionMessageIds) {
            this.customPulsarPartitions.add(partitionMessageId.first);
            ((PulsarProgress) progress).addPartitionMessageId(partitionMessageId);
        }
    }

    private void setCustomPulsarProperties(Map<String, String> pulsarProperties) {
        this.customProperties = pulsarProperties;
    }

    @Override
    String dataSourcePropertiesJsonToString() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("serviceUrl", serviceUrl);
        dataSourceProperties.put("topic", topic);
        dataSourceProperties.put("subscriptionName", subscriptionName);
        List<String> sortedPartitions = Lists.newArrayList(currentPulsarPartitions);
        Collections.sort(sortedPartitions);
        dataSourceProperties.put("currentPulsarPartitions", Joiner.on(",").join(sortedPartitions));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(dataSourceProperties);
    }

    @Override
    String customPropertiesJsonToString() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(customProperties);
    }

    @Override
    Map<String, String> getDataSourceProperties() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("pulsar_service_url", serviceUrl);
        dataSourceProperties.put("pulsar_topic", topic);
        dataSourceProperties.put("pulsar_subscription_name", subscriptionName);
        return dataSourceProperties;
    }

    @Override
    Map<String, String> getCustomProperties() {
        Map<String, String> ret = new HashMap<>();
        customProperties.forEach((k, v) -> ret.put("property." + k, v));
        return ret;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, serviceUrl);
        Text.writeString(out, topic);
        Text.writeString(out, subscriptionName);

        out.writeInt(customPulsarPartitions.size());
        for (String partitionName : customPulsarPartitions) {
            Text.writeString(out, partitionName);
        }

        out.writeInt(customProperties.size());
        for (Map.Entry<String, String> property : customProperties.entrySet()) {
            Text.writeString(out, "property." + property.getKey());
            Text.writeString(out, property.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        serviceUrl = Text.readString(in);
        topic = Text.readString(in);
        subscriptionName = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            customPulsarPartitions.add(Text.readString(in));
        }

        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String propertyKey = Text.readString(in);
            String propertyValue = Text.readString(in);
            if (propertyKey.startsWith("property.")) {
                this.customProperties.put(propertyKey.substring(propertyKey.indexOf(".") + 1), propertyValue);
            }
        }
    }

    @Override
    public void modifyProperties(AlterRoutineLoadStmt stmt) throws UserException {
        Map<String, String> jobProperties = stmt.getAnalyzedJobProperties();
        RoutineLoadDataSourceProperties dataSourceProperties = stmt.getDataSourceProperties();
        writeLock();
        try {
            if (getState() != JobState.PAUSED) {
                throw new DdlException("Only supports modification of PAUSED jobs");
            }

            modifyPropertiesInternal(jobProperties, dataSourceProperties);

            AlterRoutineLoadJobOperationLog log = new AlterRoutineLoadJobOperationLog(this.id,
                    jobProperties, dataSourceProperties);
            Env.getCurrentEnv().getEditLog().logAlterRoutineLoadJob(log);
        } finally {
            writeUnlock();
        }
    }

    private void modifyPropertiesInternal(Map<String, String> jobProperties,
                                          RoutineLoadDataSourceProperties dataSourceProperties)
            throws DdlException {
        List<Pair<String, String>> pulsarPartitionMessageIds = Lists.newArrayList();
        Map<String, String> customPulsarProperties = Maps.newHashMap();

        if (dataSourceProperties.hasAnalyzedProperties()) {
            pulsarPartitionMessageIds = dataSourceProperties.getPulsarPartitionMessageIds();
            customPulsarProperties = dataSourceProperties.getCustomPulsarProperties();
        }

        if (!pulsarPartitionMessageIds.isEmpty()) {
            ((PulsarProgress) progress).modifyMessageId(pulsarPartitionMessageIds);
        }

        if (!customPulsarProperties.isEmpty()) {
            this.customProperties.putAll(customPulsarProperties);
            convertCustomProperties(true);
        }

        if (!jobProperties.isEmpty()) {
            Map<String, String> copiedJobProperties = Maps.newHashMap(jobProperties);
            modifyCommonJobProperties(copiedJobProperties);
            this.jobProperties.putAll(copiedJobProperties);
        }

        if (!Strings.isNullOrEmpty(dataSourceProperties.getPulsarServiceUrl())) {
            this.serviceUrl = dataSourceProperties.getPulsarServiceUrl();
        }
        if (!Strings.isNullOrEmpty(dataSourceProperties.getPulsarTopic())) {
            this.topic = dataSourceProperties.getPulsarTopic();
        }
        if (!Strings.isNullOrEmpty(dataSourceProperties.getPulsarSubscriptionName())) {
            this.subscriptionName = dataSourceProperties.getPulsarSubscriptionName();
        }

        LOG.info("modify the properties of pulsar routine load job: {}, jobProperties: {}, datasource properties: {}",
                this.id, jobProperties, dataSourceProperties);
    }

    @Override
    public void replayModifyProperties(AlterRoutineLoadJobOperationLog log) {
        try {
            modifyPropertiesInternal(log.getJobProperties(), log.getDataSourceProperties());
        } catch (DdlException e) {
            LOG.error("failed to replay modify pulsar routine load job: {}", id, e);
        }
    }

    public boolean hasMoreDataToConsume(UUID taskId, Map<String, String> partitionNameToMessageId) {
        for (Map.Entry<String, String> entry : partitionNameToMessageId.entrySet()) {
            if (cachedPartitionWithLatestMessageIds.containsKey(entry.getKey())
                    /*&& entry.getValue() < cachedPartitionWithLatestMessageIds.get(entry.getKey())*/) {
                LOG.debug("has more data to consume. messageId to be consumed: {}"
                                + ", latest messageId: {}, task {}, job {}",
                        partitionNameToMessageId, cachedPartitionWithLatestMessageIds, taskId, id);
                return true;
            }
        }

        try {
            List<Pair<String, String>> tmp = PulsarUtil.getLatestmessageId(id, taskId, getServiceUrl(), getTopic(),
                    getSubscriptionName(), getConvertedCustomProperties(),
                    Lists.newArrayList(partitionNameToMessageId.keySet()));
            for (Pair<String, String> pair : tmp) {
                cachedPartitionWithLatestMessageIds.put(pair.first, pair.second);
            }
        } catch (Exception e) {
            LOG.warn("failed to get latest partition messageId. {}", e.getMessage(), e);
            return false;
        }

        for (Map.Entry<String, String> entry : partitionNameToMessageId.entrySet()) {
            if (cachedPartitionWithLatestMessageIds.containsKey(entry.getKey())
                    /*&& entry.getValue() < cachedPartitionWithLatestMessageIds.get(entry.getKey())*/) {
                LOG.debug("has more data to consume. messageId to be consumed: {}"
                                + ", latest messageId: {}, task {}, job {}",
                        partitionNameToMessageId, cachedPartitionWithLatestMessageIds, taskId, id);
                return true;
            }
        }

        LOG.debug("no more data to consume. messageId to be consumed: {}, latest messageId: {}, task {}, job {}",
                partitionNameToMessageId, cachedPartitionWithLatestMessageIds, taskId, id);
        return false;
    }

    @Override
    protected String getLag() {
        Map<String, String> partitionNameToMessageIdLag = ((PulsarProgress) progress)
                .getLag(cachedPartitionWithLatestMessageIds);
        Gson gson = new Gson();
        return gson.toJson(partitionNameToMessageIdLag);
    }

    @Override
    public double getMaxFilterRatio() {
        return 1.0;
    }
}
