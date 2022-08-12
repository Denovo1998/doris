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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.ImportSequenceStmt;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.ParseNode;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.RoutineLoadDataSourceProperties;
import org.apache.doris.analysis.Separator;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.KafkaUtil;
import org.apache.doris.common.util.PulsarUtil;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PulsarRoutineLoadJobTest {
    private static final Logger LOG = LogManager.getLogger(PulsarRoutineLoadJob.class);

    private String jobName = "job1";
    private String dbName = "db1";
    private LabelName labelName = new LabelName(dbName, jobName);
    private String tableNameString = "table1";
    private String topicName = "persistent://my_tenant/my_namespace/routine-load";
    private String serviceUrl = "pulsar://localhost:6650";
    private String subscriptionName = "pulsar-routine-load";
    private String pulsarPartitionName = "routine-load-partition-1,routine-load-partition-2,routine-load-partition-3";

    private PartitionNames partitionNames;

    private Separator columnSeparator = new Separator(",");

    private ImportSequenceStmt sequenceStmt = new ImportSequenceStmt("source_sequence");

    @Mocked
    ConnectContext connectContext;
    @Mocked
    TResourceInfo tResourceInfo;

    @Before
    public void init() {
        List<String> partitionNameList = Lists.newArrayList();
        for (String partitionName : pulsarPartitionName.split(",")) {
            partitionNameList.add(partitionName);
        }
        partitionNames = new PartitionNames(false, partitionNameList);
    }

    @Test
    public void testFromCreateStmt(@Mocked Env env,
                                   @Injectable Database database,
                                   @Injectable OlapTable table) throws UserException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc
                routineLoadDesc = new RoutineLoadDesc(columnSeparator, null,
                null, null, null, partitionNames, null,
                LoadTask.MergeType.APPEND, sequenceStmt.getSequenceColName());
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);
        List<Pair<String, String>> partitionIdToMessageId = Lists.newArrayList();
//        List<PartitionInfo> pulsarPartitionInfoList = Lists.newArrayList();
        for (String s : pulsarPartitionName.split(",")) {
            partitionIdToMessageId.add(new Pair<>(s, "0"));
//            PartitionInfo partitionInfo = new PartitionInfo(topicName, Integer.valueOf(s), null, null, null);
//            pulsarPartitionInfoList.add(partitionInfo);
        }
        RoutineLoadDataSourceProperties dsProperties = new RoutineLoadDataSourceProperties();
        dsProperties.setPulsarPartitionMessageIds(partitionIdToMessageId);
        Deencapsulation.setField(dsProperties, "pulsarServiceUrl", serviceUrl);
        Deencapsulation.setField(dsProperties, "pulsarTopic", topicName);
        Deencapsulation.setField(dsProperties, "pulsarSubscriptionName", subscriptionName);
        Deencapsulation.setField(createRoutineLoadStmt, "dataSourceProperties", dsProperties);

        long dbId = 1L;
        long tableId = 2L;

        new Expectations() {
            {
                database.getTableNullable(tableNameString);
                minTimes = 0;
                result = table;
                database.getId();
                minTimes = 0;
                result = dbId;
                table.getId();
                minTimes = 0;
                result = tableId;
                table.getType();
                minTimes = 0;
                result = Table.TableType.OLAP;
            }
        };

        new MockUp<PulsarUtil>() {
            @Mock
            public List<String> getAllPulsarPartitions(String serviceUrl, String topic, String subscriptionName,
                                                        Map<String, String> convertedCustomProperties) throws UserException {
                return Lists.newArrayList("", "", "");
            }
        };

        PulsarRoutineLoadJob pulsarRoutineLoadJob = PulsarRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
        Assert.assertEquals(jobName, pulsarRoutineLoadJob.getName());
        Assert.assertEquals(dbId, pulsarRoutineLoadJob.getDbId());
        Assert.assertEquals(tableId, pulsarRoutineLoadJob.getTableId());
        Assert.assertEquals(serviceUrl, Deencapsulation.getField(pulsarRoutineLoadJob, "serviceUrl"));
        Assert.assertEquals(topicName, Deencapsulation.getField(pulsarRoutineLoadJob, "topic"));
        Assert.assertEquals(subscriptionName, Deencapsulation.getField(pulsarRoutineLoadJob, "subscriptionName"));
        List<String> pulsarPartitionResult = Deencapsulation.getField(pulsarRoutineLoadJob, "currentPulsarPartitions");
        Assert.assertEquals(pulsarPartitionName, Joiner.on(",").join(pulsarPartitionResult));
        Assert.assertEquals(sequenceStmt.getSequenceColName(), pulsarRoutineLoadJob.getSequenceCol());
    }

    private CreateRoutineLoadStmt initCreateRoutineLoadStmt() {
        List<ParseNode> loadPropertyList = new ArrayList<>();
        loadPropertyList.add(columnSeparator);
        loadPropertyList.add(partitionNames);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.PULSAR.name();
        Map<String, String> customProperties = Maps.newHashMap();

        customProperties.put(CreateRoutineLoadStmt.PULSAR_TOPIC_PROPERTY, topicName);
        customProperties.put(CreateRoutineLoadStmt.PULSAR_SERVICE_URL_PROPERTY, serviceUrl);
        customProperties.put(CreateRoutineLoadStmt.PULSAR_SUBSCRIPTION_NAME_PROPERTY, subscriptionName);
        customProperties.put(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY, pulsarPartitionName);

        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                loadPropertyList, properties,
                typeName, customProperties,
                LoadTask.MergeType.APPEND);
        Deencapsulation.setField(createRoutineLoadStmt, "name", jobName);
        return createRoutineLoadStmt;
    }
}
