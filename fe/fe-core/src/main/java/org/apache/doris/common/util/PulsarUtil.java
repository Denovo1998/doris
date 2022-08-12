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

package org.apache.doris.common.util;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PulsarUtil {
    private static final Logger LOG = LogManager.getLogger(PulsarUtil.class);


    public static List<Pair<String, String>> getLatestmessageId(long jobId, UUID taskId, String serviceUrl,
                                                                String topic, String subscriptionName,
                                                                Map<String, String> convertedCustomProperties,
                                                                List<String> partitionNames) throws LoadException {
        TNetworkAddress address = null;
        LOG.debug("begin to get latest messageId for partitions {} in topic: {}, task {}, job {}",
                partitionNames, topic, taskId, jobId);
        try {
            List<Long> backendIds = Env.getCurrentSystemInfo().getBackendIds(true);
            if (backendIds.isEmpty()) {
                throw new LoadException("Failed to get latest messageId. No alive backends");
            }
            Collections.shuffle(backendIds);
            Backend be = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
            address = new TNetworkAddress(be.getHost(), be.getBrpcPort());

            InternalService.PPulsarMetaProxyRequest.Builder metaRequestBuilder =
                    InternalService.PPulsarMetaProxyRequest.newBuilder()
                            .setPulsarInfo(InternalService.PPulsarLoadInfo.newBuilder()
                                    .setServiceUrl(serviceUrl)
                                    .setTopic(topic)
                                    .addAllProperties(
                                            convertedCustomProperties.entrySet().stream().map(
                                                    e -> InternalService.PStringPair.newBuilder()
                                                            .setKey(e.getKey())
                                                            .setVal(e.getValue())
                                                            .build()
                                            ).collect(Collectors.toList())
                                    )
                            );
            for (String partitionName : partitionNames) {
                metaRequestBuilder.addPartitionNameForLatestMessageIds(partitionName);
            }
            InternalService.PProxyRequest request = InternalService.PProxyRequest.newBuilder().setPulsarMetaRequest(
                    metaRequestBuilder).build();
            Future<InternalService.PProxyResult> future = BackendServiceProxy.getInstance().getInfo(address, request);
            InternalService.PProxyResult result = future.get(5, TimeUnit.SECONDS);
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                throw new UserException("failed to get latest messageId: " + result.getStatus().getErrorMsgsList());
            } else {
                List<InternalService.PStringPair> pairs = result.getPartitionMessageIds().getMessageIdTimesList();
                List<Pair<String, String>> partitionMessageIds = Lists.newArrayList();
                for (InternalService.PStringPair pair : pairs) {
                    partitionMessageIds.add(Pair.create(pair.getKey(), pair.getVal()));
                }
                LOG.debug("finish to get latest messageId for partitions {} in topic: {}, task {}, job {}",
                        partitionMessageIds, topic, taskId, jobId);
                return partitionMessageIds;
            }
        } catch (Exception e) {
            LOG.warn("failed to get latest messageId.", e);
            throw new LoadException(
                    "Failed to get latest messageId of pulsar topic: " + topic + ". error: " + e.getMessage());
        }
    }

    public static List<String> getAllPulsarPartitions(String serviceUrl, String topic, String subscriptionName,
                                                       Map<String, String> convertedCustomProperties)
            throws LoadException {
        TNetworkAddress address = null;
        try {
            List<Long> backendIds = Env.getCurrentSystemInfo().getBackendIds(true);
            if (backendIds.isEmpty()) {
                throw new LoadException("Failed to get all partitions. No alive backends");
            }
            Collections.shuffle(backendIds);
            Backend be = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
            address = new TNetworkAddress(be.getHost(), be.getBrpcPort());

            // create request
            InternalService.PProxyRequest request = InternalService.PProxyRequest.newBuilder().setPulsarMetaRequest(
                    InternalService.PPulsarMetaProxyRequest.newBuilder()
                            .setPulsarInfo(InternalService.PPulsarLoadInfo.newBuilder()
                                    .setServiceUrl(serviceUrl)
                                    .setTopic(topic)
                                    .setSubscriptionName(subscriptionName)
                                    .addAllProperties(convertedCustomProperties.entrySet().stream()
                                            .map(e -> InternalService.PStringPair.newBuilder().setKey(e.getKey())
                                                    .setVal(e.getValue()).build()).collect(Collectors.toList())
                                    )
                            )
            ).build();

            // get info
            Future<InternalService.PProxyResult> future = BackendServiceProxy.getInstance().getInfo(address, request);
            InternalService.PProxyResult result = future.get(5, TimeUnit.SECONDS);
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                throw new UserException("failed to get pulsar partition info: "
                        + result.getStatus().getErrorMsgsList());
            } else {
                return result.getPulsarMetaResult().getPartitionNamesList();
            }
        } catch (Exception e) {
            LOG.warn("failed to get partitions.", e);
            throw new LoadException(
                    "Failed to get all partitions of pulsar topic: " + topic + ". error: " + e.getMessage());
        }
    }
}
