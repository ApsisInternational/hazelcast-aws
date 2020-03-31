/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cluster.impl;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.ecs.model.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.aws.AWSClient;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ExceptionUtil;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.lang.String.format;

public class TcpIpJoinerOverAWS extends TcpIpJoiner {

    private final AWSClient aws;
    private final ILogger logger;

    public TcpIpJoinerOverAWS(Node node) {
        super(node);
        logger = node.getLogger(getClass());
        AwsConfig awsConfig = node.getConfig().getNetworkConfig().getJoin().getAwsConfig();
        aws = new AWSClient(awsConfig);
    }

    @Override
    protected Collection<String> getMembers() {
        try {
//            Collection<String> machineIps = aws.getPrivateIpAddresses();
            Collection<String> machineIps = Collections.emptySet();
            List<String> taskIps = getEcsTaskEniAddress();
            ArrayList<String> addresses = new ArrayList<String>(machineIps.size() + taskIps.size());
            addresses.addAll(machineIps);
            addresses.addAll(taskIps);
            if (addresses.isEmpty()) {
                logger.warning("No EC2 instances or ESC task found!");
            } else {
                if (logger.isFinestEnabled()) {
                    StringBuilder sb = new StringBuilder("Found the following EC2 instances:\n");
                    for (String ip : addresses) {
                        sb.append("    ").append(ip).append("\n");
                    }
                    logger.finest(sb.toString());
                }
            }
            return addresses;
        } catch (Exception e) {
            logger.warning(e);
            throw ExceptionUtil.rethrow(e);
        }
    }

    private List<String> getEcsTaskEniAddress() {
        List<String> ips = new ArrayList<String>();
        try {
            String containerMetaData = System.getenv("ECS_CONTAINER_METADATA_URI");
            if (containerMetaData == null || containerMetaData.isEmpty()) {
                throw new IllegalArgumentException("ECS_CONTAINER_METADATA_URI not set in ecs config");
            }
            OkHttpClient client = new OkHttpClient().newBuilder().build();
            Request request = new Request.Builder().url(containerMetaData).get().build();
            Response response = client.newCall(request).execute();
            ObjectMapper mapper = new ObjectMapper();
            final JsonNode jsonNode = mapper.readValue(response.body().bytes(), JsonNode.class);
            String cluster = jsonNode.get("Labels").get("com.amazonaws.ecs.cluster").asText();
            String family = jsonNode.get("Labels").get("com.amazonaws.ecs.task-definition-family").asText();
            logger.finest(format("Found cluster %s, family name %s", cluster, family));
            final AmazonECS amazonECS = AmazonECSClientBuilder.standard()
                    .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                    //.withRegion("eu-west-1")
                    .build();

            ListTasksResult listTasksResult = amazonECS.listTasks(new ListTasksRequest().withCluster(cluster).withFamily(family));
            while (listTasksResult == null || listTasksResult.getTaskArns().isEmpty()) {
                Thread.sleep(2000);
                listTasksResult = amazonECS.listTasks(new ListTasksRequest().withCluster(cluster).withFamily(family));
            }
            listTasksResult = amazonECS.listTasks(new ListTasksRequest().withCluster(cluster).withFamily(family));
            if (listTasksResult != null && !listTasksResult.getTaskArns().isEmpty()) {
                logger.finest(listTasksResult.getTaskArns().toString());

                final List<Task> tasks = amazonECS.describeTasks(new DescribeTasksRequest().withCluster(cluster)
                        .withTasks(listTasksResult.getTaskArns())).getTasks();
                logger.finest(tasks.toString());
                for (Task a : tasks) {
                    for (Container c : a.getContainers()) {
                        ips.add(c.getNetworkInterfaces().get(0).getPrivateIpv4Address());
                    }
                }
                logger.finest(format("Ecs task private IP addresses : %s", ips));
            }
        } catch (Exception e) {
            logger.severe(e);
        }
        return ips;
    }

    @Override
    protected int getConnTimeoutSeconds() {
        AwsConfig awsConfig = node.getConfig().getNetworkConfig().getJoin().getAwsConfig();
        return awsConfig.getConnectionTimeoutSeconds();
    }

    @Override
    public String getType() {
        return "aws";
    }
}
