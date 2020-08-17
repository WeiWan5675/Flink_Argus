/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.weiwan.argus.start;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.deployment.StandaloneClusterDescriptor;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.weiwan.argus.core.start.StartOptions;
import org.weiwan.argus.start.enums.RunMode;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.*;

/**
 * The Factory of ClusterClient
 * <p>
 * Company: www.dtstack.com
 *
 * @author huyifanzju@163.com
 */
public class ClusterClientFactory {

    public static ClusterClient createClusterClient(StartOptions launcherOptions) throws Exception {
        String mode = launcherOptions.getMode();
        if (mode.equals(RunMode.standalone.name())) {
            return createStandaloneClient(launcherOptions);
        } else if (mode.equals(RunMode.yarn.name())) {
            return createYarnClient(launcherOptions);
        }

        throw new IllegalArgumentException("Unsupported cluster client type: ");
    }

    public static ClusterClient createStandaloneClient(StartOptions launcherOptions) throws Exception {
        String flinkConfDir = launcherOptions.getFlinkConf();
        Configuration config = GlobalConfiguration.loadConfiguration(flinkConfDir);
        StandaloneClusterDescriptor standaloneClusterDescriptor = new StandaloneClusterDescriptor(config);
        RestClusterClient clusterClient = standaloneClusterDescriptor.retrieve(StandaloneClusterId.getInstance());
        LeaderConnectionInfo connectionInfo = clusterClient.getClusterConnectionInfo();
        InetSocketAddress address = AkkaUtils.getInetSocketAddressFromAkkaURL(connectionInfo.getAddress());
        config.setString(JobManagerOptions.ADDRESS, address.getAddress().getHostName());
        config.setInteger(JobManagerOptions.PORT, address.getPort());
        clusterClient.setDetached(true);
        return clusterClient;
    }

    public static ClusterClient createYarnClient(StartOptions launcherOptions) {
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration(launcherOptions.getFlinkConf());
        String yarnConfDir = launcherOptions.getYarnConf();
        if (StringUtils.isNotBlank(yarnConfDir)) {
            try {
                flinkConfig.setString(ConfigConstants.PATH_HADOOP_CONFIG, yarnConfDir);
                FileSystem.initialize(flinkConfig);

                YarnConfiguration yarnConf = YarnConfLoader.getYarnConf(yarnConfDir);
                YarnClient yarnClient = YarnClient.createYarnClient();
                yarnClient.init(yarnConf);
                yarnClient.start();
                ApplicationId applicationId;

                if (StringUtils.isEmpty(launcherOptions.getAppId())) {
                    applicationId = getAppIdFromYarn(yarnClient);
                    if (applicationId != null && StringUtils.isEmpty(applicationId.toString())) {
                        throw new RuntimeException("No flink session found on yarn cluster.");
                    }
                } else {
                    applicationId = ConverterUtils.toApplicationId(launcherOptions.getAppId());
                }
                AbstractYarnClusterDescriptor yarnClusterDescriptor = getClusterDescriptor(launcherOptions, yarnClient, yarnConf, flinkConfig);
                ClusterClient clusterClient = yarnClusterDescriptor.retrieve(applicationId);
                clusterClient.setDetached(true);
                return clusterClient;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        throw new UnsupportedOperationException("Haven't been developed yet!");
    }

    private static AbstractYarnClusterDescriptor getClusterDescriptor(StartOptions launcherOptions,
                                                                      YarnClient yarnClient,
                                                                      YarnConfiguration yarnConf,
                                                                      Configuration flinkConfig) throws Exception {
        AbstractYarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(flinkConfig, yarnConf, "", yarnClient, true);

        yarnClusterDescriptor.setName(launcherOptions.getAppId());
        return yarnClusterDescriptor;
    }

    private static ApplicationId getAppIdFromYarn(YarnClient yarnClient) throws Exception {
        Set<String> set = new HashSet<>();
        set.add("Apache Flink");
        EnumSet<YarnApplicationState> enumSet = EnumSet.noneOf(YarnApplicationState.class);
        enumSet.add(YarnApplicationState.RUNNING);
        List<ApplicationReport> reportList = yarnClient.getApplications(set, enumSet);

        ApplicationId applicationId = null;
        int maxMemory = -1;
        int maxCores = -1;
        for (ApplicationReport report : reportList) {
            if (!report.getName().startsWith("Flink session")) {
                continue;
            }

            if (!report.getYarnApplicationState().equals(YarnApplicationState.RUNNING)) {
                continue;
            }

            int thisMemory = report.getApplicationResourceUsageReport().getNeededResources().getMemory();
            int thisCores = report.getApplicationResourceUsageReport().getNeededResources().getVirtualCores();

            boolean isOverMaxResource = thisMemory > maxMemory || thisMemory == maxMemory && thisCores > maxCores;
            if (isOverMaxResource) {
                maxMemory = thisMemory;
                maxCores = thisCores;
                applicationId = report.getApplicationId();
            }
        }

        return applicationId;
    }
}
