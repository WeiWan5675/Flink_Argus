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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.deployment.*;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.weiwan.argus.core.start.StartOptions;
import org.weiwan.argus.core.utils.ClusterConfigLoader;
import org.weiwan.argus.start.enums.RunMode;


import java.net.InetSocketAddress;


public class ClusterClientFactory {

    public static ClusterClient createClusterClient(StartOptions launcherOptions) throws Exception {
        String mode = launcherOptions.getMode();
        if (mode.equals(RunMode.standalone.name())) {
            return createStandaloneClient(launcherOptions);
        } else if (mode.equals(RunMode.yarn.name())) {
//            return createYarnClient(launcherOptions);
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


    public ClusterClient createYarnCusterClient(StartOptions startOptions) throws ClusterRetrieveException, ClusterDeploymentException {

        ClusterClient client = null;
        String flinkConf = startOptions.getFlinkConf();
        Configuration configuration = ClusterConfigLoader.loadFlinkConfig(flinkConf);
        String yarnConf = startOptions.getYarnConf();
        YarnConfiguration yarnConfig = ClusterConfigLoader.loadYarnConfig(yarnConf);
        String clusterIdStr = "Flink Session Cluster";
        ApplicationId clusterId = ConverterUtils.toApplicationId(clusterIdStr);
        AbstractYarnClusterDescriptor clusterDescriptor = getClusterDescriptor(configuration, yarnConfig, flinkConf);

        if (RunMode.yarn == RunMode.valueOf(startOptions.getMode().toLowerCase())) {
            client = clusterDescriptor.retrieve(clusterId);
        } else if (RunMode.yarn == RunMode.valueOf(startOptions.getMode().toLowerCase())) {
            ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                    .setMasterMemoryMB(512)
                    .setTaskManagerMemoryMB(512)
                    .setNumberTaskManagers(2)
                    .setSlotsPerTaskManager(2)
                    .createClusterSpecification();

            client = clusterDescriptor.deploySessionCluster(clusterSpecification);

        } else {

        }



        return null;
    }

    private AbstractYarnClusterDescriptor getClusterDescriptor(
            Configuration configuration,
            YarnConfiguration yarnConfiguration,
            String configurationDirectory) {
        final YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        return new YarnClusterDescriptor(
                configuration,
                yarnConfiguration,
                configurationDirectory,
                yarnClient,
                false);
    }
}
