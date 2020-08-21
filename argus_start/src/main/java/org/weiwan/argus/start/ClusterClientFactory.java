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

import org.apache.flink.client.deployment.*;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.*;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.yarn.YarnClusterDescriptor;
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

    public static ClusterClient createStandaloneClient(StartOptions options) throws Exception {
        String flinkConfDir = options.getFlinkConf();
        Configuration config = ClusterConfigLoader.loadFlinkConfig(flinkConfDir);
        StandaloneClusterDescriptor standaloneClusterDescriptor = new StandaloneClusterDescriptor(config);
        ClusterClient clusterClient = standaloneClusterDescriptor.retrieve(StandaloneClusterId.getInstance()).getClusterClient();
        return clusterClient;
    }


    public static ClusterClient createYarnClusterClient(StartOptions options) {


        return null;
    }
}
