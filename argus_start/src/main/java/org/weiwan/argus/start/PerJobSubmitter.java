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
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.core.start.StartOptions;
import org.weiwan.argus.core.utils.ClusterConfigLoader;

import java.io.File;
import java.net.URL;
import java.util.List;

import static org.weiwan.argus.start.FlinkPerJobUtil.createClusterSpecification;

/**
 * Date: 2019/09/11
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PerJobSubmitter {
    private static final Logger LOG = LoggerFactory.getLogger(PerJobSubmitter.class);


    public static String submit(StartOptions options, JobGraph jobGraph, File coreJarFile, List<URL> urlList, String[] remoteArgs) throws Exception {
        LOG.info("start to submit per-job task, launcherOptions = {}", options.toString());


        Configuration configuration = ClusterConfigLoader.loadFlinkConfig(options);
        YarnConfiguration yarnConfig = ClusterConfigLoader.loadYarnConfig(options);
        ClusterSpecification clusterSpecification = createClusterSpecification();
        clusterSpecification.setConfiguration(configuration);
        clusterSpecification.setClasspaths(urlList);
        clusterSpecification.setEntryPointClass(DataSyncStarter.ARGUS_CORE_RUN_CLASS);
        clusterSpecification.setJarFile(coreJarFile);

//        if (StringUtils.isNotEmpty(launcherOptions.getS())) {
//            clusterSpecification.setSpSetting(SavepointRestoreSettings.forPath(launcherOptions.getS()));
//        }
        clusterSpecification.setProgramArgs(remoteArgs);
        clusterSpecification.setCreateProgramDelay(true);
        clusterSpecification.setYarnConfiguration(yarnConfig);

        PerJobClusterClientBuilder perJobClusterClientBuilder = new PerJobClusterClientBuilder();
        perJobClusterClientBuilder.init(options);

        YarnClusterDescriptor descriptor = perJobClusterClientBuilder.createPerJobClusterDescriptor(options);
        ClusterClientProvider<ApplicationId> provider = descriptor.deployJobCluster(clusterSpecification, jobGraph, true);
        String applicationId = provider.getClusterClient().getClusterId().toString();
        String flinkJobId = jobGraph.getJobID().toString();
        LOG.info("deploy per_job with appId: {}}, jobId: {}", applicationId, flinkJobId);
        return applicationId;
    }


}