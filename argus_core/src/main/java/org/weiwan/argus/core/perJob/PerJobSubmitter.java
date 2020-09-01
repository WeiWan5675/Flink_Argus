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

package org.weiwan.argus.core.perJob;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.core.constants.ArgusConstans;
import org.weiwan.argus.core.start.StartOptions;
import org.weiwan.argus.core.utils.ClusterConfigLoader;
import org.weiwan.argus.core.utils.FlinkPerJobUtil;

import java.io.File;
import java.net.URL;
import java.util.List;


/**
 * Date: 2019/09/11
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PerJobSubmitter {
    private static final Logger LOG = LoggerFactory.getLogger(PerJobSubmitter.class);

    /**
     * submit per-job task
     *
     * @param launcherOptions LauncherOptions
     * @param jobGraph        JobGraph
     * @param remoteArgs      remoteArgs
     * @return
     * @throws Exception
     */
    public static String submit(StartOptions launcherOptions, JobGraph jobGraph, List<URL> urls, File coreJar, String[] remoteArgs) throws Exception {
        LOG.info("start to submit per-job task, launcherOptions = {}", launcherOptions.toString());
        //TODO 此处可以设置集群资源
        ClusterSpecification clusterSpecification = FlinkPerJobUtil.createClusterSpecification(null);
        clusterSpecification.setCreateProgramDelay(true);
        Configuration configuration = ClusterConfigLoader.loadFlinkConfig(launcherOptions);
        YarnConfiguration yarnConfig = ClusterConfigLoader.loadYarnConfig(launcherOptions);

        String libDir = launcherOptions.getLibDir();
        clusterSpecification.setConfiguration(configuration);
        clusterSpecification.setClasspaths(urls);
        clusterSpecification.setEntryPointClass(ArgusConstans.ARGUS_CORE_RUN_CLASS);
        clusterSpecification.setJarFile(coreJar);

//        if (StringUtils.isNotEmpty(launcherOptions.getS())) {
//            clusterSpecification.setSpSetting(SavepointRestoreSettings.forPath(launcherOptions.getS()));
//        }
        clusterSpecification.setProgramArgs(remoteArgs);
        clusterSpecification.setCreateProgramDelay(true);
        clusterSpecification.setYarnConfiguration(yarnConfig);
        clusterSpecification.setJobDesc(launcherOptions.getJobDescJson());
        PerJobClusterClientBuilder perJobClusterClientBuilder = new PerJobClusterClientBuilder();
        perJobClusterClientBuilder.init(launcherOptions);

        YarnClusterDescriptor descriptor = perJobClusterClientBuilder.createPerJobClusterDescriptor(launcherOptions);
        ClusterClientProvider<ApplicationId> provider = descriptor.deployJobCluster(clusterSpecification, jobGraph, true);
        String applicationId = provider.getClusterClient().getClusterId().toString();
        String flinkJobId = jobGraph.getJobID().toString();
        LOG.info("deploy per_job with appId: {}}, jobId: {}", applicationId, flinkJobId);
        return applicationId;
    }
}