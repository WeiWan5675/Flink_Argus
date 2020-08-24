/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.weiwan.argus.start;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.core.start.StartOptions;
import org.weiwan.argus.core.utils.ClusterConfigLoader;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Date: 2019/09/11
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PerJobClusterClientBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(PerJobClusterClientBuilder.class);

    private YarnClient yarnClient;

    private YarnConfiguration yarnConf;

    private Configuration flinkConfig;

    public void init(StartOptions startOptions, Properties conProp) throws Exception {
        String yarnConfDir = startOptions.getYarnConf();
        if (StringUtils.isBlank(yarnConfDir)) {
            throw new RuntimeException("parameters of yarn is required");
        }
        flinkConfig = ClusterConfigLoader.loadFlinkConfig(startOptions.getFlinkConf());
        conProp.forEach((key, val) -> flinkConfig.setString(key.toString(), val.toString()));
        SecurityUtils.install(new SecurityConfiguration(flinkConfig));

        yarnConf = ClusterConfigLoader.loadYarnConfig(yarnConfDir);
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConf);
        yarnClient.start();

        LOG.info("----init yarn success ----");
    }


    public YarnClusterDescriptor createPerJobClusterDescriptor(StartOptions options) throws MalformedURLException {
        String flinkJarPath = options.getLibDir();
        String flinkConf = options.getFlinkConf();
        if (StringUtils.isNotBlank(flinkJarPath)) {
            if (!new File(flinkJarPath).exists()) {
                throw new IllegalArgumentException("The Flink jar path is not exist");
            }
        } else {
            throw new IllegalArgumentException("The Flink jar path is null");
        }
        YarnClusterDescriptor descriptor = new YarnClusterDescriptor(
                flinkConfig,
                yarnConf,
                yarnClient,
                YarnClientYarnClusterInformationRetriever.create(yarnClient),
                false);

        List<File> shipFiles = new ArrayList<>();
        File[] jars = new File(flinkJarPath).listFiles();
        if (jars != null) {
            for (File jar : jars) {
                if (jar.toURI().toURL().toString().contains("flink-dist")) {
                    descriptor.setLocalJarPath(new Path(jar.toURI().toURL().toString()));
                } else {
                    shipFiles.add(jar);
                }
            }
        }


        //TODO 设置集群日志
        File log4j = new File(flinkConf + File.separator + "log4j.properties");
        if (log4j.exists()) {
            shipFiles.add(log4j);
        } else {
            File logback = new File(flinkConf + File.separator + "logback.xml");
            if (logback.exists()) {
                shipFiles.add(logback);
            }
        }
        descriptor.addShipFiles(shipFiles);
        return descriptor;
    }
}