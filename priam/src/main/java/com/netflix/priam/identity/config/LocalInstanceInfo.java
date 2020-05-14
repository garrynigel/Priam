/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.identity.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import javax.inject.Inject;

/**
 * Looks at local (props) properties for metadata about the running 'instance'. Typically, this is
 * used for locally-deployed testing.
 */
public class LocalInstanceInfo implements InstanceInfo {
    private static final String PREFIX = "Priam.localInstance.";
    private static final String DEFAULT_LOCAL_CONFIG = "/etc/priam/conf/Priam.properties";
    Properties props;

    @Inject
    public LocalInstanceInfo() {
        this.props = new Properties();
        try {
            FileInputStream fis = new FileInputStream(DEFAULT_LOCAL_CONFIG);
            props.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getRac() {
        return props.getProperty(PREFIX + "availabilityZone", "");
    }

    @Override
    public String getHostname() {
        return props.getProperty(PREFIX + "privateIp", "");
    }

    @Override
    public String getHostIP() {
        return props.getProperty(PREFIX + "privateIp", "");
    }

    @Override
    public String getPrivateIP() {
        return props.getProperty(PREFIX + "privateIp", "");
    }

    @Override
    public String getInstanceId() {
        return props.getProperty(PREFIX + "instanceId", "");
    }

    @Override
    public String getInstanceType() {
        return props.getProperty(PREFIX + "instanceType", "");
    }

    @Override
    public String getVpcId() {
        return props.getProperty(PREFIX + "vpcid", "");
    }

    @Override
    public String getAutoScalingGroup() {
        return props.getProperty(PREFIX + "asg", "");
    }

    @Override
    public InstanceEnvironment getInstanceEnvironment() {
        return (getVpcId() == null) ? InstanceEnvironment.CLASSIC : InstanceEnvironment.VPC;
    }

    @Override
    public String getRegion() {
        return props.getProperty(PREFIX + "region", "");
    }
}
