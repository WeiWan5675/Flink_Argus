package org.weiwan.argus.common.options;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JCommanderTest {
    @Parameter
    public List<String> parameters = Lists.newArrayList();
 
    @Parameter(names = { "-log", "-verbose" }, description = "Level of verbosity")
    public Integer verbose = 1;
 
    @Parameter(names = "-groups", description = "Comma-separated list of group names to be run")
    public String groups;
 
    @Parameter(names = "-debug", description = "Debug mode")
    public boolean debug = false;

    @DynamicParameter(names = "-D", description = "Dynamic parameters go here")
    public Map<String, String> dynamicParams = new HashMap<String, String>();


    public List<String> getParameters() {
        return parameters;
    }

    public void setParameters(List<String> parameters) {
        this.parameters = parameters;
    }

    public Integer getVerbose() {
        return verbose;
    }

    public void setVerbose(Integer verbose) {
        this.verbose = verbose;
    }

    public String getGroups() {
        return groups;
    }

    public void setGroups(String groups) {
        this.groups = groups;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public Map<String, String> getDynamicParams() {
        return dynamicParams;
    }

    public void setDynamicParams(Map<String, String> dynamicParams) {
        this.dynamicParams = dynamicParams;
    }

    @Override
    public String toString() {
        return "JCommanderTest{" +
                "parameters=" + parameters +
                ", verbose=" + verbose +
                ", groups='" + groups + '\'' +
                ", debug=" + debug +
                ", dynamicParams=" + dynamicParams +
                '}';
    }
}