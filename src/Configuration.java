package com.singularity.ee.agent.systemagent.monitors;

import java.util.List;

/**
 * An object holder for the configuration file
 */
public class Configuration {

    String metricPrefix;
    List<Server> servers;
    List<Command> commands;

    public List<Server> getServers() {
        return servers;
    }

    public void setServers(List<Server> servers) {
        this.servers = servers;
    }

    public String getMetricPrefix() {
        return metricPrefix;
    }

    public void setMetricPrefix(String metricPrefix) {
        this.metricPrefix = metricPrefix;
    }

    public List<Command> getCommands() {
        return commands;
    }

    public void setCommands(List<Command> commands) {
        this.commands = commands;
    }
}
