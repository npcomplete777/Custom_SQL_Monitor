package com.singularity.ee.agent.systemagent.monitors;

public class Server {

    private String server;
    private String driver;
    private String connectionString;
    private String user;
    private String password;

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }


    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }
}
