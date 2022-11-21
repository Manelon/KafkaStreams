package com.manelon.kafkastreams_springboot.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "topics")
public class Topics {
    private String users;
    private String processedUsers;
    public String getUsers() {
        return users;
    }
    public void setUsers(String users) {
        this.users = users;
    }
    public String getProcessedUsers() {
        return processedUsers;
    }
    public void setProcessedUsers(String processedUsers) {
        this.processedUsers = processedUsers;
    }
}
