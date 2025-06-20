package org.example.common;

public abstract class Agent {
    protected String name;

    public Agent(String name) {
        this.name = name;
    }

    public abstract void performTask() throws Exception;

    public String getName() {
        return this.name;
    }
    public abstract void receiveMessage(String message);

}