package org.example.common;

/**
 * Abstract base class representing a generic agent.
 * All specific agents should extend this class and implement its abstract methods.
 *
 * This class encapsulates the agent's identity and defines
 * the contract for core behavior such as task execution and message handling.
 */
public abstract class Agent {

    /**
     * Unique name or identifier of the agent.
     * Marked as final because the agent's identity should not change after creation.
     */
    protected final String name;

    /**
     * Constructor to initialize the agent with a unique name.
     *
     * @param name the name or identifier of the agent
     */
    public Agent(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Agent name cannot be null or empty");
        }
        this.name = name;
    }

    /**
     * Returns the agent's name.
     *
     * @return the name of the agent
     */
    public String getName() {
        return this.name;
    }

    /**
     * Perform the primary task or behavior of the agent.
     * Concrete subclasses must provide the implementation.
     *
     * @throws Exception if any error occurs during task execution
     */
    public abstract void performTask() throws Exception;

    /**
     * Handle an incoming message to the agent.
     * This method can be used for inter-agent communication or external events.
     * Concrete subclasses must provide the implementation.
     *
     * @param message the message received by the agent
     */
    public abstract void receiveMessage(String message);
}
