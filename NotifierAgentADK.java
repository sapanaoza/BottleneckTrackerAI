package org.example.Agents;

import jade.core.Agent;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;

public class NotifierAgentADK extends Agent {

    @Override
    protected void setup() {
        System.out.println(getLocalName() + ": NotifierAgent started.");

        // Heartbeat every 10 seconds
        addBehaviour(new TickerBehaviour(this, 10000) {
            @Override
            protected void onTick() {
                System.out.println(getLocalName() + " is alive and waiting for messages...");
            }
        });

        // Message receiver behaviour
        addBehaviour(new CyclicBehaviour(this) {
            @Override
            public void action() {
                ACLMessage msg = receive();
                if (msg != null) {
                    System.out.println(getLocalName() + " received alert: " + msg.getContent());
                    // Add additional processing here, e.g., sending email, logging, etc.
                } else {
                    block();
                }
            }
        });
    }

    @Override
    protected void takeDown() {
        System.out.println(getLocalName() + ": NotifierAgent shutting down.");
    }
}
