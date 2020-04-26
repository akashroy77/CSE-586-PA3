package edu.buffalo.cse.cse486586.simpledht;

public class ChordNode {
    int emulator;
    String identifier;
    String successor;
    String predecessor;
    public ChordNode(){

    }
    public ChordNode(int emulator,String identifier,String successor,String predecessor){
        this.emulator=emulator;
        this.identifier=identifier;
        this.successor=successor;
        this.predecessor=predecessor;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getSuccessor() {
        return successor;
    }

    public void setSuccessor(String successor) {
        this.successor = successor;
    }

    public String getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(String predecessor) {
        this.predecessor = predecessor;
    }

    public int getEmulator() {
        return emulator;
    }

    public void setEmulator(int emulator) {
        this.emulator = emulator;
    }
}
