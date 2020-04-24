package edu.buffalo.cse.cse486586.simpledht;

public class ChordNode {
    String identifier;
    String successor;
    String predecessor;
    public ChordNode(){

    }
    public ChordNode(String identifier,String successor,String predecessor){
        this.identifier=identifier;
        this.successor=successor;
        this.predecessor=predecessor;
    }
}
