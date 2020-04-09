package edu.buffalo.cse.cse486586.simpledht;

import java.util.HashMap;

public class SimpleDhtHelper {
    final static int[] remote_ports= new int[]{11108, 11112, 11116, 11120,11124};
    protected HashMap<Integer,Integer> fillHashMap(HashMap<Integer,Integer> input) {
        for(int i:remote_ports)
        {
            input.put(i,100);
        }
        return input;
    }
}
