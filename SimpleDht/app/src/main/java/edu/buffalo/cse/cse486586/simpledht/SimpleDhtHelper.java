package edu.buffalo.cse.cse486586.simpledht;

import java.util.HashMap;

public class SimpleDhtHelper {
    final static int[] remote_ports= new int[]{11108, 11112, 11116, 11120,11124};
    protected HashMap<Integer,Integer> fillHashMap(HashMap<Integer,Integer> input) {
        int emulator_number=0;
        for(int i:remote_ports)
        {
            input.put(i,(i/2));
        }
        return input;
    }
}
