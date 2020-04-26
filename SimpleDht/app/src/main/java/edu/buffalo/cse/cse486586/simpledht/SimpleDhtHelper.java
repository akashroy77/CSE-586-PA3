package edu.buffalo.cse.cse486586.simpledht;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

public class SimpleDhtHelper {
    final static int[] remote_ports= new int[]{11108, 11112, 11116, 11120,11124};

    HashMap<String,Integer> emulatorHash= new HashMap<String, Integer>();
    protected HashMap<Integer,Integer> fillHashMap(HashMap<Integer,Integer> input) {
        int emulator_number=0;
        for(int i:remote_ports)
        {
            input.put(i,(i/2));
        }
        return input;
    }

//    public int genratePort(String h) throws NoSuchAlgorithmException {
//        for ( int i:remote_ports){
//            int j=i/2;
//            emulatorHash.put(provider.genHash(Integer.toString(j)),i);
//        }
//        return emulatorHash.get(h);
//    }
}
