package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.concurrent.ConcurrentHashMap;
import java.io.Serializable;
/**
 * Created by darshanbagul on 28/04/17.
 */

public class Message implements Serializable {
    public ConcurrentHashMap<String, String> map;
    public int index;
    public MessageType operation;
    public boolean forward;
    public boolean is_skipped = false;

    public Message(int index, ConcurrentHashMap<String, String> map, MessageType operation, boolean forward) {
        this.map = map;
        this.index = index;
        this.operation = operation;
        this.forward = forward;
    }
}
