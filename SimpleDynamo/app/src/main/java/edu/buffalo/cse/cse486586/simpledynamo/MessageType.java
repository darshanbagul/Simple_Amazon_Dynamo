package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by darshanbagul on 05/05/17.
 */

/*
 * This is a class that contains the different types of Message types. We shall be using these
 * types to decide the operation when each message is received at a node.
 */
public enum MessageType {
    INSERT,
    QUERY,
    FETCH_ALL,
    DELETE,
    SUCCESS,
    RECOVER,
    SEND_BACK
}
