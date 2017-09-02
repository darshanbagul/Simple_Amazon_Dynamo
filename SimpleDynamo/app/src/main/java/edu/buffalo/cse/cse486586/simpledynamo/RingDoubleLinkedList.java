package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.Vector;
/**
 * Created by darshanbagul on 28/04/17.
 */

/*
 * This is a class that handles the implementation of Dynamo Ring, which handles dynamic
 * partitioning. Dynamo’s partitioning scheme relies on consistent hashing to distribute the
 * load across multiple storage hosts. The output range of a hash function is treated as a
 * fixed circular space or “ring”.
 *
 * As we had to implement a ring or a circular space as stated above, the natural data structure
 * popping to mind was a Circular Linked List which can be implemented using a Double Linked List.
 */
public class RingDoubleLinkedList {
    private Node headNode = null;
    private int numNodes = 0;

    // Definition of a node in the Linked List
    class Node implements Comparable<Node> {
        private String nodeId;
        private int nodePort;
        private Node prevNode;
        private Node nextNode;

        Node(String nodeId, int nodePort, Node prevNode, Node nextNode) {
            this.nodeId = nodeId;
            this.nodePort = nodePort;
            this.nextNode = nextNode;
        }

        @Override
        public int compareTo(Node nodeToCompare) {
            return this.nodeId.compareTo(nodeToCompare.nodeId);
        }
    }

    /*
     * Below we implement helper functions for enabling operations over this Ring Data Structure.
     */
    public void add(String nodeId, int port) {
        Node addNode = new Node(nodeId, port, null, null);

        if (numNodes == 0) {
            addNode.prevNode = addNode;
            addNode.nextNode = addNode;
            headNode = addNode;
        } else if (numNodes == 1) {
            addNode.prevNode = headNode;
            addNode.nextNode = headNode;
            headNode.prevNode = addNode;
            headNode.nextNode = addNode;
            if (addNode.compareTo(headNode) <= 0) {
                headNode = addNode;
            }
        } else {
            Node currNode = headNode;
            for (int i = 0; i < numNodes; i = i + 1) {
                if (addNode.compareTo(currNode) <= 0) {
                    addNode.prevNode = currNode.prevNode;
                    addNode.nextNode = currNode;
                    currNode.prevNode.nextNode = addNode;
                    currNode.prevNode = addNode;
                    if (currNode == headNode) {
                        headNode = addNode;
                    }
                    break;
                } else if (currNode.nextNode == headNode) {
                    addNode.prevNode = currNode;
                    addNode.nextNode = headNode;
                    currNode.nextNode = addNode;
                    headNode.prevNode = addNode;
                    break;
                } else {
                    currNode = currNode.nextNode;
                }
            }
        }
        numNodes = numNodes + 1;
    }

    public int getNumNodes() {
        return numNodes;
    }

    public int getPort(String hashKey) {
        return getNodeByKey(hashKey).nodePort;
    }

    public int getQueryPort(String hashKey) {
        Node tNode = getNodeByKey(hashKey);
        for (int i = 0; i < SimpleDynamoProvider.num_replicas-1; i++) {
            tNode = tNode.nextNode;
        }
        return tNode.nodePort;
    }

    public Vector<Integer> fetchAllPorts() {
        Vector<Integer> v = new Vector<Integer>();
        Node curr = headNode;
        for (int i = 0; i < numNodes; i++) {
            v.add(curr.nodePort);
            curr = curr.nextNode;
        }
        return v;
    }

    public int getNextPort(int port) {
        Node tNode = getNodeByPort(port);
        if (tNode != null) {
            return tNode.nextNode.nodePort;
        }
        return 0;
    }

    public int getPrevPort(int port) {
        Node tNode = getNodeByPort(port);
        if (tNode != null) {
            return tNode.prevNode.nodePort;
        }
        return 0;
    }

    private Node getNodeByKey(String hashKey) {
        Node currNode = headNode;
        for (int i = 0; i < numNodes; i++) {
            if (hashKey.compareTo(currNode.nodeId) <= 0) {
                return currNode;
            }
            currNode = currNode.nextNode;
        }
        return currNode;
    }

    private Node getNodeByPort(int port) {
        Node currNode = headNode;
        for (int i = 0; i < numNodes; i++) {
            if (currNode.nodePort == port) {
                return currNode;
            }
            currNode = currNode.nextNode;
        }
        return null;
    }
}