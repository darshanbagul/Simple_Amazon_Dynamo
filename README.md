# Replicated Key-Value Storage - Simplified Amazon Dynamo
This project contains a submission for an assignment; requirement for the course CSE 586: Distributed Systems offered in Spring 2017 at State University of New York.

## Introduction

The main goal is to provide both availability and linearizability at the same time. In other words, your implementation should always perform read and write operations successfully even under node failures. At the same time, a read operation should always return the most recent value.

There are three key concepts that we implemented: 
    1) Partitioning 
    2) Replication
    3) Failure handling.

## Implementation Details

  **1. Membership**

  Just as the original Dynamo, every node can know every other node. This means that each node knows all other nodes in the system and also knows exactly which partition belongs to which node; any node can forward a request to the correct node without using a ring-based routing.

  **2. Request routing**
  
  Unlike Chord, each Dynamo node knows all other nodes in the system and also knows exactly which partition belongs to which node. Under no failures, a request for a key is directly forwarded to the coordinator (i.e., the successor of the key), and the coordinator should be in charge of serving read/write operations.
  
  **3. Quorum replication**
      
    a. For linearizability, we implemented a quorum-based replication used by Dynamo. The original design does not provide linearizability, we adapt the design.

    b. The replication degree N is 3. This means that given a key, the key’s coordinator as well as the 2 successor nodes in the Dynamo ring store the key.
    
    c. Both the reader quorum size R and the writer quorum size W is 2.
    
    d. The coordinator for a get/put request always contacts other two nodes and gets a vote from each (i.e., an acknowledgement for a write, or a value for a read). For write operations, all objects can be versioned in order to distinguish stale copies from the most recent copy. For read operations, if the readers in the reader quorum have different versions of the same object, the coordinator picks the most recent version and returns it.

  **4. Chain replication**
    
Another replication strategy is chain replication, which provides linearizability as introduced [here](http://www.cs.cornell.edu/home/rvr/papers/osdi04.pdf)

  In chain replication, a write operation always comes to the first partition; then it propagates to the next two partitions in sequence. The last partition returns the result of the write. A read operation always comes to the last partition and reads the value from the last partition.

  **5. Failure handling**
  
  Just as the original Dynamo, each request can be used to detect a node failure. For this purpose, we use a timeout for a socket read; and if a node does not respond within the timeout, we consider it a failure.

  Relying on socket creation or connect status to determine if a node has failed is not a foolproof startegy. Due to the Android emulator networking setup, it is not safe to rely on socket creation or connect status to judge node failures. 
  
  When a coordinator for a request fails and it does not respond to the request, its successor can be contacted next for the request

## Testing using Grader

  1. Load the Project in Android Studio and create the apk file.

  2. Download the Testing Program for your platform. Before you run the program, please make sure that you are running five AVDs. The below command will do it:
  ```
      python run_avd.py 5
  ```

  3. Also make sure that the Emulator Networking setup is done. The below command will do it:
  ```
      python set_redir.py 10000
  ```

  4. Run the grader:
  ```
      $ chmod +x < grader executable> - $ ./< grader executable> apk file path
  ```
  
  5. ‘-h’ argument will show you what options are available. Usage is shown below: 
  ```
      $ < grader executable> -h
  ```
  
  6. You can specify which testing phase you want to test by providing ‘-p’ or ‘--phase’ argument to the tester.
  7. **Note:** If you run an individual phase with "-p", it will always be a fresh install. However if you run all phases (without "-p"), it will not always be a fresh install; the grader will do a fresh-install before phase 1, and do another fresh-install before phase 2. Afterwards, there will be no install. This means that all data from previous phases will remain intact.
  8. The grader uses multiple threads to test your code and each thread will independently print out its own log messages. This means that an error message might appear in the middle of the combined log messages from all threads, rather than at the end.

## Credits

This project comprises of scripts developed by Networked Systems Research Group at The State University of New York. I thank Prof. Steve Ko for teaching the course and encouraging practical implementations of important concepts in Large Scale Distributed Systems.

## References

  1. [Amazon Dynamo](http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) research paper
  
  2. Distributed Systems: Concepts and Design (5th Edition)

  2. Coursera MOOC - Cloud Computing Concepts - University of Illinois at Urbana-Champaign by Dr. Indranil Gupta
