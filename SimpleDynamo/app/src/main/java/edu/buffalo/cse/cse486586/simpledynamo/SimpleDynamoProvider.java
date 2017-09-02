package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


public class SimpleDynamoProvider extends ContentProvider {
	private static final String TAG = SimpleDynamoProvider.class.getSimpleName();

	/*
	 * We begin by initialising all the ports, including server port to which we have redirected all
	 * our emulator listeners.
	 */
	private static final int SERVER_PORT = 10000;
	static final int REMOTE_PORT0 = 11108;
	static final int REMOTE_PORT1 = 11112;
	static final int REMOTE_PORT2 = 11116;
	static final int REMOTE_PORT3 = 11120;
	static final int REMOTE_PORT4 = 11124;
	private int currPort;

	// Create a port-device map, so that it is accessible for future use
	Map<String,Integer> devicePortMap = new HashMap<String,Integer>(){{
		put("5554", REMOTE_PORT0);
		put("5556", REMOTE_PORT1);
		put("5558", REMOTE_PORT2);
		put("5560", REMOTE_PORT3);
		put("5562", REMOTE_PORT4);
	}};

	public Uri mUri;

	private static boolean is_recover = false;

	// The replication degree N should be 3 as stated in the requirements.
	public static final int num_replicas = 3;

	public static final int TIMEOUT = 10000;

	/*
	 * We shall be using an AtomicInteger variable as an incremental counter in this assignment, and
	 * I believe this is one of the important parts to correctly implement the Ring DHT in our
	 * Dynamo database. This class provides us with an integer variable that can be updated
	 * atomically. To find more in detail,
	 * visit: https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/atomic/AtomicInteger.html
	 */
	private static AtomicInteger waitNotifier = new AtomicInteger(0);

	// Instantiate our Ring DHT, which we have implemented using a Circular Double Linked List.
	private static RingDoubleLinkedList ringPartition = new RingDoubleLinkedList();
	private static Vector<ConcurrentHashMap<String,String>> replicaPtr = new
			Vector<ConcurrentHashMap<String,String>>(num_replicas);
	// Initialize variables that will store our <KEY,VALUE> pairs, also while recovery operation
	private static ConcurrentHashMap<String,String> hashTable = new ConcurrentHashMap<String, String>();
	private static ConcurrentHashMap<String, Object> waitList = new ConcurrentHashMap<String, Object>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// If the recovery variable, 'is_recover' is set to True, then we wait for recovery
		// operation to be performed. We will check this condition before all operations on our
		// dynamo and pause execution until recovery is completed.
		if (is_recover) {
			synchronized (waitNotifier) {
				try {
					waitNotifier.wait();
				} catch (InterruptedException e) {
					Log.e("DELETE_RECOVERY", "InterruptedException: "+ e.getMessage());
				}
			}
		}
		ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
		map.put(selection, "");
		try {
			int port = ringPartition.getPort(genHash(selection));

			if (port == currPort) {
				replicaPtr.get(0).remove(selection);
				requestRouter(map, 1, MessageType.DELETE, ringPartition.getNextPort(port));
			} else {
				requestRouter(map, 0, MessageType.DELETE, port);
			}
		} catch (NoSuchAlgorithmException e) {
			Log.e("DELETE_HASH_ERR", "NoSuchAlgorithmException: " + e.getMessage());
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		if (is_recover) {
			synchronized (waitNotifier) {
				try {
					waitNotifier.wait();
				} catch (InterruptedException e) {
					Log.e("INSERT_RECOVERY", "InterruptedException: " + e.getMessage());
				}
			}
		}

		ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
		map.put(values.getAsString("key"), values.getAsString("value"));

		try {
			int port = ringPartition.getPort(genHash(values.getAsString("key")));

			if (port == currPort) {
				replicaPtr.get(0).putAll(map);
				requestRouter(map, 1, MessageType.INSERT, ringPartition.getNextPort(port));
			} else {
				requestRouter(map, 0, MessageType.INSERT, port);
			}
		} catch (NoSuchAlgorithmException e) {
			Log.e("INSERT_HASH_ERR", "NoSuchAlgorithmException: " + e.getMessage());
		}
		return uri;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		// If the recovery variable, 'is_recover' is set to True, then we wait for recovery
		// operation to be performed. We will check this condition before all operations on our
		// dynamo and pause execution until recovery is completed.
		if (is_recover) {
			synchronized (waitNotifier) {
				try {
					waitNotifier.wait();
				} catch (InterruptedException e) {
					Log.e("QUERY_INTERRUPT", "InterruptedException: Recover failed in querying - "
							+ e.getMessage());
				}
			}
		}

		/*
		 * Since different nodes(threads) can be accessing the data stored in this object, we need
		 * to perform locking so that there cannot be simultaneous modifications to the object.
		 * This is done by a synchronized block or a method in Java. We shall be using such
		 * synchronized blocks everywhere through out our implementation.
		 *
		 * A synchronized block in Java is synchronized on some object.
		 * All synchronized blocks synchronized on the same object can only have one thread executing
		 * inside them at the same time. All other threads attempting to enter the synchronized block are
		 * blocked until the thread inside the synchronized block exits the block.
		 * Refer here for details: https://docs.oracle.com/javase/tutorial/essential/concurrency/locksync.html
		*/
		synchronized (this) {
			// MatrixCursor is useful if you have a collection of data that is not in the database,
			// and you want to create a cursor for it.  You simply construct it with an array of
			// column names, and optionally an initial capacity. Then, you can add one row at a
			// time to it by passing either an array of objects or an Iterable to its addRow() method.
			// Refer: https://developer.android.com/reference/android/database/MatrixCursor.html
			MatrixCursor cursor = new MatrixCursor(new String[] {"key", "value"});
			ConcurrentHashMap<String, String> map = null;
			int port = 0;

			if (selection.equals("@")) {
				// Handling local queries @
				for (int i = 0; i < num_replicas; i++) {
					for (Entry<String, String> entry : replicaPtr.get(i).entrySet()) {
						String[] row_temp = new String[] {entry.getKey(), entry.getValue()};
						cursor.addRow(row_temp);
					}
				}
			} else if (selection.equals("*")) {
				// Handling all queries *
				hashTable.clear();
				Vector<Integer> ports = ringPartition.fetchAllPorts();

				for (Integer portIter : ports) {
					if (portIter == currPort) {
						hashTable.putAll(replicaPtr.get(num_replicas - 1));
						waitNotifier.incrementAndGet();
					} else {
						requestRouter(null, num_replicas - 1, MessageType.FETCH_ALL, portIter);
					}
				}
				synchronized (waitNotifier) {
					try {
						waitNotifier.wait();
					} catch (InterruptedException e) {
						Log.e("QUERY_ALL_INTERRUPT", "InterruptedException: Query all interrupted "
								+ e.getMessage());
					}
				}
				for (Entry<String, String> entry : hashTable.entrySet()) {
					String[] row_temp = new String[] {entry.getKey(), entry.getValue()};
					cursor.addRow(row_temp);
				}
			} else {
				hashTable.clear();
				try {
					port = ringPartition.getQueryPort(genHash(selection));
				} catch (NoSuchAlgorithmException e) {
					Log.e("QUERY_HASH_ERR", "NoSuchAlgorithmException: " + e.getMessage());
				}
				if (port == currPort) {
					String value = replicaPtr.get(num_replicas - 1).get(selection);
					if (value == null) {
						Object val_obj = waitList.get(selection);
						if (val_obj == null) {
							val_obj = new Object();
							waitList.put(selection, val_obj);
						}
						synchronized (val_obj) {
							try {
								val_obj.wait();
							} catch (InterruptedException e) {
								Log.e("QUERY_KEY_INTERRUPT", "InterruptedException: "
										+ e.getMessage());
							}
						}
						value = replicaPtr.get(num_replicas - 1).get(selection);
					}
					String[] row_temp = new String[] {selection, value};
					cursor.addRow(row_temp);
				} else {
					map = new ConcurrentHashMap<String, String>();
					map.put(selection, "");
					requestRouter(map, num_replicas - 1, MessageType.QUERY, port);
					synchronized (hashTable) {
						try {
							hashTable.wait();
						} catch (InterruptedException e) {
							Log.e("HASHTABLE_Q_INTERRUPT", "InterruptedException: " + e.getMessage());
						}
					}
					String[] row = null;
					for (Entry<String, String> entry : hashTable.entrySet()) {
						row = new String[] {
								entry.getKey(), entry.getValue()
						};
						cursor.addRow(row);
					}
				}
			}
			return cursor;
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		return 0;
	}

	@Override
	public boolean onCreate() {
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		TelephonyManager tel = (TelephonyManager)this.getContext().getSystemService(
				Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		currPort = Integer.parseInt(portStr) * 2;

		try {
			for (Map.Entry<String, Integer> entry : devicePortMap.entrySet()) {
				ringPartition.add(genHash(entry.getKey()), entry.getValue());
			}
		} catch (NoSuchAlgorithmException e) {
			Log.e("PARTITION_HASH_ERR", "NoSuchAlgorithmException: Ring Partitioning error " + e.getMessage());
			return false;
		}

		for (int i = 0; i < replicaPtr.capacity(); i++) {
			replicaPtr.add(i, new ConcurrentHashMap<String, String>());
		}

		SharedPreferences check_failure = this.getContext().getSharedPreferences("check_failure", 0);
		if (check_failure.getBoolean("init_boot", true)) {
			check_failure.edit().putBoolean("init_boot", false).commit();
		} else {
			is_recover = true;
		}

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e("SERVER_SOCKET_IO", "IOException: " + e.getMessage());
			return false;
		}

		if (is_recover) {
			int port = ringPartition.getNextPort(currPort);
			requestRouter(null, 2, MessageType.RECOVER, port);
			port = ringPartition.getNextPort(port);
			requestRouter(null, 2, MessageType.RECOVER, port);
			port = ringPartition.getPrevPort(currPort);
			requestRouter(null, 1, MessageType.RECOVER,port);
		}

		return true;
	}

	/* Server and Client Tasks can be written using an AsyncTask as we have been doing for earlier
     * assignments. In this assignment we shall use an AsyncTask to handle the server implementation
     * and simple Java Thread based approach for client tasks. I tried implementing AsyncTask for
     * Client tasks and routing requests, but faced issues on debugging. Hence I decided to try out
     * the basics of implementing a simple Java Thread based approach for client tasks.
     * The benefit of this method is in the use of synchronized blocks, which help us cope with
     * concurrency via locking mechanism.
     * https://docs.oracle.com/javase/tutorial/essential/concurrency/
     */

	// Server Implementation
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try {
				while(true) {
					Socket socket = serverSocket.accept();
					server_helper(socket);
				}
			} catch (IOException e) {
				Log.e("SERVER_IO", "IOException: " + e.getMessage());
			}
			return null;

		}
	}

	// Helper function implementing the server tasks.
	private void server_helper(final Socket socket) {
		Thread serverThread = new Thread(new Runnable() {
			public void run() {
				try {
					ObjectOutputStream out_stream = new ObjectOutputStream(socket.getOutputStream());
					out_stream.flush();
					ObjectInputStream in_stream = new ObjectInputStream(socket.getInputStream());
					Message message = (Message) in_stream.readObject();
					if (is_recover) {
						synchronized (waitNotifier) {
							try {
								waitNotifier.wait();
							} catch (InterruptedException e) {
								Log.e("SERVER_INTERRUPTED", "InterruptedException: Recover failed: "
										+ e.getMessage());
							}
						}
					}

					String key = "";
					switch(message.operation) {
						case INSERT:
							// Handle inserting new message in the DHT
							replicaPtr.get(message.index).putAll(message.map);
							if (waitList.size() > 0) {
								key = message.map.keySet().iterator().next();
								Object val_obj = waitList.get(key);
								if (val_obj != null) {
									waitList.remove(key);
									synchronized (val_obj) {
										val_obj.notifyAll();
									}
								}
							}
							if (message.forward) {
								int port = ringPartition.getNextPort(currPort);
								int index = message.index + 1;
								requestRouter(message.map, index, message.operation, port);
							}
							if (message.is_skipped) {
								int port = ringPartition.getPrevPort(currPort);
								int index = message.index - 1;
								requestRouter(message.map, index,MessageType.SEND_BACK, port);
							}
							out_stream.writeObject(new Message(0, null, MessageType.SUCCESS, false));
							break;
						case QUERY:
							// Handling querying the DHT. First we fetch the key, value pairs from
							// within the DHT. If value is not present currently, we pause the
							// thread until the value is accessible to all the nodes
							key = message.map.keySet().iterator().next();
							String value = replicaPtr.get(message.index).get(key);
							if (value == null) {
								Object val_obj = waitList.get(key);
								if (val_obj == null) {
									val_obj = new Object();
									waitList.put(key, val_obj);
								}
								synchronized (val_obj) {
									try {
										val_obj.wait();
									} catch (InterruptedException e) {
										Log.e("SERVER_INTERRUPT", "InterruptedException: QUERY - " +
												"Waiting for key failed: " + e.getMessage());
									}
								}
								value = replicaPtr.get(message.index).get(key);
							}
							message.map.put(key, value);
							out_stream.writeObject(message);
							break;
						case FETCH_ALL :
							// Handles fetch all queries over the distributed hash table created.
							message.map = replicaPtr.get(message.index);
							out_stream.writeObject(message);
							break;
						case DELETE :
							// Handles the deletion of data from our distributed hashtable. If the
							// forward variable is set, then we send this message to the next port
							// before destroying the message.
							key = message.map.keySet().iterator().next();
							replicaPtr.get(message.index).remove(key);
							if (message.forward) {
								int port = ringPartition.getNextPort(currPort);
								int index = message.index + 1;
								requestRouter(message.map, index, message.operation, port);
							}
							Message temp_msg = new Message(0, null,MessageType.SUCCESS, false);
							out_stream.writeObject(temp_msg);
							break;
						default:
							message.map = replicaPtr.get(message.index);
							out_stream.writeObject(message);

					}
					out_stream.close();
				} catch (ClassNotFoundException e) {
					Log.e("SERVER_CLASS_NOT_FOUND", "ClassNotFoundException: ObjectInputStream error "
							+ e.getMessage());
				} catch (IOException e) {
					Log.e("SERVER_IO", "IOException: " + e.getMessage());
				}
			}
		});
		serverThread.start();
	}

	/*
	 * In Dynamo implementation, each node knows all other nodes in the system and also knows
	 * exactly which partition belongs to which node. Hence, under no failures, a request for a key
	 * is directly forwarded to the coordinator (i.e., the successor of the key), and the
	 * coordinator should be in charge of serving read/write operations.
	 * We implement this coordinator next, which implements Request routing.
	 */
	private void requestRouter(final ConcurrentHashMap<String, String> map, final int index,
							   final MessageType operation,final int port) {
		Thread routerThread = new Thread(new Runnable() {
			public void run() {
				if(operation == MessageType.QUERY)
					Log.i("QUERY_MESSAGE", "QUERY: " + map.keySet().iterator().next());
				else if(operation == MessageType.INSERT)
					Log.i("INSERT_MESSAGE", "INSERT: " + map.keySet().iterator().next());

				if (operation == MessageType.SEND_BACK) {
					if (ClientTask(map, index, MessageType.INSERT, port, false)) {
						Log.i("SEND_BACK_SUCCESS", "Successful send back operation");
					} else {
						Log.i("SEND_BACK_FAILURE", "Failed send back operation");
					}
				} else if (!ClientTask(map, index, operation, port, false)) {
					Log.e("NODE_FAILURE", "Node failed at port: " + port);

					if (operation == MessageType.QUERY || operation == MessageType.FETCH_ALL) {
						int newPort = ringPartition.getPrevPort(port);
						int newIndex = index - 1;

						if(!ClientTask(map, newIndex, operation, newPort, false)) {
							Log.e("QUERY_FAILURE", "INSERT Failed at port: " + port);
						}
					} else if (operation == MessageType.INSERT || operation == MessageType.DELETE) {
						if (index != num_replicas-1) {
							int newPort = ringPartition.getNextPort(port);
							int newIndex = index + 1;

							if(!ClientTask(map, newIndex, operation, newPort, true)) {
								Log.e("INSERT_FAILURE", "INSERT Failed at port: " + port);
							}
						}
					} else {
						Log.e("CLIENT_RECOVERY_FAILED", "Failed recovery port: " + port);
					}
				}
			}
		});
		routerThread.start();

		if (operation == MessageType.INSERT || operation == MessageType.DELETE) {
			try {
				routerThread.join();
			} catch (InterruptedException e) {
				Log.e("THEAD_JOIN_INTERRUPT", "InterruptedException: "+ e.getMessage());
			}
		}
	}

	// Client Implementation
	private boolean ClientTask(final ConcurrentHashMap<String, String> map, final int index,
							   final MessageType operation,final int port, boolean skipped) {
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[] {10, 0, 2, 2}), port);
			socket.setSoTimeout(TIMEOUT);
			ObjectOutputStream out_stream = null;
			ObjectInputStream in_stream = null;

			boolean forward = false;
			if (index < num_replicas-1 && (operation == MessageType.INSERT ||
					operation == MessageType.DELETE)) {
				forward = true;
			}
			Message message = new Message(index, map, operation, forward);
			message.is_skipped = skipped;
			// Write Message
			try {
				out_stream = new ObjectOutputStream(socket.getOutputStream());
				out_stream.writeObject(message);
			} catch (IOException e) {
				Log.e("CLIENT_IO", "IOException: Write failed: "+ currPort +":" + port + e.getMessage());
				socket.close();
				return false;
			}

			// Read Message
			try {
				in_stream = new ObjectInputStream(socket.getInputStream());;
				message = (Message) in_stream.readObject();
			} catch (IOException e) {
				Log.e("CLIENT_IO", "IOException: Read failed: "+ currPort +":" + port + e.getMessage());
				socket.close();
				return false;
			}

			out_stream.close();
			socket.close();

			// Handle different types of messages
			switch(operation) {
				case QUERY:
					hashTable.putAll(message.map);
					synchronized (hashTable) {
						hashTable.notify();
					}
					break;
				case FETCH_ALL:
					hashTable.putAll(message.map);
					if (waitNotifier.incrementAndGet() == ringPartition.getNumNodes()) {
						waitNotifier.set(0);
						synchronized (waitNotifier) {
							waitNotifier.notify();
						}
					}
					break;
				case RECOVER:
					if (port == ringPartition.getNextPort(currPort)) {
						replicaPtr.get(1).putAll(message.map);
					} else if (port == ringPartition.getPrevPort(currPort)) {
						replicaPtr.get(2).putAll(message.map);
					} else {
						replicaPtr.get(0).putAll(message.map);
					}

					if (waitNotifier.incrementAndGet() == num_replicas) {
						// reset recovery
						is_recover = false;
						// reset counter
						waitNotifier.set(0);
						synchronized (waitNotifier) {
							waitNotifier.notifyAll();
						}
					}
					break;
			}
		} catch (ClassNotFoundException e) {
			Log.e("CLIENT_CLASS_NOT_FOUND", "ClassNotFoundException: " + e.getMessage());
			return false;
		} catch (IOException e) {
			Log.e("CLIENT_IO", "IOException: " + e.getMessage());
			return false;
		}
		return true;
	}

	/*
     * Helper functions for hashing a key and building an URI for content provider.
     */
	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
}