package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspace (because we are pretending different
 * replicas are like different keyspaces), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 * Faster
	 * is not necessarily better, so don't sweat speed. Focus on safety.
	 */
	private final Session session;
	private final Cluster cluster;
	public static final int SLEEP = 1000;

	/**
	 * All Gigapaxos apps must either support a no-args constructor or a
	 * constructor taking a String[] as the only argument. Gigapaxos relies on
	 * adherence to this policy in order to be able to reflectively construct
	 * customer application instances.
	 *
	 * @param args Singleton array whose args[0] specifies the keyspace in the
	 *             backend data store to which this server must connect.
	 *             Optional args[1] and args[2]
	 * @throws IOException
	 */
	public MyDBReplicableAppGP(String[] args) throws IOException {
		// TODO: setup connection to the data store and keyspace
		try {
				this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
				this.session = cluster.connect(args[0]);
		} catch (NoHostAvailableException e) {
				throw new IOException("Unable to connect to Cassandra", e);
		}
	}

	/**
	 * Refer documentation of {@link Replicable#execute(Request, boolean)} to
	 * understand what the boolean flag means.
	 * <p>
	 * You can assume that all requests will be of type {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket}.
	 *
	 * @param request
	 * @param b
	 * @return
	 */
	@Override
	public boolean execute(Request request, boolean b) {
		// TODO: submit request to data store
		// if request is not an instance of {@link* edu.umass.cs.gigapaxos.paxospackets.RequestPacket}, return false as the request cannot be executed
		if (!(request instanceof RequestPacket)) {
			return false;
		}
		// Cast the request to type RequestPacket
		RequestPacket requestPacket = (RequestPacket) request;
		try {
				// Extract the CQL query from the request
				String cqlQuery = requestPacket.requestValue;
				// Execute the Cassandra query
				session.execute(cqlQuery);
				// Return true upon sucessfully running the query
				return true;
		} catch (Exception e) {
				// Return false upon error
				e.printStackTrace();
				return false;
		}
	}

	/**
	 * Refer documentation of
	 * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
	 *
	 * @param request
	 * @return
	 */
	@Override
	public boolean execute(Request request) {
		// TODO: execute the request by sending it to the data store
		return execute(request, false);
	}

	/**
	 * Refer documentation of {@link Replicable#checkpoint(String)}.
	 *
	 * @param s
	 * @return
	 */
	@Override
	public String checkpoint(String s) {
		// TODO:
		try {
				// StringBuilder variable to hold the data state 
				StringBuilder serializedState = new StringBuilder();
				// Selecting all rows from the Cassandra table and storing the results in a ResultSet
				ResultSet results = session.execute("SELECT * FROM grade;");
				// Loop through all rows to serialize the results
				for (Row row : results) {
						int id = row.getInt("id");
						List<Integer> events = row.getList("events", Integer.class);
						// Construct the serialized state string with key - value pair from cassandra table
						serializedState.append(id).append(":").append(events.toString()).append("\n");
				}
				// Return the serialized string as the state of the database
				return serializedState.toString();
		} catch (Exception e) {
				e.printStackTrace();
				return null;
		}
	}

	/**
	 * Refer documentation of {@link Replicable#restore(String, String)}
	 *
	 * @param s
	 * @param s1
	 * @return
	 */
	@Override
	public boolean restore(String s, String s1) {
		// TODO:
		// Truncate the whole table if the state to be restored to is an empty state (no key - value pairs)
		if ("{}".equals(s1.trim())) {
			session.execute("TRUNCATE grade;");
			return true;
		}

		try {
				// Clear current table 
				session.execute("TRUNCATE grade;");
				// Split the rows of the serialized database
				String[] rows = s1.split("\n");
				for (String rowData : rows) {
						// Split key and value pairs
						String[] parts = rowData.split(":");
						// Retrieve the id (key)
						int id = Integer.parseInt(parts[0]);
						// Parsing the events list
						List<Integer> events = parseEventsList(parts[1]);
						// Query to Insert key-value pair into table
						String cqlInsert = "INSERT INTO grade (id, events) VALUES (" + id + ", " + events + ");";
						// Run the query
						session.execute(cqlInsert);
				}
				// Returns true if all row insertion is successful
				return true;
		} catch (Exception e) {
				// Return false if there are any errors
				e.printStackTrace();
				return false;
		}
	}

	private List<Integer> parseEventsList(String eventsStr) {
			// Parse the events list from the string
			// System.out.println("Parsing events list: " + eventsStr);
			// eventsStr is in the format: [event1, event2, ...]
			String[] eventItems = eventsStr.replaceAll("\\[", "").replaceAll("\\]","").split(", ");
			List<Integer> events = new ArrayList<>();
			for (String item : eventItems) {
					if (!item.isEmpty()) {
							events.add(Integer.parseInt(item.trim()));
					}
			}
			return events;
	}

	/**
	 * No request types other than {@link edu.umass.cs.gigapaxos.paxospackets
	 * .RequestPacket will be used by Grader, so you don't need to implement
	 * this method.}
	 *
	 * @param s
	 * @return
	 * @throws RequestParseException
	 */
	@Override
	public Request getRequest(String s) throws RequestParseException {
		return null;
	}

	/**
	 * @return Return all integer packet types used by this application. For an
	 * example of how to define your own IntegerPacketType enum, refer {@link
	 * edu.umass.cs.reconfiguration.examples.AppRequest}. This method does not
	 * need to be implemented because the assignment Grader will only use
	 * {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket} packets.
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>();
	}
}
