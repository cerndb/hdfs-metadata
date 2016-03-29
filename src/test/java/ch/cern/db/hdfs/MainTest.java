package ch.cern.db.hdfs;

import java.util.HashMap;

import org.junit.Test;

public class MainTest {

	@Test
	public void printNodeDisksDistribution(){
		HashMap<String, HashMap<Integer, Integer>> hosts_diskIds = new HashMap<>();
		
		HashMap<Integer, Integer> node1 = new HashMap<>();
		node1.put(3, 1);
		node1.put(7, 3);
		hosts_diskIds.put("node1", node1);
		
		HashMap<Integer, Integer> node2 = new HashMap<>();
		node1.put(5, 1);
		node1.put(1, 30);
		hosts_diskIds.put("node2", node2);
		
		Main.printNodeDisksDistribution(hosts_diskIds, -1);
	}
	
}
