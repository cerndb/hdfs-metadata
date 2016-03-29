package ch.cern.db.hdfs;

import java.util.HashMap;

import org.junit.Test;

public class MainTest {

	@Test
	public void printNodeDisksDistributionWithNumberOfDiskUnknown(){
		HashMap<String, HashMap<Integer, Integer>> hosts_diskIds = new HashMap<>();
		
		HashMap<Integer, Integer> node1 = new HashMap<>();
		node1.put(3, 1);
		node1.put(7, 3);
		hosts_diskIds.put("node1", node1);
		
		HashMap<Integer, Integer> node2 = new HashMap<>();
		node2.put(5, 1);
		node2.put(1, 30);
		node2.put(-1, 2);
		hosts_diskIds.put("node2", node2);
		
		Main.printNodeDisksDistribution(hosts_diskIds, -1);
	}
	
	@Test
	public void printNodeDisksDistributionWithDiskIdsUnknown(){
		HashMap<String, HashMap<Integer, Integer>> hosts_diskIds = new HashMap<>();
		
		HashMap<Integer, Integer> node1 = new HashMap<>();
		node1.put(-1, 7);
		hosts_diskIds.put("node1", node1);
		
		HashMap<Integer, Integer> node2 = new HashMap<>();
		node2.put(-1, 28);
		hosts_diskIds.put("node2", node2);
		
		Main.printNodeDisksDistribution(hosts_diskIds, -1);
	}
	
}
