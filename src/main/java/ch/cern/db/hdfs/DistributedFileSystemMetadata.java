/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.db.util.SUtils;
import ch.cern.db.util.SUtils.Color;
import ch.cern.db.util.Utils;

public class DistributedFileSystemMetadata extends DistributedFileSystem{
	
	private static final Logger LOG = LoggerFactory.getLogger(DistributedFileSystemMetadata.class);
	
	private static final int MAX_NUMBER_OF_LOCATIONS = 20000;

	public DistributedFileSystemMetadata() throws IOException{
		HdfsConfiguration conf = new HdfsConfiguration();
		
		initialize(getDefaultUri(conf), conf);
	}

	public String[] getDataNodes() {

		try {
			DatanodeInfo[] dataNodeStats = getDataNodeStats();
			
			String[] hosts = new String[dataNodeStats.length];
			for (int i = 0; i < hosts.length; i++)
				hosts[i] = dataNodeStats[i].getHostName();
			
			return hosts;
		} catch (IOException e) {
			LOG.warn("list of data nodes could not be got from API (requieres higher privilegies).");
		}
		
		try {
			LOG.warn("getting datanode list from configuration file (may contain data nodes which are not active).");
			return getDataNodesFromConf();
		} catch (IOException e) {
		}
		
		return new String[0];
	}

	private String[] getDataNodesFromConf() throws IOException {
		InputStream in = getClass().getResourceAsStream("/dfs.includes");
		if(in == null)
			throw new IOException("File dfs.includes not found in classpath");
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        
		LinkedList<String> hostnames = new LinkedList<>();
        String line;
        while ((line = reader.readLine()) != null) {
        	hostnames.add(line);
        }
        
        reader.close();
		
		return hostnames.toArray(new String[hostnames.size()]);
	}

	public void printBlockMetadata(LinkedList<BlockLocation> blockLocations, String[] dataDirs, int limitPrintedBlocks) 
			throws IOException {
		
		System.out.println();
		System.out.println(" === Metadata of blocks and replicass ===");
		System.out.println();
		
		int j = 0;
		for (BlockLocation blockLocation : blockLocations) {
			System.out.println("Block (" + j + ") info:");
			
			printBlockMetadata(blockLocation, dataDirs);
			
			j++;
			
			System.out.println();
			
			if(j >= limitPrintedBlocks){
				LOG.warn("Not showing more blocks because limit has been reached");
				break;
			}
		}
		
		System.out.println();
	}

	public void printNodeDisksDistribution(HashMap<String, HashMap<Integer, Integer>> hosts_diskIds, String[] dataDirs) {
		System.out.println();
		System.out.println(" === Distribution along nodes and disks ===");
		System.out.println();
		
		if(dataDirs == null){
			LOG.error("Cannot be calculated due to data directories configuration parameter are not available.");
		}
		
		System.out.print(SUtils.adjustLength("DiskId", 25));
		if(dataDirs.length >= 10){
			for (int i = 0; i < dataDirs.length; i++) {
				System.out.print(SUtils.adjustLength(Integer.toString(i / 10), 2));
			}
			System.out.println();
			System.out.print(SUtils.adjustLength("", 25));
		}
		for (int i = 0; i < dataDirs.length; i++) {
			System.out.print(SUtils.adjustLength(Integer.toString(i % 10), 2));
		}
		System.out.print(SUtils.adjustLength("Unknown", 10));
		System.out.print(SUtils.adjustLength("Count", 10));
		System.out.println(SUtils.adjustLength("Average", 10));
		System.out.println("Host");
		for (Entry<String, HashMap<Integer, Integer>> host_diskIds : hosts_diskIds.entrySet()) {
			System.out.print(SUtils.adjustLength(host_diskIds.getKey(), 25));
			
			HashMap<Integer, Integer> diskIds_count = host_diskIds.getValue();
			
			float sum = 0;
			for (int i = 0; i < dataDirs.length; i++){
				Integer count = diskIds_count.get(i);
				
				if(count != null)
					sum += diskIds_count.get(i);
			}
			float avg = sum / dataDirs.length;
			
			float low = (float) (avg - 0.2 * avg);
			if(avg - low < 2)
				low = avg - 2;			
			float high = (float) (avg + 0.2 * avg);
			if(high - avg < 2)
				high = avg + 2;
			
			for (int i = 0; i < dataDirs.length; i++) {
				Integer count = diskIds_count.get(i);
				
				if(count == null)
					System.out.print(SUtils.color(Color.R, SUtils.adjustLength("0", 2)));
				else if(count < low)
					System.out.print(SUtils.color(Color.Y, SUtils.adjustLength("-", 2)));
				else if(count > high)
					System.out.print(SUtils.color(Color.Y, SUtils.adjustLength("+", 2)));
				else
					System.out.print(SUtils.color(Color.G, SUtils.adjustLength("=", 2)));
			}
			
			Integer count_unk = diskIds_count.get(-1);
			if(count_unk == null)
				System.out.print(SUtils.color(Color.G, SUtils.adjustLength("0", 10)));
			else
				System.out.print(SUtils.color(Color.R, SUtils.adjustLength(count_unk+"", 10)));
			
			System.out.print(SUtils.adjustLength((int)sum+"", 10));
			System.out.println((int) avg);
		}
		
		System.out.println();
		System.out.println("Leyend");
		System.out.println("  " + SUtils.color(Color.R,  "0") + ": no blocks in this disk");
		System.out.println("  " + SUtils.color(Color.Y,  "+") + ": #blocks is more than 20% of the avergae of blocks per disk of this host");
		System.out.println("  " + SUtils.color(Color.G, "=") + ": #blocks is aproximatilly the avergae of blocks per disk of this host");
		System.out.println("  " + SUtils.color(Color.Y,  "-") + ": #blocks is less than 20% of the avergae of blocks per disk of this host");
	}

	public String[] getDataDirs() {
		
		//Proper way would be to get it from each node
		//For cluster with same configuration this method is fine
		String dataDirsParam = getConf().get("dfs.data.dir");
		if(dataDirsParam == null) 
			dataDirsParam = getConf().get("dfs.datanode.data.dir");
		
		String[] dataDirs = null;
		
		if(dataDirsParam == null){
			LOG.warn("dfs.data.dir or dfs.datanode.data.dir "
					+ "cofiguration parameter is not set, so block distribution can not be shown.");
		}else{
			dataDirs = dataDirsParam.split(",");
		}
		
		return dataDirs;
	}

	public HashMap<String, HashMap<Integer, Integer>> calculateHostsDiskIdsCount(
			BlockStorageLocation[] blockStorageLocations) throws IOException {
		
		HashMap<String, HashMap<Integer, Integer>> hosts_diskIds = new HashMap<>(); 
		for (BlockStorageLocation blockStorageLocation : blockStorageLocations) {
			String[] hosts = blockStorageLocation.getHosts();
			VolumeId[] volumeIds = blockStorageLocation.getVolumeIds();
			
			for (int i = 0; i < hosts.length; i++) {
				String host = hosts[i];
				Integer diskId = getDiskId(volumeIds[0]);
				
				if(!hosts_diskIds.containsKey(host)){
					HashMap<Integer, Integer> diskIds = new HashMap<>();
					diskIds.put(diskId, 1);
					hosts_diskIds.put(host, diskIds);
				}else{
					HashMap<Integer, Integer> diskIds = hosts_diskIds.get(host);
					Integer count = diskIds.get(diskId + 1);
					if(count != null){
						diskIds.put(diskId, count + 1);
					}else{
						diskIds.put(diskId, 1);
					}
				}
			}
		}
		
		return hosts_diskIds;
	}

	public void printFileStatus(Path path) throws IOException {
		FileStatus status = getFileStatus(path);

		System.out.println();
		System.out.println("Showing metadata for: " + status.getPath());
		System.out.println("	isDirectory: " + status.isDirectory());
		System.out.println("	isFile: " + status.isFile());
		System.out.println("	isSymlink: " + status.isSymlink());
		System.out.println("	encrypted: " + status.isEncrypted());
		System.out.println("	length: " + status.getLen());
		System.out.println("	replication: " + status.getReplication());
		System.out.println("	blocksize: " + status.getBlockSize());
		System.out.println("	modification_time: " + new Date(status.getModificationTime()));
		System.out.println("	access_time: " + new Date(status.getAccessTime()));
		System.out.println("	owner: " + status.getOwner());
		System.out.println("	group: " + status.getGroup());
		System.out.println("	permission: " + status.getPermission());
		System.out.println();
	}

	public LinkedList<BlockLocation> getBlockLocations(Path path) throws IOException {
		System.out.println("Collecting block locations...");
		
		LinkedList<BlockLocation> blockLocations = new LinkedList<BlockLocation>();
		RemoteIterator<LocatedFileStatus> statuses = listFiles(path, true);
		while(statuses.hasNext()){
			LocatedFileStatus fileStatus = statuses.next();
			
			if(fileStatus.isFile()){
				BlockLocation[] blockLocations_tmp = getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
				
				blockLocations.addAll(Arrays.asList(blockLocations_tmp));
			}
			
			int size = blockLocations.size();
			if(size > 0 && size % 5000 == 0)
				System.out.println("Collected " + size + " locations. Still in progress...");
			
			if(size >= MAX_NUMBER_OF_LOCATIONS){
				System.out.println("Reached max number of locations to collect. The amount will be representative enough.");
				break;
			}
		}
		System.out.println("Collected " + blockLocations.size() + " locations.");
		System.out.println();
		
		return blockLocations;
	}

	public void printBlockMetadata(BlockLocation blockStorageLocation, String[] dataDirs) 
			throws IOException {
		
		System.out.println("	Offset: " + blockStorageLocation.getOffset());
		System.out.println("	Length: " + blockStorageLocation.getLength());

		String[] cachedHosts = blockStorageLocation.getCachedHosts();
		if (cachedHosts.length == 0) {
			System.out.println("	No cached hosts");
		}

		System.out.println("	Hosts:");
		VolumeId[] volumeIds = blockStorageLocation instanceof BlockStorageLocation ?
				(((BlockStorageLocation) blockStorageLocation).getVolumeIds()) : null;
		String[] hosts = blockStorageLocation.getHosts();
		String[] names = blockStorageLocation.getNames();
		String[] topologyPaths = blockStorageLocation.getTopologyPaths();
		for (int i = 0; i < topologyPaths.length; i++) {
			int diskId = volumeIds != null ? getDiskId(volumeIds[i]) : -1;
			
			System.out.println("		Replica (" + i + "):");
			System.out.println("			Host: " + hosts[i]);
			
			if(diskId == -1)
				System.out.println("			DiskId: unknown");
			else if(dataDirs != null && diskId < dataDirs.length)
				System.out.println("			Location: " + dataDirs[diskId] + " (DiskId: " + diskId + ")");
			else
				System.out.println("			DiskId: " + diskId);
			
			System.out.println("			Name: " + names[i]);
			System.out.println("			TopologyPaths: " + topologyPaths[i]);
		}

		if (cachedHosts.length > 0) {
			System.out.println("	Cached hosts:");
			for (String cachedHost : cachedHosts) {
				System.out.println("		Host: " + cachedHost);
			}
		}
	}

	/**
	 * Returns a disk id (0-based) index from the Hdfs VolumeId object. There is
	 * currently no public API to get at the volume id. We'll have to get it by
	 * accessing the internals.
	 */
	public int getDiskId(VolumeId hdfsVolumeId){
		// Initialize the diskId as -1 to indicate it is unknown
		int diskId = -1;

		if (hdfsVolumeId != null) {
			String volumeIdString = hdfsVolumeId.toString();

			byte[] volumeIdBytes = StringUtils.hexStringToByte(volumeIdString);
			if (volumeIdBytes != null && volumeIdBytes.length == 4) {
				diskId = Utils.toInt(volumeIdBytes);
			}else if (volumeIdBytes.length == 1) {
				diskId = (int) volumeIdBytes[0];  // support hadoop-2.0.2
	        }
		}

		return diskId;
	}

}
