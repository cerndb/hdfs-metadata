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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
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
			LOG.warn(e.getMessage());
		}
		
		LOG.warn("No list of data nodes found");
		
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

	public String[] getDataDirs() {
		
		//Proper way would be to get it from each node
		//For clusters with same configuration this method is fine
		String dataDirsParam = getConf().get("dfs.data.dir");
		if(dataDirsParam == null) 
			dataDirsParam = getConf().get("dfs.datanode.data.dir");
		
		String[] dataDirs = null;
		
		if(dataDirsParam == null){
			LOG.warn("dfs.data.dir or dfs.datanode.data.dir cofiguration parameter is not set");
		}else{
			dataDirs = dataDirsParam.split(",");
		}
		
		return dataDirs;
	}

	public static HashMap<String, HashMap<Integer, Integer>> computeHostsDiskIdsCount(
			List<BlockStorageLocation> blockStorageLocations) throws IOException {
		
		HashMap<String, HashMap<Integer, Integer>> hosts_diskIds = new HashMap<>(); 
		for (BlockStorageLocation blockStorageLocation : blockStorageLocations) {
			String[] hosts = blockStorageLocation.getHosts();
			VolumeId[] volumeIds = blockStorageLocation.getVolumeIds();
			
			for (int i = 0; i < hosts.length; i++) {
				String host = hosts[i];
				Integer diskId = getDiskId(volumeIds[i]);
				
				if(!hosts_diskIds.containsKey(host)){
					HashMap<Integer, Integer> diskIds = new HashMap<>();
					diskIds.put(diskId, 1);
					hosts_diskIds.put(host, diskIds);
				}else{
					HashMap<Integer, Integer> diskIds = hosts_diskIds.get(host);
					Integer count = diskIds.get(diskId);
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

	public LinkedList<BlockLocation> getBlockLocations(Path path) throws IOException {
		LOG.info("Collecting block locations...");
		
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
				LOG.info("Collected " + size + " locations. Still in progress...");
			
			if(size >= MAX_NUMBER_OF_LOCATIONS){
				LOG.info("Reached max number of locations to collect. The amount will be representative enough.");
				break;
			}
		}
		LOG.info("Collected " + blockLocations.size() + " locations.");
		
		if(isHdfsBlocksBetadataEnabled()){
			BlockStorageLocation[] blockStorageLocations = getFileBlockStorageLocations(blockLocations);
			
			blockLocations.clear();
			blockLocations.addAll(Arrays.asList(blockStorageLocations));
		}else{
			LOG.error("VolumnId/DiskId can not be collected since "
					+ "dfs.datanode.hdfs-blocks-metadata.enabled is not enabled.");
		}
		
		return blockLocations;
	}

	public boolean isHdfsBlocksBetadataEnabled() {
		return getConf().getBoolean("dfs.datanode.hdfs-blocks-metadata.enabled", false);
	}

	/**
	 * Returns a disk id (0-based) index from the Hdfs VolumeId object. There is
	 * currently no public API to get at the volume id. We'll have to get it by
	 * accessing the internals.
	 */
	public static int getDiskId(VolumeId hdfsVolumeId){
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
