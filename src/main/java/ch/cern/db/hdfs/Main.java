package ch.cern.db.hdfs;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

	private static final Logger LOG = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) throws IOException {

		if (args.length == 0 || args.length > 2) {
			LOG.error("You need to specify a path as first argument. ");
			System.exit(1);
		}
		
		int limitPrintedBlocks = 20;
		if(args.length == 2){
			limitPrintedBlocks = Integer.parseInt(args[1]);
		}
		
		Path path = new Path(args[0]);
		
		@SuppressWarnings("resource")
		DistributedFileSystemMetadata fsm = new DistributedFileSystemMetadata();
		
		fsm.printFileStatus(path);
		
		LinkedList<BlockLocation> blockLocations = fsm.getBlockLocations(path);
		
		String[] dataDirs = fsm.getDataDirs();
		if(dataDirs != null){
			System.out.println("Data directories and disk ids");
			for (int i = 0; i < dataDirs.length; i++) {
				System.out.println("  DiskId: " + i + "  Directory: " + dataDirs[i]);
			}
		}
		System.out.println();
		
		if(fsm.getConf().getBoolean("dfs.datanode.hdfs-blocks-metadata.enabled", false)){
			BlockStorageLocation[] blockStorageLocations = fsm.getFileBlockStorageLocations(blockLocations);
			
			HashMap<String, HashMap<Integer, Integer>> hosts_diskIds = 
					fsm.calculateHostsDiskIdsCount(blockStorageLocations);
			
			String[] dataNodes = fsm.getDataNodes();
			for (String name : dataNodes) {
				if(!hosts_diskIds.containsKey(name))
					hosts_diskIds.put(name, new HashMap<Integer, Integer>());
			}
			
			fsm.printNodeDisksDistribution(hosts_diskIds, dataDirs);
			
			blockLocations.clear();
			blockLocations.addAll(Arrays.asList(blockStorageLocations));
		}else{
			LOG.error("VolumnId/DiskId can not be collected since "
					+ "dfs.datanode.hdfs-blocks-metadata.enabled is not enabled. So block distribution can not be shown.");
		}
		
		fsm.printBlockMetadata(blockLocations, dataDirs, limitPrintedBlocks);
	}


}
