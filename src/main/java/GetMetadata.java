import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.StringUtils;

public class GetMetadata {
	
	public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

	public static void main(String[] args) throws IOException {

		if (args.length != 1) {
			System.err.println("You need to specify a path as first argument");
			System.exit(1);
		}

		Path path = new Path(args[0]);
		HdfsConfiguration conf = new HdfsConfiguration();
		
		//Proper way would be to get it from each node
		//Cluster with same configuration this is fine
		String dataDirsParam = conf.get("dfs.data.dir");
		if(dataDirsParam == null) 
			dataDirsParam = conf.get("dfs.datanode.data.dir");
		String[] dataDirs = null;
		if(dataDirsParam == null){
			System.err.println("WARNING: dfs.data.dir or dfs.datanode.data.dir cofiguration parameter is not set, so location for replicas will not be shown.");
		}else{
			dataDirs = dataDirsParam.split(",");
		}
		
		FileSystem fs = FileSystem.get(conf);
		FileStatus status = fs.getFileStatus(path);

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
		
		if(!status.isFile()){
			System.out.println("Block/replicas meta data is only shown for files.");
			System.out.println();
			return;
		}
		
		BlockLocation[] locations = fs.getFileBlockLocations(status, 0, status.getLen());
		BlockStorageLocation[] blockStorageLocations = ((DistributedFileSystem) fs)
				.getFileBlockStorageLocations(Arrays.asList(locations));
		
		for (int j = 0; j < blockStorageLocations.length; j++) {
			System.out.println("Block (" + j + ") info:");
			
			printBlockMetadata(blockStorageLocations[j], dataDirs);
			
			System.out.println();
		}
	}

	private static void printBlockMetadata(BlockStorageLocation blockStorageLocation, String[] dataDirs) 
			throws IOException {
		
		System.out.println("	Offset: " + blockStorageLocation.getOffset());
		System.out.println("	Length: " + blockStorageLocation.getLength());

		String[] cachedHosts = blockStorageLocation.getCachedHosts();
		if (cachedHosts.length == 0) {
			System.out.println("	No cached hosts");
		}

		System.out.println("	Hosts:");
		VolumeId[] volumeIds = blockStorageLocation.getVolumeIds();
		String[] hosts = blockStorageLocation.getHosts();
		String[] names = blockStorageLocation.getNames();
		String[] topologyPaths = blockStorageLocation.getTopologyPaths();
		for (int i = 0; i < topologyPaths.length; i++) {
			int diskId = getDiskId(volumeIds[i]);
			
			System.out.println("		Replica (" + i + "):");
			System.out.println("			Host: " + hosts[i]);
			
			if(dataDirs != null && diskId < dataDirs.length)
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
	private static int getDiskId(VolumeId hdfsVolumeId){
		// Initialize the diskId as -1 to indicate it is unknown
		int diskId = -1;

		if (hdfsVolumeId != null) {
			String volumeIdString = hdfsVolumeId.toString();

			byte[] volumeIdBytes = StringUtils.hexStringToByte(volumeIdString);
			if (volumeIdBytes != null && volumeIdBytes.length == 4) {
				diskId = toInt(volumeIdBytes);
			}else if (volumeIdBytes.length == 1) {
				diskId = (int) volumeIdBytes[0];  // support hadoop-2.0.2
	        }
		}

		return diskId;
	}

	/**
	 * Converts a byte array to an int value
	 * 
	 * @param bytes
	 *            byte array
	 * @return the int value
	 * @throws IllegalArgumentException
	 *             if length is not {@link #SIZEOF_INT}
	 */
	public static int toInt(byte[] bytes) {
		if (SIZEOF_INT > bytes.length)
			throw new IllegalArgumentException("length is not SIZEOF_INT");
		
		int n = 0;
		for (int i = 0; i < + bytes.length; i++) {
			n <<= 8;
			n ^= bytes[i] & 0xFF;
		}
		return n;
	}

}
