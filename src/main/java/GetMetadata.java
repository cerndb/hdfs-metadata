import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class GetMetadata {

	public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

	public static void main(String[] args) throws IOException {

		if(args.length != 1){
			System.out.println("You need to specify a file path as first argument");
			System.exit(1);
		}
		
		Path pt = new Path(args[0]);
		FileSystem fs = FileSystem.get(new Configuration());

		FileStatus status = fs.getFileStatus(pt);
		BlockLocation[] locations = fs.getFileBlockLocations(status, 0, status.getLen());

		BlockStorageLocation[] blockStorageLocations = ((DistributedFileSystem) fs)
				.getFileBlockStorageLocations(Arrays.asList(locations));

		for (int j = 0; j < blockStorageLocations.length; j++) {
			BlockStorageLocation blockStorageLocation = blockStorageLocations[j];

			System.out.println("Block (" + j + ") info:");
			System.out.println("	Offset: " + blockStorageLocation.getOffset());
			System.out.println("	Length: " + blockStorageLocation.getLength());
			System.out.println("	Hosts:");
			
			String[] cachedHosts = blockStorageLocation.getCachedHosts();
			if(cachedHosts.length == 0){
				System.out.println("	No cached hosts.");
			}
			
			VolumeId[] volumeIds = blockStorageLocation.getVolumeIds();
			String[] hosts = blockStorageLocation.getHosts();
			String[] names = blockStorageLocation.getNames();
			String[] topologyPaths = blockStorageLocation.getTopologyPaths();
			for (int i = 0; i < topologyPaths.length; i++) {
				System.out.println("		Replica (" + i + "):");
				System.out.println("			Host: " + hosts[i]);
				System.out.println("			VolumeId: " + volumeIds[i]);
				System.out.println("			Name: " + names[i]);
				System.out.println("			TopologyPaths: " + topologyPaths[i]);
			}
			
			if(cachedHosts.length > 0){
				System.out.println("	Cached hosts:");
				for (String cachedHost : cachedHosts) {
					System.out.println("		Host: " + cachedHost);
				}
			}
			
			System.out.println();
		}
	}

}
