/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.hdfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Assert;
import org.junit.Test;

import ch.cern.db.util.SUtils;

public class DistributedFileSystemMetadataTest extends DistributedFileSystem{	
	
	private class TVolumeId implements VolumeId{
		
		String hex;

		public TVolumeId(String hex) {
			this.hex = SUtils.adjustLength(hex, 8, '0', false);
		}
		
		@Override
		public int compareTo(VolumeId arg0) {
			return 0;
		}
		
		@Override
		public String toString() {
			return hex;
		}
		
	}

	@Test
	public void computeHostsDiskIdsCount() throws IOException{
		List<BlockLocation> blockStorageLocations = new LinkedList<>();
		blockStorageLocations.add(new BlockStorageLocation(
				new BlockLocation(null, new String[]{"host1", "host2"}, 0, 0), 
				new VolumeId[]{new TVolumeId("3"), new TVolumeId("4")}));
		blockStorageLocations.add(new BlockStorageLocation(
				new BlockLocation(null, new String[]{"host2", "host3"}, 0, 0), 
				new VolumeId[]{new TVolumeId("4"), new TVolumeId("5")}));
		blockStorageLocations.add(new BlockStorageLocation(
				new BlockLocation(null, new String[]{"host10", "host2"}, 0, 0), 
				new VolumeId[]{new TVolumeId("3"), new TVolumeId("4")}));
		blockStorageLocations.add(new BlockStorageLocation(
				new BlockLocation(null, new String[]{"host10", "host3"}, 0, 0), 
				new VolumeId[]{new TVolumeId("8"), new TVolumeId("5")}));
		blockStorageLocations.add(new BlockLocation(null, new String[]{"host10", "host3", "host3"}, 0, 0));
				
		HashMap<String, HashMap<Integer, Integer>> hosts_diskids = 
				DistributedFileSystemMetadata.computeHostsDiskIdsCount(blockStorageLocations);
		
		Assert.assertEquals(1, hosts_diskids.get("host1").get(3).intValue());
		Assert.assertEquals(3, hosts_diskids.get("host2").get(4).intValue());
		Assert.assertEquals(2, hosts_diskids.get("host3").get(5).intValue());
		Assert.assertEquals(2, hosts_diskids.get("host3").get(-1).intValue());
		Assert.assertEquals(1, hosts_diskids.get("host10").get(3).intValue());
		Assert.assertEquals(1, hosts_diskids.get("host10").get(8).intValue());
		Assert.assertEquals(1, hosts_diskids.get("host10").get(-1).intValue());
	}

	@Test
	public void getDiskId(){
		Assert.assertEquals(-1, DistributedFileSystemMetadata.getDiskId(null));
		
		Assert.assertEquals(1, DistributedFileSystemMetadata.getDiskId(new TVolumeId("1")));
		
		Assert.assertEquals(16, DistributedFileSystemMetadata.getDiskId(new TVolumeId("10")));
		
		Assert.assertEquals(27652112, DistributedFileSystemMetadata.getDiskId(new TVolumeId(Integer.toHexString(27652112))));
	}

}
