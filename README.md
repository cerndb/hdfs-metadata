Tool for gathering blocks and replicas meta data from HDFS. It also builds a heat map showing how replicas are distributed along disks and nodes.

Find detailed information at a CERN DB blog entry dedicated to this tool: [http://db-blog.web.cern.ch/blog/daniel-lanza-garcia/2016-04-tool-visualise-block-distribution-hadoop-hdfs-cluster]

Build project:

```
bin/compile
```

Usage ([Hadoop generic options](https://hadoop.apache.org/docs/r2.6.4/hadoop-project-dist/hadoop-common/CommandsManual.html#Generic_Options) can be passed):

``` 
bin/hdfs-blkd [-Dhdfs.tool.blocks.dumping.path=<FILE_PATH>] <path> [<max_number_of_blocks_to_print>]
```

If hdfs.tool.blocks.dumping.path configuration parameter is set, block distribution will be dump to specified path in CSV format.

Example output:

```
[user@machine-1]$ bin/hdfs-blkd /user/ 2

Showing metadata for: hdfs://machine-1.cern.ch/user/
	isDirectory: true
	isFile: false
	isSymlink: false
	encrypted: false
	length: 0
	replication: 0
	blocksize: 0
	modification_time: Tue Mar 15 13:48:08 CET 2016
	access_time: Thu Jan 01 01:00:00 CET 1970
	owner: user
	group: supergroup
	permission: rwxr-xr-x

Collecting block locations...
Collected 269 locations.

Data directories and disk ids
  DiskId: 0  Directory: FILE:/data01/hadoop/hdfs/data
  DiskId: 1  Directory: FILE:/data1/hadoop/hdfs/data
  DiskId: 2  Directory: FILE:/data10/hadoop/hdfs/data
  DiskId: 3  Directory: FILE:/data11/hadoop/hdfs/data
  DiskId: 4  Directory: FILE:/data12/hadoop/hdfs/data
  DiskId: 5  Directory: FILE:/data13/hadoop/hdfs/data
  DiskId: 6  Directory: FILE:/data14/hadoop/hdfs/data
  DiskId: 7  Directory: FILE:/data15/hadoop/hdfs/data
  DiskId: 8  Directory: FILE:/data16/hadoop/hdfs/data
  DiskId: 9  Directory: FILE:/data17/hadoop/hdfs/data
  DiskId: 10  Directory: FILE:/data18/hadoop/hdfs/data
  DiskId: 11  Directory: FILE:/data19/hadoop/hdfs/data
  DiskId: 12  Directory: FILE:/data2/hadoop/hdfs/data
  DiskId: 13  Directory: FILE:/data20/hadoop/hdfs/data
  DiskId: 14  Directory: FILE:/data21/hadoop/hdfs/data
  DiskId: 15  Directory: FILE:/data22/hadoop/hdfs/data
  DiskId: 16  Directory: FILE:/data23/hadoop/hdfs/data
  DiskId: 17  Directory: FILE:/data24/hadoop/hdfs/data
  DiskId: 18  Directory: FILE:/data25/hadoop/hdfs/data
  DiskId: 19  Directory: FILE:/data26/hadoop/hdfs/data
  DiskId: 20  Directory: FILE:/data27/hadoop/hdfs/data
  DiskId: 21  Directory: FILE:/data28/hadoop/hdfs/data
  DiskId: 22  Directory: FILE:/data29/hadoop/hdfs/data
  DiskId: 23  Directory: FILE:/data3/hadoop/hdfs/data
  DiskId: 24  Directory: FILE:/data30/hadoop/hdfs/data
  DiskId: 25  Directory: FILE:/data31/hadoop/hdfs/data
  DiskId: 26  Directory: FILE:/data32/hadoop/hdfs/data
  DiskId: 27  Directory: FILE:/data33/hadoop/hdfs/data
  DiskId: 28  Directory: FILE:/data34/hadoop/hdfs/data
  DiskId: 29  Directory: FILE:/data35/hadoop/hdfs/data
  DiskId: 30  Directory: FILE:/data36/hadoop/hdfs/data
  DiskId: 31  Directory: FILE:/data37/hadoop/hdfs/data
  DiskId: 32  Directory: FILE:/data38/hadoop/hdfs/data
  DiskId: 33  Directory: FILE:/data39/hadoop/hdfs/data
  DiskId: 34  Directory: FILE:/data4/hadoop/hdfs/data
  DiskId: 35  Directory: FILE:/data40/hadoop/hdfs/data
  DiskId: 36  Directory: FILE:/data41/hadoop/hdfs/data
  DiskId: 37  Directory: FILE:/data42/hadoop/hdfs/data
  DiskId: 38  Directory: FILE:/data43/hadoop/hdfs/data

16/03/17 17:41:57 WARN DistributedFileSystemMetadata: list of data nodes could not be got from API (requires higher privileges).
16/03/17 17:41:57 WARN DistributedFileSystemMetadata: getting data node list from configuration file (may contain data nodes which are not active).

 === Distribution across nodes and disks ===

DiskId                   0 0 0 0 0 0 0 0 0 0 1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3 3 3 3 3 3 3 3 
                         0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 Unknown   Count     Average
Host
machine-1.cern.ch        0 = 0 = 0 = = = = 0 0 = 0 = 0 = = 0 = = + = = = + + = = = = = 0 = = + = = = = 0         77        1
machine-2.cern.ch        0 = + = = = 0 = = 0 0 = 0 = = = 0 = 0 = = = = 0 = = = 0 = 0 = 0 + = = = = = = 0         69        1
machine-3.cern.ch        0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0         0         0
machine-4.cern.ch        0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0         0         0
machine-5.cern.ch        0 = = = = = 0 = = = 0 0 0 = 0 = = 0 0 = = 0 = 0 0 0 = 0 = 0 = 0 0 0 0 = 0 0 0 0         29        0
machine-6.cern.ch        0 = = 0 = 0 0 = 0 = 0 = 0 = 0 0 = = = 0 0 = 0 = = + + = = = = = = 0 0 = = = 0 0         39        0
machine-7.cern.ch        0 = = = 0 = = = = 0 = = = = = 0 + + = = = = = = 0 = = = = = = = + = = = = 0 = 0         106       2
machine-8.cern.ch        0 = = 0 = 0 0 = = 0 0 = 0 = = = = = 0 = = + = = + = = = = = = = 0 = 0 = = 0 = 0         66        1
machine-9.cern.ch        0 0 = 0 = = 0 = = 0 = = = 0 = 0 = 0 = = = = 0 0 = 0 = = = = = 0 0 = 0 = = 0 0 0         33        0
machine-10.cern.ch       0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0         0         0
machine-11.cern.ch       0 0 = = = = = = = 0 = = 0 = = = = 0 = 0 = = = 0 = = = 0 = = = = = 0 0 = 0 = 0 0         56        1
machine-12.cern.ch       0 = 0 0 = 0 0 0 = = = 0 0 = = 0 = = = 0 = = = 0 = 0 0 = = = 0 0 0 0 0 0 = = 0 0         37        0
machine-13.cern.ch       0 0 0 = 0 = 0 = = 0 0 0 = 0 = = = = = 0 = = = = 0 = = = = 0 = = = 0 = = = = 0 0         68        1
machine-14.cern.ch       0 0 = 0 0 0 0 0 = 0 = 0 0 = = 0 = 0 = = = 0 = 0 = = = 0 = 0 0 0 0 0 0 0 = 0 0 0         26        0
machine-15.cern.ch       0 = = = = = 0 0 0 = = 0 = = 0 0 = 0 + = = = = = = = = 0 = = = = = 0 0 = = = = 0         61        1
machine-16.cern.ch       0 = = = = = 0 = = = 0 0 = 0 = = 0 = = 0 0 = = 0 = + = = = = = 0 = = = = = = 0 0         64        1
machine-17.cern.ch       0 0 = = 0 0 + = = 0 0 = = 0 0 = 0 0 = = = 0 = 0 0 = = 0 = = = = 0 0 = = = 0 = 0         42        0

Legend
  0: no blocks in this disk
  +: #blocks is more than 20% of the average of blocks per disk of this host
  =: #blocks is aproximatelly the average of blocks per disk of this host
  -: #blocks is less than 20% of the average of blocks per disk of this host
  
 === Metadata of blocks and replicas ===

Block (0) info:
	Offset: 0
	Length: 268435456
	No cached hosts
	Hosts:
		Replica (0):
			Host: machine-11.cern.ch
			Location: FILE:/data30/hadoop/hdfs/data (DiskId: 24)
			Name: 12.12.20.26:1004
			TopologyPaths: /0513_R-0050/RL05/12.12.20.26:1004
		Replica (1):
			Host: machine-5.cern.ch
			Location: FILE:/data2/hadoop/hdfs/data (DiskId: 12)
			Name: 12.2.10.32:1004
			TopologyPaths: /0513_R-0050/RL17/12.2.10.32:1004
		Replica (2):
			Host: machine-7.cern.ch
			Location: FILE:/data34/hadoop/hdfs/data (DiskId: 28)
			Name: 12.12.21.2:1004
			TopologyPaths: /0513_R-0050/RL17/12.12.21.2:1004

Block (1) info:
	Offset: 268435456
	Length: 268435456
	No cached hosts
	Hosts:
		Replica (0):
			Host: machine-2.cern.ch
			Location: FILE:/data19/hadoop/hdfs/data (DiskId: 11)
			Name: 12.12.2.26:1004
			TopologyPaths: /0513_R-0050/RL05/12.12.2.26:1004
		Replica (1):
			Host: machine-8.cern.ch
			Location: FILE:/data7/hadoop/hdfs/data (DiskId: 46)
			Name: 12.14.10.2:1004
			TopologyPaths: /0513_R-0050/RL21/12.14.10.2:1004
		Replica (2):
			Host: machine-16.cern.ch
			Location: FILE:/data25/hadoop/hdfs/data (DiskId: 18)
			Name: 12.12.0.25:1004
			TopologyPaths: /0513_R-0050/RL21/12.12.0.25:1004

16/03/17 17:41:57 WARN DistributedFileSystemMetadata: Not showing more blocks because limit has been reached
```