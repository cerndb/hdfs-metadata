Tool for gathering blocks and replicas meta data from HDFS.

Usage:

``` 
./run <path>
```

Example output:

```
[dlanza@itrac1511 hdfs-meta]$ ./run /user/hloader/SCADAR/VAC_LHC_LOAD.db/eventhistory_00100001_f/part-m-00000.avro

Showing information fo filer: hdfs://p01001532067275.cern.ch/user/hloader/SCADAR/VAC_LHC_LOAD.db/eventhistory_00100001_f/part-m-00000.avro
	isDirectory: false
	isFile: true
	isSymlink: false
	encrypted: false
	length: 9046982642
	replication: 3
	blocksize: 268435456
	modification_time: Tue Mar 15 09:04:03 CET 2016
	access_time: Wed Mar 16 15:54:10 CET 2016
	owner: hloader
	group: supergroup
	permission: rw-r--r--

Block (0) info:
	Offset: 0
	Length: 268435456
	No cached hosts
	Hosts:
		Replica (0):
			Host: itrac1509.cern.ch
			Location: FILE:/data43/hadoop/hdfs/data (DiskId: 38)
			Name: 128.142.210.237:1004
			TopologyPaths: /0513_R-0050/RL21/128.142.210.237:1004
		Replica (1):
			Host: itrac1502.cern.ch
			Location: FILE:/data47/hadoop/hdfs/data (DiskId: 42)
			Name: 128.142.210.238:1004
			TopologyPaths: /0513_R-0050/RL17/128.142.210.238:1004
		Replica (2):
			Host: itrac1504.cern.ch
			Location: FILE:/data42/hadoop/hdfs/data (DiskId: 37)
			Name: 128.142.210.236:1004
			TopologyPaths: /0513_R-0050/RL17/128.142.210.236:1004

Block (1) info:
	Offset: 268435456
	Length: 268435456
	No cached hosts
	Hosts:
		Replica (0):
			Host: itrac1509.cern.ch
			Location: FILE:/data25/hadoop/hdfs/data (DiskId: 18)
			Name: 128.142.210.237:1004
			TopologyPaths: /0513_R-0050/RL21/128.142.210.237:1004
		Replica (1):
			Host: itrac1507.cern.ch
			...
```