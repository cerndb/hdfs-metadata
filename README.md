Tool for gathering blocks and replicas meta data of a specific file.

Usage:

``` 
./run <path_to_file>
```

Example output:

```
[dlanza@itrac1511 hdfs-meta]$ ./run /user/hloader/SCADAR/VAC_LHC_LOAD.db/eventhistory_00100001_f/part-m-00000.avro

Showing information for: FileStatus{path=hdfs://p01001532067275.cern.ch/user/hloader/SCADAR/VAC_LHC_LOAD.db/eventhistory_00100001_f/part-m-00000.avro; isDirectory=false; length=9046982642; replication=3; blocksize=268435456; modification_time=1458029043727; access_time=1458140050321; owner=hloader; group=supergroup; permission=rw-r--r--; isSymlink=false}

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