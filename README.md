Tool for gathering blocks and replicas meta data of a specific file.

Usage:

``` 
./run <path_to_file>
```

Example output:

```
./run /user/hloader/SCADAR/VAC_LHC_LOAD.db/eventhistory_00100001_f/part-m-00000.avro
Block (0) info:
	Offset: 0
	Length: 268435456
	Hosts:
	No cached hosts.
		Replica (0):
			Host: itrac1509.cern.ch
			VolumeId: 00000026
			Name: 128.142.210.237:1004
			TopologyPaths: /0513_R-0050/RL21/128.142.210.237:1004
		Replica (1):
			Host: itrac1502.cern.ch
			VolumeId: 0000002a
			Name: 128.142.210.238:1004
			TopologyPaths: /0513_R-0050/RL17/128.142.210.238:1004
		Replica (2):
			Host: itrac1504.cern.ch
			VolumeId: 00000025
			Name: 128.142.210.236:1004
			TopologyPaths: /0513_R-0050/RL17/128.142.210.236:1004

Block (1) info:
	Offset: 268435456
	Length: 268435456
	Hosts:
	No cached hosts.
		Replica (0):
			Host: itrac1509.cern.ch
			VolumeId: 00000012
			Name: 128.142.210.237:1004
			TopologyPaths: /0513_R-0050/RL21/128.142.210.237:1004
		Replica (1):
			Host: itrac1506.cern.ch
			...
```