# TriParts : Scalable Streaming Graph Partitioning to Enhance Community Structure

#### Ruchi Bhoot<sup>1</sup>, Tuhin Khare<sup>2,^</sup>, Manoj Agarwal<sup>3</sup>, Siddharth Jaiswal<sup>4,^</sup> and [Yogesh Simmhan](https://cds.iisc.ac.in/faculty/simmhan/)<sup>1</sup>

<sup>1</sup>Indian Institute of Science, Bangalore, India

<sup>2</sup>Georgia Institute of Technology, Atlanta, GA, USA

<sup>3</sup>GiKA.AI, Bangalore, India

<sup>4</sup>Indian Institute of Technology (IIT), Kharagpur, India

ruchibhoot@iisc.ac.in, tkhare7@gatech.edu, agarwalm@gikagraph.ai, siddsjaiswal@kgpian.iitkgp.ac.in, simmhan@iisc.ac.in

<sup>^</sup>*Based on work done while at the Indian Institute of Science, Bangalore, India*

𝑘-way edge based partitioning algorithms for processing large streaming graphs, such as social networks and web crawls, assign each arriving edge to one of the 𝑘 partitions. This can result in vertices being replicated on multiple partitions. Typically, such partitioning algorithms aim to balance the edge counts across partitions while minimizing the vertex replication. However, such objectives ignore the community structure inherently embedded in the graph, which is an important partitioning quality metric for clustering and graph mining applications that subsequently operate on the partitions. 
To address this gap, we propose a novel optimization function, to maximize the number of local triangles in the partitions as an additional objective function. Triangle count is an effective metric to measure the conservation of community structure. Further, we propose a family of cascading heuristics to perform online partitioning over an edge stream of a graph, which use three complementary state data structures: Bloom Filters, Triangle Map and High degree Map. Each state adds tangible value to meeting our objectives. These are implemented as part of our **TriParts** distributed edge-based streaming partitioner.
We validate our partitioning algorithms on six diverse real world graphs, comprising up to 1.6B edges, with varying triangle densities, using both random and BFS ordered edge streams. Our best heuristic BTH outperforms the state-of-the-art DBH and HDRF streaming graph partitioners on the triangle-count metric by up to 4−8.3x while maintaining competitive vertex replication factor and edge-balancing. We are also able to achieve an ingest rate of 500k edges/sec.

## Transparency and Reproducibility

This supplemental material provides instructions for installing and running our partitioning algorithms.
**The goal is ensure that the artifacts can be evaluated to be [Functional](https://www.acm.org/publications/policies/artifact-review-and-badging-current)**, i.e., *the artifacts associated with the research are found to be documented, consistent, complete, exercisable*.

---

## Instructions
This work is implement on a distributed cluster using **Java v8** and
**Apache Thrift 0.13.0**. on each node.

## 1. Installing Pre-requisites and Dependencies
Pre-requisites:
* A Linux Ubuntu-based system (VM or bare-metal) with >= 8GB RAM
* Java JDK 8
* Maven >= 3.6.0

Install Apache thrift on all nodes of the cluster.
Instruction for this are in https://thrift.apache.org/docs/install/debian.html

---

## 2. Build the project 
The source code is present under ['src/'](https://github.com/dream-lab/GraphPartitioning/tree/VLDB-2024/src/main/java/in/dreamlab) folder.
To build the project, executing the following command.
```
$ mvn -DskipTests clean package assembly:single
```
---
## 3. Running the TriParts Partitioner
To run we need the graph edge list to be present in csv format in say file `edgelist.txt`:
```
v1,v2
v2,v3
...
```



To create `k` partitions, we need k workers. workers can be different port on same machine or k different machines.
Create a worker conf file, say `wconf.txt`, and mention `IP,Port,WorkerID` for each of the k partitions:
```
IP_1,PORT_1,0
IP_2,PORT_2,1
...
IP_k,PORT_k,k-1
```
A sample conf file is provided in [config-4-partitions.conf](https://github.com/dream-lab/GraphPartitioning/tree/VLDB-2024/src/main/resources).

To run we need additional arguments:
1. dataset : path of the edge list csv file, eg; edgelist.txt
2. Npart : Number of partitions to be created
3. Heuristic : Partitioner heuristic to use : {B, BH, BTH, BT} (These are explained in detail in the paper)
4. Mthr : number of master threads to use. 
5. Wthr : number of worker threads to use.
6. Vertices : total number of vertices in the graph
7. Edges : total number of edges in the graph
8. WorkerConf : worker configuration file path.
9. Worker ID : worker id number for each worker starting from 0 till (k-1).


To start execution copy the jar built above on all the nodes in the cluster.
Then, start all the workers first using the following command. Once all the workers are running, start the leader.


#### To Start the Worker
Run the following command on each worker node:
```
$ ssh <IP_i> java -cp target/StreamingPartitioning-0.0.1-SNAPSHOT-jar-with-dependencies.jar in.dreamlab.MultiThreadedPartitioning.MultiThreadedWorkerServer <PORT_i> <Worker ID> <Heuristic> <Wthr>
```

###### Example

To run BTH on 4 workers using WorkerConf file: [config-4-partitions.conf](https://github.com/dream-lab/GraphPartitioning/tree/VLDB-2024/src/main/resources) which contains:
```
192.168.0.12,6661,0
192.168.0.13,6662,1
192.168.0.14,6663,2
192.168.0.15,6664,3
```
Run the following commands to start the workers:
```
ssh 192.168.0.12 java -cp target/StreamingPartitioning-0.0.1-SNAPSHOT-jar-with-dependencies.jar in.dreamlab.MultiThreadedPartitioning.MultiThreadedWorkerServer 6661 0 BTH 16
ssh 192.168.0.13 java -cp target/StreamingPartitioning-0.0.1-SNAPSHOT-jar-with-dependencies.jar in.dreamlab.MultiThreadedPartitioning.MultiThreadedWorkerServer 6662 1 BTH 16
ssh 192.168.0.14 java -cp target/StreamingPartitioning-0.0.1-SNAPSHOT-jar-with-dependencies.jar in.dreamlab.MultiThreadedPartitioning.MultiThreadedWorkerServer 6663 2 BTH 16
ssh 192.168.0.15 java -cp target/StreamingPartitioning-0.0.1-SNAPSHOT-jar-with-dependencies.jar in.dreamlab.MultiThreadedPartitioning.MultiThreadedWorkerServer 6664 3 BTH 16
```

#### To Start the Leader and Run a Partitioning

Run the following command on the Leader and partition the given graph.
``` 
$ ssh <leader_node> java -cp target/StreamingPartitioning-0.0.1-SNAPSHOT-jar-with-dependencies.jar in.dreamlab.MultiThreadedPartitioning.MultiThreadedMaster <dataset> <Npart> <Heuristic> <Mthr> <Vertices> <Edges> <WorkerConf>
```

#### Output:
After execution completes, a file named
`WorkerData_<Heuristic>_<Worker ID>.txt` will
be created on each worker containing the partitioned edgelist of
partition `<Worker ID>`.

---

## 4. Running the baseline partitioning algorithms: DBH and HDRF

We refer the code provided by the authors of Vertex-cut Graph Partitioning (VGP) available [here](https://github.com/fabiopetroni/VGP).
The jar file for VGP we used is shared in folder [baseline](https://github.com/dream-lab/GraphPartitioning/tree/VLDB-2024/baseline/).

To run baseline DBH or HDRF we need to create a config file for each dataset containing batch-size, load balance threshold and number of edges in the graph. A sample config file is shown below:
```
batch-size:10000000
threshold:40
edges:133727516
```
To run baseline we also need the following arguments:
1. leader_node: IP of the leader node of the cluster.
2. jar_path: path to the jar file; created and shared [here](https://github.com/dream-lab/GraphPartitioning/tree/VLDB-2024/baseline/).
3. config_path: path to the config file created above.
4. dataset_path: path to the edgelist file that needs to be partitioned.
5. num_parts: number og partitions to be created.
6. method: enum for partitioning algorithm to use : "hdrf" or "dbh".
7. output_folder_path: path where we want output files to be saved.

Run the following command
```
$ ssh <leader_node> java -jar <jar_path> <config_path> <dataset_path> <num_parts> -algorithm <method> -lambda 3 -threads 16 -output <output_folder_path> 
```

#### Output
After execution completes, a file named `results.edges` is created in the given folder <output_folder_path>, which contains a mapping from each edges to its respective partition.

---
## 5. Graphs Evaluated in the Paper

The paper evaluates six different graphs, which were downloaded from the following sources.
1. USRN: http://networkrepository.com/road-road-usa.php
2. ORKUT: https://snap.stanford.edu/data/com-Orkut.html
3. DBPdeia: http://konect.cc/networks/dbpedia-link/
4. Brain: http://networkrepository.com/bn-human-BNU-1-0025864-session-2-bg.php
5. MAG: https://doi.org/10.1145/2872518.2890525
6. Twitter: https://doi.org/10.1609/icwsm.v4i1.14033
---

## Attribution and Citation
You may cite ths work as follows:

* **TriParts: Scalable Streaming Graph Partitioning to Enhance Community Structure**, Ruchi Bhoot, Tuhin Khare, Manoj Agarwal, Siddharth Jaiswal, and Yogesh Simmhan, *Technical Report, Indian Institute of Science*, 2025, Available at: https://github.com/dream-lab/triparts.

## License and Copyright
```
Copyright 2025 INDIAN INSTITUTE OF SCIENCE

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at


       http://www.apache.org/licenses/LICENSE-2.0


   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
```
