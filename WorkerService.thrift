namespace java in.dreamlab.StreamingPartitioning.thrift

enum MessageType {
	EDGE,
	SYNC,
	EXIT
}

service WorkerService {
	
	oneway void edgeReceiver(1: MessageType messageType, 2: i64 v1, 3: i64 v2, 4: i8 edgeColor);
	map<string,binary> specialVertices (1: MessageType messageType, 2: i32 avgDegree);

	void cleanRedVertices();
	void writeEdgeToFile();
	
	void closeConnection(1: MessageType messageType);
}
