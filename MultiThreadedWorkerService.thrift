namespace java in.dreamlab.MultiThreadedPartitioning.thrift

enum MessageType {
	EDGE,
	SYNC,
	EXIT
}

service MultiThreadedWorkerService {

	void edgeReceiver(1: MessageType messageType, 2: list<string> edge, 4: i8 edgeColor);
	oneway void singleEdgeReceiver(1: MessageType messageType, 2: i64 v1 3: i64 v2, 4: i8 edgeColor);
	map<string, binary> specialVertices (1: MessageType messageType, 2: i32 avgDegree, 3: i64 v1, 4: i64 v2);
	void cleanRedVertices();
	void writeEdgeToFile();
	void closeConnection(1: MessageType messageType);
}