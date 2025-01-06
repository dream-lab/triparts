package in.dreamlab.MultiThreadedPartitioning.serviceHandler;

import in.dreamlab.MultiThreadedPartitioning.MultiThreadedWorker;
import in.dreamlab.MultiThreadedPartitioning.thrift.MessageType;
import in.dreamlab.MultiThreadedPartitioning.thrift.MultiThreadedWorkerService;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class MultiThreadedWorkerServiceHandler implements MultiThreadedWorkerService.Iface{

    private MultiThreadedWorker worker;

    public MultiThreadedWorkerServiceHandler(MultiThreadedWorker worker) {
        this.worker = worker;
    }

    @Override
    public void edgeReceiver(MessageType messageType, List<String> edge, byte edgeColor) throws TException {
        worker.edgeListHandler(messageType, edge, edgeColor);
    }

    @Override
    public void singleEdgeReceiver(MessageType messageType, long v1, long v2, byte edgeColor) throws TException {
//        System.out.println("Entering single edge receiver");
        worker.edgeHandler(v1, v2, edgeColor);
    }

    @Override
    public Map<String, ByteBuffer> specialVertices(MessageType messageType, int avgDegree, long v1, long v2) throws TException {
        return worker.syncHandler(messageType, avgDegree, v1, v2);
    }

    @Override
    public void cleanRedVertices() throws TException {

    }

    @Override
    public void writeEdgeToFile() throws TException {
        try {
            worker.writeEdges();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void closeConnection(MessageType messageType) throws TException {
        worker.shutDownServer(messageType);
    }
}
