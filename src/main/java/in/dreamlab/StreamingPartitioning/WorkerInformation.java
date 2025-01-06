package in.dreamlab.StreamingPartitioning;

public class WorkerInformation {

    private String IP;
    private int port;

    public String getIP() {
        return IP;
    }

    public void setIP(String iP) {
        IP = iP;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public WorkerInformation(String iP, int port) {
        IP = iP;
        this.port = port;
    }
}
