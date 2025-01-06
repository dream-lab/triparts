package in.dreamlab.MultiThreadedPartitioning;

public class Edge {
    private long source;
    private long sink;

    public Edge() {}

    public Edge(long source, long sink) {
        assert (source > 0 || sink > 0) : "Invalid source or sink id - (source=" + source + ",sink=" + sink + ")";
        this.source = source;
        this.sink = sink;
    }

    public static Edge toEdge(String edge) {
        String[] items = edge.split(",");
        long source = Long.parseLong(items[0]);
        long sink = Long.parseLong(items[1]);

        return new Edge(source, sink);
    }

    public String toString() {
        String v1 = Long.toString(this.source);
        String v2 = Long.toString(this.sink);
        return (v1 + "," + v2);
    }

    public long getSource() {
        return this.source;
    }

    public long getSink() {
        return this.sink;
    }

    public void setSource(long source) {
        this.source = source;
    }

    public void setSink(long sink) {
        this.sink = sink;
    }

    public static boolean compare(Edge e1, Edge e2) {
        return (e1.getSource() == e2.getSource() && e1.getSink() == e2.getSink());
    }

}
