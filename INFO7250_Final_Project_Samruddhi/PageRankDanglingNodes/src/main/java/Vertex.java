import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Vertex {

    private List<String> edgeList;
    private Double pageRank;

    public Vertex() {
    }

    public Vertex(List<String> edgeList, Double pageRank) {
        this.edgeList = edgeList;
        this.pageRank = pageRank;
    }

    public List<String> getEdgeList() {
        return edgeList;
    }

    public void setEdgeList(List<String> edgeList) {
        this.edgeList = edgeList;
    }

    public Double getPageRank() {
        return pageRank;
    }

    public void setPageRank(Double pageRank) {
        this.pageRank = pageRank;
    }

    @Override
    public String toString() {
        //String list = edgeList.toString().replace(" ", "").replace("[", "").replace("]", "");
        String list = edgeList.toString().replace(" ", "");
        return pageRank + "\t" + list;

    }
}

