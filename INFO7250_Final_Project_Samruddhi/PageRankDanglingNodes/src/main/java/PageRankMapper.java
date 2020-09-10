import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text val = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] token = line.split("\t");

        String temp = token[2].replace(" ", "").replace("[", "").replace("]", "");
        List<String> adjList;

        if(temp.equals("")){
            adjList = new ArrayList<String>();
        }
        else {
            adjList = new ArrayList<String>(Arrays.asList(temp.split(",")));
        }
        String nodeID = token[0];
        Double pageRank = Double.valueOf(token[1]);
        k.set(nodeID);
        Vertex cvw = new Vertex(adjList, pageRank);
        val.set(cvw.toString());

        if (adjList.size() > 0) {
            context.write(k, val);
            Double newRank = (Double)(pageRank / adjList.size());

            for (String edge : adjList) {
                context.write(new Text(edge), new Text(String.valueOf(newRank)));
            }
        }
        // To handle Dangling Nodes
        else{
            context.write(new Text("dummy"), new Text(String.valueOf(pageRank)));
        }
        context.write(k, val);
    }
}
