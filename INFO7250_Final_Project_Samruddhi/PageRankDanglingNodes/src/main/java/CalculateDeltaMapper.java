import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CalculateDeltaMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final int K = 10000;
    Text k = new Text();
    Text val = new Text();
    private static double delta;
    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        String confVar = context.getConfiguration().get("Delta PR");
        Long delta_long = Long.parseLong(confVar);
        delta =  ((double)delta_long / 1000000000);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] token = line.split("\t");

        List<String> adjList = new ArrayList<String>(Arrays.asList(token[2].replace(" ", "").replace("[", "").replace("]", "").split(",")));

        String nodeID = token[0];
        Double pageRank = Double.valueOf(token[1]);
        Double sum = 0D;
        Double alpha = 0.85D;
        Double delta = 0D;
        Vertex vertex = new Vertex();
        vertex.setEdgeList(adjList);

        Double newRank = pageRank + (alpha * (delta/ K) )+ ((1-alpha) / K);
        vertex.setPageRank(newRank);

        k.set(nodeID);
        val.set(vertex.toString());
        context.write(k, val);
    }
}
