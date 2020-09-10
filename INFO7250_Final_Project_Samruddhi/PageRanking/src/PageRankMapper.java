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


        List<String> adjList = new ArrayList<String>(Arrays.asList(token[2].split(",")));

        if (adjList.size() > 0) {
            String nodeID = token[0];
            Double pageRank = Double.valueOf(token[1]);
            k.set(nodeID);
            Vertex cvw = new Vertex(adjList, pageRank);
            val.set(cvw.toString());

            context.write(k, val);

            Double newRank = (Double)(pageRank / adjList.size());

            for (String edge : adjList) {
                context.write(new Text(edge), new Text(String.valueOf(newRank)));
            }
        }
    }
}
