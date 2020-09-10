import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    private static final int K = 10000;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        List<String> adjList = new ArrayList<String>();
        Double pageRank = 0D;

        Double sum = 0D;
        Double alpha = 0.85D;

        Vertex cvw = new Vertex();

        for(Text val: values){
            if(val.toString().split("\t").length == 2){
                String[] token = val.toString().split("\t");
                if(!token[0].isEmpty()) {

                    adjList = Arrays.asList(token[1].split(","));
                    pageRank = Double.valueOf(token[0]);
                    cvw = new Vertex(adjList, pageRank);
                }
            }
            else{
                sum += Double.valueOf(val.toString());
            }
        }
        try {
            cvw.setPageRank((Double)(((1D - alpha) / K ) + (alpha * sum)));
            context.write(key, new Text(cvw.toString()));
        }catch(Exception e){}
    }
}
