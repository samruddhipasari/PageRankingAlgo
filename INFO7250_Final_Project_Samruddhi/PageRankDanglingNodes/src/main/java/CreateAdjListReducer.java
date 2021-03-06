import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreateAdjListReducer extends Reducer<Text, Text, Text, Text> {

    private static final Integer K = 10000;
    Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        List<String> adjList = new ArrayList<String>();

        for(Text val: values){
            if(val.toString().equals(""))
                continue;
            adjList.add(String.valueOf(val));

        }

        Double pageRank =  (Double) (1D/ K);

        Vertex cvw = new Vertex(adjList, pageRank);
        result.set(cvw.toString());
        context.write(key, result);
    }
}