import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreateAdjListMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int K = 10000;
    Text k = new Text();
    Text val = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] token = line.split("\t");

        String node = null;
        String edge = null;

        try {
            if (Integer.parseInt(token[0]) <= K || Integer.parseInt(token[1]) <= K) {
                node = token[0];
                edge = token[1];
                k.set(node);
                val.set(edge);

                //Emitting nodeID as key and outgoing links as value
                context.write(k, val);

                //Emitting empty value for Dangling nodes
                context.write(val, new Text(""));
            }

        }
        catch(Exception e){

        }

    }
}
