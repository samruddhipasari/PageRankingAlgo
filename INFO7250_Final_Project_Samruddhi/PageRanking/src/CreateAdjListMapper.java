import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

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
            if(!line.startsWith("#") && token.length == 2) {
                if (Integer.parseInt(token[0]) <= K || Integer.parseInt(token[1]) <= K) {
                    node = token[0];    //nodeID as key
                    edge = token[1];    //nodeID of the outgoing link
                    k.set(node);
                    val.set(edge);

                    context.write(k, val);
                }
            }
        }
        catch(Exception e){

        }

    }
}
