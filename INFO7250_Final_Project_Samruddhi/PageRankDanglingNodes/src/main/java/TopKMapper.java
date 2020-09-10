import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

import java.util.*;

public class TopKMapper extends Mapper<Object, Text, NullWritable, Text> {
    private TreeMap<Double, Text> LocalKWinner = new TreeMap<Double, Text>();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] token = line.split("\t");

        String nodeID = token[0];
        Double pageRank = Double.valueOf(token[1]);

        LocalKWinner.put(pageRank, new Text(nodeID));

        if (LocalKWinner.size() > 10) {
            LocalKWinner.remove(LocalKWinner.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for(Map.Entry<Double, Text> entry: LocalKWinner.entrySet()) {
            String val = entry.getKey().toString() + "\t" + entry.getValue();
            Text t = new Text(val);
            context.write(NullWritable.get(), t);
        }
    }
}