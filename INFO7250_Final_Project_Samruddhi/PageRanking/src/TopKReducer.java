import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class TopKReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    private TreeMap<Double, Text> GlobalKWinner = new TreeMap<Double, Text>();

    @Override
    public void reduce(NullWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] token = value.toString().split("\t");

            GlobalKWinner.put(Double.parseDouble(token[0]), new Text(token[1]));

            if (GlobalKWinner.size() > 10) {
                GlobalKWinner.remove(GlobalKWinner.firstKey());
            }
        }

        NavigableMap<Double, Text> nmap = GlobalKWinner.descendingMap();

        for(Map.Entry<Double, Text> entry: nmap.entrySet()) {
            String op = entry.getKey().toString() + "\t" + entry.getValue();
            Text t = new Text(op);
            context.write(NullWritable.get(), t);
        }
    }
}