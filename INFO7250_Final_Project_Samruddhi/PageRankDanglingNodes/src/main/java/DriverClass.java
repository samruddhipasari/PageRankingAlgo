import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Counter;

import java.io.IOException;
//import java.nio.file.Path;

public class DriverClass extends Configured implements Tool {



//    private static final Logger logger = LogManager.getLogger(DriverClass.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // write your code here
        // Create a new Job

        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new DriverClass(), args);
        } catch (final Exception e) {
            //logger.error("", e);
            System.out.println(e);
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        double delta = 0D;
        long delta_long = 0L;
        int no_of_iter = 10;

        // MapReduce job to Create Adjacency List
        final Configuration conf = getConf();;
        Job job = Job.getInstance(conf, "AdjList");
        job.setJarByClass(DriverClass.class);

        Path outDir = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outDir);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(CreateAdjListMapper.class);
        job.setReducerClass(CreateAdjListReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean status = false;

        //10 Fixed  iterations of MapReduce job to calculate Page Rank Value after the creation of Adjacency List
        int i;
        if(job.waitForCompletion(true)) {
            for (i = 0; i < no_of_iter; i++) {
                Job job1 = Job.getInstance(conf, "PageRanking");
                job1.setJarByClass(DriverClass.class);
                final Configuration conf1 = job1.getConfiguration();

                job1.setInputFormatClass(TextInputFormat.class);
                job1.setOutputFormatClass(TextOutputFormat.class);

                job1.setMapOutputKeyClass(Text.class);
                job1.setMapOutputValueClass(Text.class);

                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(Text.class);

                job1.setMapperClass(PageRankMapper.class);
                job1.setReducerClass(PageRankReducer.class);

                if (i == 0) {
                    FileInputFormat.addInputPath(job1, outDir);
                } else {
                    FileInputFormat.addInputPath(job1, new Path(args[1], "out" + Integer.toString(i - 1)));
                }
                FileOutputFormat.setOutputPath(job1, new Path(args[1], "dangling_out" + Integer.toString(i)));

                job1.waitForCompletion(true);

                Counters delta_counter = job1.getCounters();
                Counter c = delta_counter.findCounter(DeltaCounter.DELTA);
                delta_long = c.getValue();

                //Mapper to Calculate Delta for Dangling node handling
                Job job2 = Job.getInstance(conf, "DeltaCalculation");
                job2.setJarByClass(DriverClass.class);
                final Configuration conf2 = job2.getConfiguration();

                conf2.setLong("Delta PR", delta_long);
                job2.setMapperClass(CalculateDeltaMapper.class);

                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);

                job2.setInputFormatClass(TextInputFormat.class);
                job2.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.addInputPath(job2, new Path(args[1], "dangling_out" + Integer.toString(i)));
                FileOutputFormat.setOutputPath(job2, new Path(args[1], "out" + Integer.toString(i)));

                status = job2.waitForCompletion(true);
            }

            //MapReduce job for TopK Algorithm
            if (i == no_of_iter) {
                Job job3 = new Job(conf, "Top Ten Users by Reputation");
                job3.setJarByClass(DriverClass.class);
                final Configuration conf3 = job3.getConfiguration();

                job3.setMapperClass(TopKMapper.class);
                job3.setReducerClass(TopKReducer.class);
                job3.setNumReduceTasks(1);
                job3.setOutputKeyClass(NullWritable.class);
                job3.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job3, new Path(args[1] ,"out" + Integer.toString(no_of_iter - 1)));
                FileOutputFormat.setOutputPath(job3, new Path(args[1], "topK"));
                job3.waitForCompletion(true);
            }
            return 0;
        } else {
            return 1;
        }
    }
}
