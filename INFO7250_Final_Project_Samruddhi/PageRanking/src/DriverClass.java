import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DriverClass {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        int no_of_iter = 10;

        // MapReduce job to Create Adjacency List
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AdjList");
        job.setJarByClass(DriverClass.class);

        Path outDir = new Path(args[2]);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, outDir);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(CreateAdjListMapper.class);
        job.setReducerClass(CreateAdjListReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //10 Fixed  iterations of MapReduce job to calculate Page Rank Value after the creation of Adjacency List
        if(job.waitForCompletion(true)) {

            int i;
            int status = 1;
            for ( i = 0; i < no_of_iter; i++) {

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
                    FileInputFormat.addInputPath(job1, new Path(args[2], "out" + Integer.toString(i - 1)));
                }
                FileOutputFormat.setOutputPath(job1, new Path(args[2], "out" + Integer.toString(i)));

                job1.waitForCompletion(true);
            }

            //MapReduce job to calculate TopK Web Pages
            if (i == no_of_iter) {
                Job job3 = new Job(conf, "Top Ten Users by Reputation");
                job3.setJarByClass(DriverClass.class);
                final Configuration conf3 = job3.getConfiguration();

                job3.setMapperClass(TopKMapper.class);
                job3.setReducerClass(TopKReducer.class);
                job3.setNumReduceTasks(1);
                job3.setOutputKeyClass(NullWritable.class);
                job3.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job3, new Path(args[2] ,"out" + Integer.toString(no_of_iter - 1)));
                FileOutputFormat.setOutputPath(job3, new Path(args[2], "topK"));
                job3.waitForCompletion(true);
            }

            System.exit(status);
        }

    }

}
