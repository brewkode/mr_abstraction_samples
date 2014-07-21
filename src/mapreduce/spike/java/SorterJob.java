package mapreduce.spike.java;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;


public class SorterJob  extends Configured implements Tool {
    private Job createJob(String inPath, String outPath) throws IOException {
        String jobName = "Sorter Job MR";
        Job job = new Job(getConf());
        job.setJobName(jobName.trim());

        job.setMapSpeculativeExecution(false);
        job.setMapperClass(SorterJobMapper.class);
        job.setReducerClass(SorterJobReducer.class);
        job.setNumReduceTasks(1);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, inPath);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(outPath));

        job.setJarByClass(this.getClass());
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = createJob(args[0], args[1]);
        if(job.waitForCompletion(true))
            return 0;
        else
            return -1;
    }

    public static class SorterJobMapper extends Mapper<Text, IntWritable, IntWritable, Text> {
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(value, key);
        }
    }

    public static class SorterJobReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, key);
            }
        }
    }
}
