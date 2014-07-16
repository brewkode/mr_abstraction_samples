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
import java.util.ArrayList;
import java.util.List;

public class WordDistributionJob extends Configured implements Tool {
    private Job createJob(String inPath, String outPath) throws IOException {
        String jobName = "WordDistribution Vanilla MR";
        Job job = new Job(getConf());
        job.setJobName(jobName.trim());

        job.setMapSpeculativeExecution(false);
        job.setMapperClass(WordDistributionMapper.class);
        job.setReducerClass(WordDistributionReducer.class);

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

    public static class WordDistributionMapper extends Mapper<IntWritable, Text, Text, IntWritable> {

        @Override
        protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            List<String> cleanedWords = new ArrayList<String>();

            for (String word : words) {
                if(word.trim().length() > 2)
                    cleanedWords.add(word.trim().toLowerCase());
            }

            for (String cleanedWord : cleanedWords) {
                context.write(new Text(cleanedWord), new IntWritable(1));
            }
        }
    }

    public static class WordDistributionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        new WordDistributionJob().run(args);
    }
}
