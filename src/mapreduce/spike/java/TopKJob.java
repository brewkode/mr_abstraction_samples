package mapreduce.spike.java;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

class WordFrequencyWritable implements Writable {
    int count;
    String word;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeInt(word.length());
        dataOutput.write(word.getBytes());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readInt();
        int length = dataInput.readInt();
        byte[] b = new byte[length];
        dataInput.readFully(b);
        word = new String(b);
    }

    // Needed For the deserialization at reducer level
    WordFrequencyWritable(){}
    WordFrequencyWritable(int count, String word) {
        this.count = count;
        this.word = word;
    }
}

public class TopKJob extends Configured implements Tool {
    final static int K = 3;
    private Job createJob(String inPath, String outPath) throws IOException {
        String jobName = "Top K Vanilla MR";
        Job job = new Job(getConf());
        job.setJobName(jobName.trim());

        job.setMapSpeculativeExecution(false);
        job.setMapperClass(TopKMapper.class);
        job.setReducerClass(TopKReducer.class);
        // Because we want only the TopK, it should be reasonable to assume that,
        // sending data to one reducer would not create any problems
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

    public static void main(String[] args) throws Exception {
        new TopKJob().run(args);
    }

    public static class TopKMapper extends Mapper<Text, IntWritable, NullWritable, WordFrequencyWritable> {
        TreeMap<IntWritable, List<Text>> freqMap = new TreeMap<IntWritable, List<Text>>();
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            if (freqMap.containsKey(value)){
                freqMap.get(value).add(key);
            }else{
                List<Text> words = new ArrayList<Text>();
                words.add(key);
                freqMap.put(value, words);
            }

            // We don't have exactly do Top K on mappers.
            // So, we are approximating things here.
            if(freqMap.size() > K){
                freqMap.remove(freqMap.firstKey());
            }
            // keep alive
            context.progress();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (IntWritable key : freqMap.keySet()) {
                List<Text> words = freqMap.get(key);
                for (Text word : words) {
                    // because we want to have the word and its frequency; we construct the Writable
                    context.write(null, new WordFrequencyWritable(key.get(), word.toString()));
                }
            }
            super.cleanup(context);
        }
    }

    public static class TopKReducer extends Reducer<NullWritable, WordFrequencyWritable, Text, IntWritable> {
        TreeMap<Integer, List<String>> freqMap = new TreeMap<Integer, List<String>>();

        @Override
        protected void reduce(NullWritable key, Iterable<WordFrequencyWritable> values, Context context) throws IOException, InterruptedException {
            for (WordFrequencyWritable value : values) {
                if (freqMap.containsKey(value.count)){
                    List<String> words = freqMap.get(value.count);
                    words.add(value.word);
                }else{
                    List<String> words = new ArrayList<String>();
                    words.add(value.word);
                    freqMap.put(value.count, words);
                }

                if(freqMap.size() > K){
                    freqMap.remove(freqMap.firstKey());
                }
                // keep alive
                context.progress();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            NavigableSet<Integer> keys = freqMap.descendingKeySet();
            int count = 0;
            for (Integer key : keys) {
                List<String> words = freqMap.get(key);
                for (String word : words) {
                    count++;
                    if(count > K){
                        break;
                    }
                    context.write(new Text(word), new IntWritable(key));
                }

                if(count > K)
                    break;
            }
        }
    }



}
