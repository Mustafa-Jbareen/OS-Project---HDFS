import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Same MR skeleton as the canonical WordCount example, but the mapper does
 * no tokenization: it emits exactly one ("trivial", 1) per input record.
 * This isolates HDFS read I/O + MapReduce framework cost from the per-word
 * CPU work the standard WordCount mapper does.
 *
 * Usage: hadoop jar trivial-wordcount.jar TrivialWordCount <input> <output>
 */
public class TrivialWordCount {

    public static class TrivialMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final Text KEY = new Text("trivial");
        private static final IntWritable ONE = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Touch the record so the InputFormat actually reads the bytes,
            // but skip every tokenization / per-word emit.
            if (value.getLength() >= 0) {
                context.write(KEY, ONE);
            }
        }
    }

    public static class SumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: TrivialWordCount <input> [<input2> ...] <output>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "TrivialWordCount");
        job.setJarByClass(TrivialWordCount.class);
        job.setMapperClass(TrivialMapper.class);
        // Combiner collapses per-map output to a single (trivial,N) before shuffle.
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
