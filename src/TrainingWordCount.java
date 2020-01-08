import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TrainingWordCount {
    //HDFS的URI
    static String hdfsURI = "hdfs://localhost:9000";
    //HDFS上训练集的路径前缀
    static Path trainingSetPrefixOnHDFS = new Path("/Training");
    //HDFS上结果路径前缀
    static Path resultPrefixOnHDFS = new Path("/Result");

    static class WordCountMapper extends Mapper<LongWritable, Text, TextPair, IntWritable>{
        IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Path path = ((FileSplit)context.getInputSplit()).getPath();
            Text className = new Text(path.getParent().getName());
            TextPair textPair = new TextPair(className, value);
            context.write(textPair, one);
        }
    }

    static class WordCountReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable>{
        @Override
        protected void reduce(TextPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsURI);
        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(TrainingWordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem hdfs = FileSystem.get(new URI(hdfsURI), new Configuration());
        FileStatus[] fileStatuses = hdfs.listStatus(trainingSetPrefixOnHDFS);
        for(FileStatus status: fileStatuses){
            FileInputFormat.addInputPath(job, status.getPath());
        }
        Path wordCountResultPath = new Path(resultPrefixOnHDFS, "WordCount");
        if(hdfs.exists(wordCountResultPath)){
            hdfs.delete(wordCountResultPath,true);
        }
        FileOutputFormat.setOutputPath(job, wordCountResultPath);
        System.exit(job.waitForCompletion(true)?0:1);
    }

}

class TextPair implements WritableComparable<TextPair>{

    private Text first;
    private Text second;
    public TextPair(){
        first = new Text();
        second = new Text();
    }
    public TextPair(Text first, Text second){
        this.first = new Text(first);
        this.second = new Text(second);
    }

    public Text getFirst(){return this.first;}
    public Text getSecond(){return this.second;}

    @Override
    public int compareTo(@NotNull TextPair textPair) {
        int cmp = first.compareTo(textPair.getFirst());
        if(cmp!=0){
            return cmp;
        }
        return second.compareTo(textPair.getSecond());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof TextPair){
            TextPair textPair = (TextPair)o;
            return first.equals(textPair.getFirst())&&second.equals(textPair.getSecond());
        }
        return false;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }
}
