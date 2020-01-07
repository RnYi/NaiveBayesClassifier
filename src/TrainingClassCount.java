import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrainingClassCount {
    //本地样本的目录前缀
    static Path localSamplePrefix = new Path("/home/rany/NBCorpus/Country");
    //HDFS的URI
    static String hdfsURI = "hdfs://localhost:9000";
    //HDFS预测集的路径前缀
    static Path predictionSetPrefixOnHDFS = new Path("/Prediction");
    //HDFS上训练集的路径前缀
    static Path trainingSetPrefixOnHDFS = new Path("/Training");
    //HDFS上结果路径前缀
    static Path resultPrefixOnHDFS = new Path("/Result");
    //训练集所占比例
    static double trainingRatio = 0.5;

    static class ClassCountMapper extends Mapper<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    static class ClassCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val: values){
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        if(TrainingPreHandle(args)){
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hdfsURI);
            Job job = Job.getInstance(conf, "ClassCount");
            job.setJarByClass(TrainingClassCount.class);
            job.setInputFormatClass(FilePathInputFormat.class);
            job.setMapperClass(ClassCountMapper.class);
            job.setReducerClass(ClassCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileSystem hdfs = FileSystem.get(new URI(hdfsURI), new Configuration());
            FileStatus[] fileStatuses = hdfs.listStatus(trainingSetPrefixOnHDFS);
            for(FileStatus status: fileStatuses){
                FilePathInputFormat.addInputPath(job, status.getPath());
            }
            FileOutputFormat.setOutputPath(job, new Path(resultPrefixOnHDFS, "ClassCount"));
            System.exit(job.waitForCompletion(true)?0:1);
        }else{
            System.out.println("样本分割失败");
            System.exit(1);
        }
    }

    public static void cleanUpAndMkdir(FileSystem fs, Path path) throws IOException {
        if(fs.exists(path)){
            fs.delete(path, true);
        }
        fs.mkdirs(path);
    }

    private static boolean TrainingPreHandle(String[] args) throws IOException, URISyntaxException {
        //配置并获取本地文件系统和HDFS实例
        try (FileSystem localfs = FileSystem.get(new URI("file:///"), new Configuration());
             FileSystem hdfs = FileSystem.get(new URI(hdfsURI), new Configuration())) {

            //清空Prediction和Training文件夹
            cleanUpAndMkdir(localfs, predictionSetPrefixOnHDFS);
            cleanUpAndMkdir(hdfs, trainingSetPrefixOnHDFS);

            //本地样本目录
            Path localSampleDir;
            //HDFS预测集目录
            Path hdfsPredictionDir;
            //HDFS训练集目录
            Path hdfsTrainingDir;

            //实例化一个随机类对象
            Random random = new Random();

            //遍历args参数作为类别
            for (String arg : args) {
                //通过参数确定并建立三个目录
                arg = arg.toUpperCase();
                localSampleDir = new Path(localSamplePrefix, arg);
                hdfsPredictionDir = new Path(predictionSetPrefixOnHDFS, arg);
                localfs.mkdirs(hdfsPredictionDir);
                hdfsTrainingDir = new Path(trainingSetPrefixOnHDFS, arg);
                hdfs.mkdirs(hdfsTrainingDir);
                if (localfs.exists(localSampleDir)) {
                    //获取样本的所有文件信息
                    ArrayList<FileStatus> fileStatuses = new ArrayList<>(Arrays.asList(
                            localfs.listStatus(
                                    localSampleDir)));
                    //确定训练集数量
                    int countOfTrainingSample = (int) (fileStatuses.size() * trainingRatio);
                    System.out.println("开始上传训练集" + arg + ": " + countOfTrainingSample);
                    while ((countOfTrainingSample--) != 0) {
                        //随机获取一个文件上传到HDFS
                        FileStatus chosenFileStatus = fileStatuses.remove(random.nextInt(fileStatuses.size()));
                        hdfs.copyFromLocalFile(chosenFileStatus.getPath(), hdfsTrainingDir);
                    }
                    //将剩下的文件复制到预测文件夹
                    System.out.println("开始上传预测集" + arg + ": " + fileStatuses.size());
                    for (FileStatus status : fileStatuses) {
                        hdfs.copyFromLocalFile(status.getPath(), hdfsPredictionDir);
                    }
                } else {
                    System.out.println(arg + "不存在");
                    return false;
                }
            }
            return true;
        }
    }
}

class FilePathInputFormat extends FileInputFormat<Text, IntWritable> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, IntWritable> createRecordReader(InputSplit inputSplit,
                                                               TaskAttemptContext taskAttemptContext) {
        FilePathRecordReader reader = new FilePathRecordReader();
        reader.initialize(inputSplit, taskAttemptContext);
        return reader;
    }
}

class FilePathRecordReader extends RecordReader<Text, IntWritable> {

    private FileSplit fileSplit;
    private Text key = new Text();
    private final static IntWritable one = new IntWritable(1);
    private boolean processed = false;

    @Override
    public void initialize(InputSplit inputSplit,
                           TaskAttemptContext taskAttemptContext) {
        this.fileSplit = (FileSplit) inputSplit;
    }

    @Override
    public boolean nextKeyValue() {
        if(!processed){
            Path filePath = this.fileSplit.getPath();
            key = new Text(filePath.getParent().getName());
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() {
        return this.key;
    }

    @Override
    public IntWritable getCurrentValue() {
        return one;
    }

    @Override
    public float getProgress() {
        return processed?1.0f:0.0f;
    }

    @Override
    public void close() {
        //Do nothing
    }
}
