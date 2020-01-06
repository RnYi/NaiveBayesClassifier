import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

public class TrainingClassCount {
    //本地样本的目录
    static Path localSamplePrefix = new Path("/home/rany/NBCorpus/Country");
    //HDFS的URI
    static String hdfsURI = "hdfs://localhost:9000";
    //本地预测集的路径
    static Path predictionSetPrefixOnLocal = new Path(localSamplePrefix, "Prediction");
    //HDFS上训练集的路径
    static Path trainingSetPrefixOnHDFS = new Path("/Training");
    //训练集所占比例
    static double trainingRatio = 0.5;

    public static void main(String[] args) throws IOException, URISyntaxException {
        TrainingPreHandle(args);
    }

    private static boolean TrainingPreHandle(String[] args) throws IOException, URISyntaxException {
        //配置并获取本地文件系统和HDFS实例
        try (FileSystem localfs = FileSystem.get(new URI("file:///"), new Configuration());
             FileSystem hdfs = FileSystem.get(new URI(hdfsURI), new Configuration())) {
            //清空Prediction文件夹
            if (localfs.exists(predictionSetPrefixOnLocal)) {
                localfs.delete(predictionSetPrefixOnLocal, true);
            }
            localfs.mkdirs(predictionSetPrefixOnLocal);

            //清空HDFS上的Training文件夹
            if (hdfs.exists(trainingSetPrefixOnHDFS)) {
                hdfs.delete(trainingSetPrefixOnHDFS, true);
            }
            hdfs.mkdirs(trainingSetPrefixOnHDFS);

            //本地样本目录
            Path localSampleDir;
            //本地预测集目录
            Path localPredictionDir;
            //HDFS训练集目录
            Path hdfsTrainingDir;

            //实例化一个随机类对象
            Random random = new Random();

            //遍历args参数作为类别
            for (String arg : args) {
                //通过参数确定并建立三个目录
                arg = arg.toUpperCase();
                localSampleDir = new Path(localSamplePrefix, arg);
                localPredictionDir = new Path(predictionSetPrefixOnLocal, arg);
                localfs.mkdirs(localPredictionDir);
                hdfsTrainingDir = new Path(trainingSetPrefixOnHDFS, arg);
                hdfs.mkdirs(hdfsTrainingDir);
                if (localfs.exists(localPredictionDir)) {
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
                        localfs.copyFromLocalFile(status.getPath(), localPredictionDir);
                    }
                } else {
                    System.out.println(arg + "doesn't exists");
                    return false;
                }
            }
            return true;
        }
    }
}

