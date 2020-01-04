/*
* 输入将要预测的国别名，确定训练预测比例，将本地的文件上传到HDFS，形成训练集和预测集。
*/

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
public class TrainingPreHandle {
    //本地样本的目录
    static Path localPrefix = new Path("/home/rany/NBCorpus/Country");
    //HDFS上训练集的路径
    static Path trainingSetPrefixOnHDFS = new Path("/Training");
    //训练集所占比例
    static double trainingRatio = 0.5;

    public static void main(String[] args) throws IOException {
        //配置并获取HDFS实例
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        //清空HDFS上的Training文件夹
        if(fs.exists(trainingSetPrefixOnHDFS)){
            fs.delete(trainingSetPrefixOnHDFS,true);
        }
        fs.mkdirs(trainingSetPrefixOnHDFS);

        //遍历args参数作为类别
        //获取一个类别文件夹中的文件列表
        //随机选取一个文件移动到HDFS，直到达到训练集所占比例
    }
}
