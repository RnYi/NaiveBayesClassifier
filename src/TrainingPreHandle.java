/**
* 输入将要预测的国别名，确定训练预测比例，将本地的文件上传到HDFS，形成训练集和预测集。
*/

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TrainingPreHandle {
    //本地样本的目录
    static Path localPrefix = Paths.get("/home/rany/NBCorpus/Country");
    //HDFS的URI
    static URI hdfsURI = URI.create("hdfs://localhost:9000");
    //HDFS的训练集所在目录
    static Path hdfsPrefix = Paths.get(hdfsURI.getPath(),"Training");
    //训练集所占比例
    static double trainingRatio = 0.5;

    public static void main(String[] args) {
        //清空HDFS上的Training文件夹
        //遍历args参数作为类别
        //获取一个类别文件夹中的文件列表
        //随机选取一个文件移动到HDFS，直到达到训练集所占比例
    }
}
