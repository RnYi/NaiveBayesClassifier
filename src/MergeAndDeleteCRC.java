
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class MergeAndDeleteCRC {
    public static void main(String[] args) throws IOException {
        Runtime rt = Runtime.getRuntime();
        rt.exec("hdfs dfs -getmerge /Result/ClassCount/* ~/Result/ClassCount.txt");
        rt.exec("hdfs dfs -getmerge /Result/WordCount/* ~/Result/WordCount.txt");
        Path localPredictionDir = Paths.get("/home/rany/NBCorpus/Country/Prediction");
        File dir = localPredictionDir.toFile();
        for(File className: dir.listFiles()){
            for(File sample: className.listFiles()){
                if(sample.getPath().endsWith(".crc")){
                    sample.delete();
                }
            }
        }

    }
}

