import org.apache.hadoop.shaded.com.jcraft.jsch.IO;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.lang.System;
import java.io.File;
public class Test {
    public static void main(String[] args) {
        Path prefix = Paths.get("/home/rany/NBCorpus/Country");
        for(String arg:args){
            arg = arg.toUpperCase();
            File file = prefix.resolve(arg).toFile();
            if(file.exists()){
                System.out.println(file);
            }
        }
    }
}
