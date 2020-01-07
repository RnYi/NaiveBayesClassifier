import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.shaded.com.jcraft.jsch.IO;

import java.io.IOException;
import java.lang.System;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
public class Test {
    public static void main(String[] args) throws URISyntaxException, IOException {
        StringBuilder str = new StringBuilder();
        str.append("123");
        str.append("234");
        System.out.println(str);
    }
}
