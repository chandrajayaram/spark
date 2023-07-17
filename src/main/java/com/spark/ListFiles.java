package com.spark;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class ListFiles {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: ListFiles <path>");
            System.exit(1);
        }
        //var fileName = fsys.listStatus(new Path("/data/ihs_markit/cdspd")).filter(_.isFile).map(_.getPath.getName).toList
        		
        SparkConf sparkConf = new SparkConf().setAppName("ListFiles")
            .setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        FileSystem hadoopFileSys = FileSystem.get(ctx.hadoopConfiguration());
        FileStatus[] status = hadoopFileSys.listStatus(new  Path("/data/ihs_markit/cdspd"));
        List<Path> paths = Stream.of(status).filter(e -> e.isFile()).map(e -> e.getPath()).collect(Collectors.toList());
         //.filter(e ->e.isFile()).map(e -> e.getPath().getName());
        paths.forEach(e ->{
        	System.out.println(e + ":" + e.getName());
        });
        ctx.stop();
    }
}