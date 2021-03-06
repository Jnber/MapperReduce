package tn.insat.tp1.homeapplication;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class TotMagazin {
    public static void main(String[] args) throws Exception {
        File file = new File(args[1]);
        FileUtils.deleteDirectory(file);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sales per shop");
        job.setJarByClass(TotMagazin.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(TotalSumReducer.class);
        job.setReducerClass(TotalSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}