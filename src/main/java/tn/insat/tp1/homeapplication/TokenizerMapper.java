package tn.insat.tp1.homeapplication;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {

    private final static FloatWritable sum = new FloatWritable();
    private final Text magazin = new Text();

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t", -1);
        magazin.set(split[2]);
        sum.set(Float.parseFloat(split[4]));
        context.write(magazin, sum);
    }
}
