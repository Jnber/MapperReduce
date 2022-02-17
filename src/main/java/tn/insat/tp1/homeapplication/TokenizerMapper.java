package tn.insat.tp1.homeapplication;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {

    private final static FloatWritable number = new FloatWritable();
    private Text word = new Text();

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String myString = value.toString();
        String[] split = myString.split("\t", -1);
        String shop = split[2];
        word.set(shop);
        number.set(Float.parseFloat(split[4]));
        context.write(word, number);
    }
}
