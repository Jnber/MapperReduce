package tn.insat.tp1.homeapplication;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TotalSumReducer  extends Reducer<Text, FloatWritable,Text,FloatWritable> {
    private final FloatWritable result = new FloatWritable();
    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        float sum = 0;
        for (FloatWritable val : values) {
            System.out.println("key: "+ key.toString() +"value: "+val.get());
            sum += val.get();
        }
        System.out.println("--> Sum = "+sum);
        result.set(sum);
        context.write(key, result);
    }
}