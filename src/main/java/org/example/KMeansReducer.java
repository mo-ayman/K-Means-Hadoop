package org.example;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<LongWritable, PointWritable, LongWritable, PointWritable> {
    @Override
    public void reduce(LongWritable key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {
        PointWritable point = new PointWritable(values.iterator().next().toString().split(","));
        int size = 1;
        while(values.iterator().hasNext()) {

            point.add(values.iterator().next());
            size++;
        }
        for (int i=0; i< point.getNumFeatures(); i++) {
            point.setFeature(i, point.getFeature(i) / (double)size);
        }

        context.write(key, point);
    }
}
