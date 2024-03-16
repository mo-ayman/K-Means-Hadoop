package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class KMeansMapper extends Mapper<LongWritable, Text, LongWritable, PointWritable> {

    private final LongWritable closestCenter = new LongWritable(0);
    private final PointWritable currentPoint = new PointWritable();
    private final PointWritable centroid = new PointWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        // put 0-3 into the point
        currentPoint.setFeature(Arrays.copyOfRange(values, 0, values.length - 1));
        int centroidCount = Integer.parseInt(context.getConfiguration().get("centroidCount"));
        double min_dis = Double.MAX_VALUE;

        for (int centroidIdx = 0; centroidIdx < centroidCount; centroidIdx++) {
            String[] centerStr = context.getConfiguration().getStrings(Integer.toString(centroidIdx));
            centroid.setFeature(centerStr);

            double dis = centroid.distance(currentPoint);
            if (dis < min_dis) {
                min_dis = dis;
                closestCenter.set(centroidIdx);
            }
        }

        context.write(closestCenter, currentPoint);

    }
}
