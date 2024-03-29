package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

public class KMeansDriver {

    private static String getRandomCentroid(double[] lowerBounds, double[] upperBounds) {
        StringBuilder centroid = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < lowerBounds.length - 1; i++)
            centroid.append(lowerBounds[i] + (upperBounds[i] - lowerBounds[i]) * random.nextDouble()).append(",");
        centroid.append(lowerBounds[lowerBounds.length - 1] + (upperBounds[lowerBounds.length - 1] - lowerBounds[lowerBounds.length - 1]) * random.nextDouble());
        return centroid.toString();
    }

    private static String[] readNewCentroids(String hdfsPath, int numberOfClasses) {
        String[] newCentroids = new String[numberOfClasses];
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(new Path(hdfsPath));
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = br.readLine()) != null) {
                String[] splitLine = line.split("\t");
                newCentroids[Integer.parseInt(splitLine[0])] = splitLine[1];
            }
            br.close();
            inputStream.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return newCentroids;
    }

    private static boolean shouldStop(String[] oldCentroids, String[] newCentroids) {
        System.out.println(Arrays.toString(oldCentroids));
        System.out.println(Arrays.toString(newCentroids));
        for (int i = 0; i < oldCentroids.length; i++) {
            PointWritable oldCentroid = new PointWritable();
            oldCentroid.setFeature(oldCentroids[i].split(","));
            PointWritable newCentroid = new PointWritable();
            newCentroid.setFeature(newCentroids[i].split(","));
            if (oldCentroid.distance(newCentroid) > 0.0001)
                return false;
        }
        return true;
    }

    private static double getAccuracy(String hdfsDataFilePath, String[] centroids) {
        HashMap<Integer, HashMap<String, Integer>> centroidAssignments = new HashMap<>();
        int dataSize = 0;
        PointWritable[] centroidPoints = new PointWritable[centroids.length];
        for (int i = 0; i < centroids.length; i++) {
            centroidPoints[i] = new PointWritable(centroids[i].split(","));
            centroidAssignments.put(i, new HashMap<>());
        }
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(new Path(hdfsDataFilePath));
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = br.readLine()) != null) {
                dataSize++;
                String[] split = line.split(",");
                String label = split[split.length - 1];
                PointWritable point = new PointWritable(Arrays.copyOfRange(split, 0, split.length - 1));
                double minDistance = Double.MAX_VALUE;
                int minCentroid = -1;
                for (int i = 0; i < centroidPoints.length; i++) {
                    double distance = point.distance(centroidPoints[i]);
                    if (distance < minDistance) {
                        minDistance = distance;
                        minCentroid = i;
                    }
                }
                centroidAssignments.get(minCentroid)
                        .put(label, centroidAssignments
                                .get(minCentroid).getOrDefault(label, 0) + 1);
            }
            br.close();
            inputStream.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        int trueAssignments = 0;
        for (int i = 0; i < centroidPoints.length; i++) {
            int maxCount = Integer.MIN_VALUE;
            for (int count : centroidAssignments.get(i).values())
                if (count > maxCount)
                    maxCount = count;
            trueAssignments += maxCount;
        }
        return trueAssignments / (double) dataSize;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage KMeansDriver <input_dir> <output_dir> <number_of_classes>");
            System.exit(1);
        }

        int numberOfClasses = Integer.parseInt(args[2]);
        double[] lowerBounds = new double[] {4.3, 3.0, 1.1, 0.1};
        double[] upperBounds = new double[] {7.9, 3.8, 6.4, 2.0};

        Configuration conf = new Configuration();
        conf.set("centroidCount", String.valueOf(numberOfClasses));

        String[] oldCentroids = new String[numberOfClasses];
        for (int i = 0; i < numberOfClasses; i++) {
            oldCentroids[i] = getRandomCentroid(lowerBounds, upperBounds);
            conf.set(String.valueOf(i), oldCentroids[i]);
        }

        String input = args[0];
        String output = args[1];

        String[] newCentroids = new String[0];
        for (int i = 0; i < 1000; i++) {
            System.out.println("Iteration " + i);

            FileSystem fs = FileSystem.get(conf);
            boolean exists = fs.exists(new Path(output));
            if (exists)
                fs.delete(new Path(output), true);

            Job job = Job.getInstance(conf, "Iteration " + i);
            job.setJarByClass(KMeansDriver.class);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(PointWritable.class);

            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));

            if (!job.waitForCompletion(true)) {
                System.err.println("Iteration " + i + " Failed!");
                System.exit(1);
            }
            newCentroids = readNewCentroids(output + "/part-r-00000", numberOfClasses);
            for (int j = 0; j < numberOfClasses; j++)
                if (newCentroids[j] == null)
                    newCentroids[j] = oldCentroids[j];
            if (shouldStop(oldCentroids, newCentroids))
                break;
            for (int j = 0; j < numberOfClasses; j++)
                conf.set(String.valueOf(j), newCentroids[j]);
            oldCentroids = newCentroids;
        }
        
        System.out.println(getAccuracy(input + "/iris.data.txt", newCentroids));
    }
}