package org.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PointWritable implements WritableComparable<PointWritable> {
    private double[] features;

    public PointWritable() {
        // Empty constructor needed for serialization/deserialization
    }

    public PointWritable(double[] features) {
        this.features = features;
    }

    public void setFeature(String[] strings) {
        features = new double[strings.length];
        for (int i = 0; i < strings.length; i++) {
            features[i] = Double.parseDouble(strings[i]);
        }
    }
    public PointWritable(String[] features) {
        this.features = new double[features.length];
        for (int i = 0; i < features.length; i++) {
            this.features[i] = Double.parseDouble(features[i]);
        }
    }

    public void add(PointWritable p) {
        for (int i = 0; i < p.getNumFeatures(); i++) {
            this.features[i] += p.getFeature(i);
        }
    }

    public int getNumFeatures() {
        return this.features.length;
    }

    public double getFeature(int i) {
        return features[i];
    }

    public void setFeature(int i, double value) {
        features[i] = value;
    }

    public double distance(PointWritable p) {
        double dis = 0;
        for (int i = 0; i < p.getNumFeatures(); i++) {
            double diff = p.getFeature(i) - getFeature(i);
            dis += diff * diff;
        }
        return Math.sqrt(dis);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(features.length);
        for (double feature : features) {
            out.writeDouble(feature);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        features = new double[length];
        for (int i = 0; i < length; i++) {
            features[i] = in.readDouble();
        }
    }

    @Override
    public int compareTo(PointWritable other) {
        // Assuming points with fewer features come before points with more features
        int minLength = Math.min(features.length, other.features.length);
        for (int i = 0; i < minLength; i++) {
            if (features[i] < other.features[i]) {
                return -1;
            } else if (features[i] > other.features[i]) {
                return 1;
            }
        }
        return Integer.compare(features.length, other.features.length);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        for (double feature : features) {
            long temp = Double.doubleToLongBits(feature);
            result = prime * result + (int) (temp ^ (temp >>> 32));
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PointWritable other = (PointWritable) obj;
        if (features.length != other.features.length)
            return false;
        for (int i = 0; i < features.length; i++) {
            if (features[i] != other.features[i])
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < features.length; i++) {
            sb.append(features[i]);
            if (i < features.length - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

}
