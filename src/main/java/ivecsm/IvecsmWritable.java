package ivecsm;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IvecsmWritable implements Writable {

    private String item;
    private double[] vector;

    public IvecsmWritable() {
        this.item = "";
        this.vector = new double[0];
    }

    public IvecsmWritable(String item, double[] vector)
    {
        this.item = item;
        this.vector = vector.clone();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(item);

        out.writeInt(vector.length);
        for (int i = 0; i < vector.length; i++) {
            out.writeDouble(vector[i]);
        }
    }

    public void readFields(DataInput in) throws IOException {
        item = in.readUTF();

        int len = in.readInt();
        vector = new double[len];

        for (int i = 0; i < len; i++) {
            vector[i] = in.readDouble();
        }
    }

    public void set(String item, double[] vector) {
        this.item = item;
        this.vector = vector.clone();
    }

    public String getItem() {
        return item;
    }

    public double[] getVector() {
        return vector;
    }
}
