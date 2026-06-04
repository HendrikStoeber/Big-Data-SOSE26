package dsldev;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;

public class DsldevWritable implements Writable {

    private String day;
    private double psd;
    private double pd;
    private double ddev;
    private double rddev;

    public DsldevWritable() {
    }

    public DsldevWritable(String i, double u, double v, double w, double x) {
        this.day = i;
        this.psd = u;
        this.pd = v;
        this.ddev = w;
        this.rddev = x;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(day);
        out.writeDouble(psd);
        out.writeDouble(pd);
        out.writeDouble(ddev);
        out.writeDouble(rddev);
    }

    public void readFields(DataInput in) throws IOException {
        //i = WritableUtils.readVInt(in);
        day = in.readUTF();
        psd = in.readDouble();
        pd=in.readDouble();
        ddev = in.readDouble();
        rddev = in.readDouble();
    }

    public String toString() {
        return day + "\t" + psd + "\t" + pd + "\t" + String.format(Locale.US,"%.4f",ddev) +  "\t" + String.format(Locale.US,"%.4f",rddev);
    }

    public void setDay(String x) {
        day =x;
    }

    public void setDdev(Double x) { ddev =x; }

    public void setPsd(Double x) { psd =x; }

    public void setPd(Double x) { pd =x; }

    public void setRddev(Double x) { rddev =x; }

    public String getDay() {
        return day;
    }

    public double getDdev() { return ddev;}

    public double getPsd() { return psd;}

    public double getPd() { return pd;}

    public double getRddev() { return rddev;}


}
