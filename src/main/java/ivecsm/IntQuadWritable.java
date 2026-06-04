package ivecsm;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntQuadWritable implements WritableComparable<IntQuadWritable>, Cloneable {

	private int x, y, part1, part2;


	public IntQuadWritable() {
	}

	public IntQuadWritable(int x, int y, int part1, int part2) {
		this.x = x;
		this.y = y;
		this.part1 = part1;
		this.part2 = part2;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(x);
		out.writeInt(y);
		out.writeInt(part1);
		out.writeInt(part2);
	}

	public void readFields(DataInput in) throws IOException {
		x = in.readInt();
		y = in.readInt();
		part1  = in.readInt();
		part2 = in.readInt();
	}

	public String toString() {
		return x + "\t" + y;
	}

	public int getX() {
		return x;
	}

	public int getY() {
		return y;
	}

	public int getPart1(){return part1;}

	public int getPart2(){return part2;}

	public void set(int u,int v, int a, int b) {
		x=u; y=v; part1=a; part2=b;
	}

	public void setX(int v) {
		x=v;
	}

	public void setY(int v) {
		y=v;
	}

	public void setPart1(int v){part1=v;}

	public void setPart2(int v){part2=v;}


	public int compareTo(IntQuadWritable other) {
		int delta = this.x - other.x;
		if (delta != 0) {
			return delta;
		}

		delta = this.y - other.y;
		if (delta != 0) {
			return delta;
		}

		delta = this.part1 - other.part1;
		if (delta != 0) {
			return delta;
		}

		return this.part2 - other.part2;
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof IntQuadWritable)) return false;
		return compareTo((IntQuadWritable)o)==0;
	}

	public Object clone() {
		return new IntQuadWritable(x,y,part1,part2);
	}

	@Override
	public int hashCode() {
		return 31 * (31 * (31 * x + y) + part1) + part2;
	}
}
