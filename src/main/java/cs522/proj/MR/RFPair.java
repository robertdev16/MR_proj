package cs522.proj.MR;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class RFPair implements WritableComparable<RFPair> {

	private String left = "";
	private String right = "";

	
	public RFPair() {
		super();
	}
	
	public RFPair(String left, String right) {
		super();
		this.left = left;
		this.right = right;
	}

	public void set(String left, String right) {
		this.left = left;
		this.right = right;
	}

	public String getLeft() {
		return left;
	}

	public String getRight() {
		return right;
	}

	public void write(DataOutput out) throws IOException {
		Text.writeString(out, left);
		Text.writeString(out, right);
	}

	public void readFields(DataInput in) throws IOException {
		left = Text.readString(in);
		right = Text.readString(in);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((left == null) ? 0 : left.hashCode());
		result = prime * result + ((right == null) ? 0 : right.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RFPair other = (RFPair) obj;
		if (left == null) {
			if (other.left != null)
				return false;
		} else if (!left.equals(other.left))
			return false;
		if (right == null) {
			if (other.right != null)
				return false;
		} else if (!right.equals(other.right))
			return false;
		return true;
	}

	private int convertInt(String s) {
		if ((s == null) || (s.isEmpty()))
			return 0;
		if ("*".compareTo(s) == 0)
			return Integer.MIN_VALUE;
		int result;
		try {
			result = Integer.parseInt(s);
		} catch (NumberFormatException nfe) {
			result = 0;
		}
		return result;
	}

	public int compareTo(RFPair o) {
		if (convertInt(left) != convertInt(o.getLeft())) {
			return convertInt(left) < convertInt(o.getLeft()) ? -1 : 1;
		}
		else if (convertInt(right) != convertInt(o.getRight())) {
			return convertInt(right) < convertInt(o.getRight()) ? -1 : 1;
		}
		else {
			return 0;
		}
	}

	@Override
	public String toString() {
		return "(" + left + ", " + right + ")";
	}
}
