/**
 * 
 */
package cs522.proj.MR;

import java.text.DecimalFormat;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author Robin
 *
 */
public class Tools {

	public static Text mapWritableToText(MapWritable map) {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		int i = 0;
		Writable value;
		String valueStr;
		for (Entry<Writable, Writable> entry : map.entrySet()) {
			value = entry.getValue();
			if (value instanceof DoubleWritable)
				valueStr = formatDouble(((DoubleWritable) value).get());
			else
				valueStr = value.toString();
			i++;
			sb.append("(");
			sb.append(entry.getKey());
			sb.append(", ");
			sb.append(valueStr);
			sb.append(")");
			if (i != map.size())
				sb.append(", ");
		}
		sb.append("}");
		return new Text(sb.toString());
	}
	
	public static String formatDouble(double num) {
		DecimalFormat formatter = new DecimalFormat("#0.000");
		return formatter.format(num);
	}
}
