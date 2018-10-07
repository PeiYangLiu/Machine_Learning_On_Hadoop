package hadoop_machine_learning.move_average;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class TimeSeriesData implements Writable, Comparable<TimeSeriesData> {

    private long timestamp;
    private double value;

    public TimeSeriesData(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public TimeSeriesData() {
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    /**
     * Used in sorting the data in the reducer
     */
    @Override
    public int compareTo(TimeSeriesData data) {
        if (this.timestamp  < data.timestamp ) {
            return -1;
        }
        else if (this.timestamp  > data.timestamp ) {
            return 1;
        }
        else {
            return 0;
        }
    }

    public String toString() {
        return "("+timestamp+","+value+")";
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.timestamp);
        dataOutput.writeDouble(this.value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.timestamp  = dataInput.readLong();
        this.value  = dataInput.readDouble();
    }

    /**
     * Convert a binary data into TimeSeriesData
     *
     * @param in A DataInput object to read from.
     * @return A TimeSeriesData object
     * @throws IOException
     */
    public static TimeSeriesData read(DataInput in) throws IOException {
        TimeSeriesData tsData = new TimeSeriesData();
        tsData.readFields(in);
        return tsData;
    }

    public String getDate() {
        return DateUtil.getDateAsString(this.timestamp);
    }

    /**
     * Creates a clone of this object
     */
    public TimeSeriesData clone() {
        return new TimeSeriesData(timestamp, value);
    }


}
