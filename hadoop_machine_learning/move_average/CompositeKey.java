package hadoop_machine_learning.move_average;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable<CompositeKey> {

    private String name;
    private Long timestamp;

    public CompositeKey(String name, Long timestamp) {
        this.name = name;
        this.timestamp = timestamp;
    }

    public CompositeKey() {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(CompositeKey other) {
        if(this.name.compareTo(other.name) != 0) {
            return this.name.compareTo(other.name);
        }
        else if(this.timestamp < other.timestamp){
            return -1;
        }
        else if(this.timestamp > other.timestamp){
            return 1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.name);
        dataOutput.writeLong(this.timestamp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.timestamp = dataInput.readLong();
    }
}
