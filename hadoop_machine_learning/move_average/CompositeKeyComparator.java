package hadoop_machine_learning.move_average;

import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator(){
        super(CompositeKey.class, true);
    }
}
