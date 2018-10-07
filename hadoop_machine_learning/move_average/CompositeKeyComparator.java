package hadoop_machine_learning.move_average;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey w1 = (CompositeKey) a;
        CompositeKey w2 = (CompositeKey) b;

        return super.compare(a, b);
    }
}
