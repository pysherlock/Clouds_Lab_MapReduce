package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * TextPair is a Pair of Text that is Writable (Hadoop serialization API)
 * and Comparable to itself.
 *
 */
public class TextPair implements WritableComparable<TextPair> {

    private Text first;
    private Text second;

    public void set(Text first, Text second) {
        this.first = new Text(first);
        this.second = new Text(second);
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public TextPair() {
        first = new Text();
        second = new Text();
    }

    public TextPair(String first, String second) {
        this.set(new Text(first), new Text(second));
    }

    public TextPair(Text first, Text second) {
        this.set(first, second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
        // TODO: write to out the serialized version of this such that
        // can be deserializated in future. This will be use to write to HDFS
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
        // TODO: read from in the serialized version of a Pair and deserialize it
    }//Don't know now ...????

    @Override
    public int hashCode() {
        return (this.first.hashCode()*157 + this.first.hashCode()); //how to calculate the most appropriate hashcode
    }

    @Override
    public boolean equals(Object o) { //I don't know what is it's use
        return false;
    }

    @Override
    public int compareTo(TextPair tp) {  //still have some doubt!!!!
        int rect = this.first.compareTo(tp.getFirst());
        if(rect == 0) { //left word of TextPair is same
            if(this.first.toString().equals('*'))
                return -1;
            else if(tp.first.toString().equals('*'))
                return 1;
         /*   else if(this.first == tp.first && this.second == tp.second)
                return 0;
            else if(this.first == tp.second && this.second == tp.first)
                return 0;*/
        }
        return rect; //Sort the TextPair by left word
    }

    @Override
    public String toString() {
        return this.first.toString() +' '+ this.second.toString();
        // TODO: implement toString for text output format
        // return super.toString();
    }


// DO NOT TOUCH THE CODE BELOW
    /**
     * Compare two pairs based on their values
     */
    public static class Comparator extends WritableComparator {

        /**
         * Reference to standard Hadoop Text comparator
         */
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public Comparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                if (cmp != 0) {
                    return cmp;
                }
                return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                        b2, s2 + firstL2, l2 - firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    static {
        WritableComparator.define(TextPair.class, new Comparator());
    }

    /**
     * Compare just the first element of the Pair
     */
    public static class FirstComparator extends WritableComparator {

        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public FirstComparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof TextPair && b instanceof TextPair) {
                return ((TextPair) a).getFirst().compareTo(((TextPair) b).getFirst());
            }
            return super.compare(a, b);
        }

    }
}
