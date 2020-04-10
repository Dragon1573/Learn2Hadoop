package task2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MyKey implements WritableComparable<MyKey> {
    private int first, second;

    public MyKey() {
    }

    public MyKey(final int first, final int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return this.first;
    }

    public int getSecond() {
        return this.second;
    }

    @Override
    public void readFields(final DataInput inStream) throws IOException {
        this.first = inStream.readInt();
        inStream.readUTF();
        this.second = inStream.readInt();
    }

    @Override
    public void write(final DataOutput outStream) throws IOException {
        outStream.writeInt(this.first);
        outStream.writeUTF(",");
        outStream.writeInt(this.second);
    }

    @Override
    public int compareTo(final MyKey o) {
        final int comparision = this.first - o.first;
        if (comparision == 0) {
            return this.second - o.second;
        }
        return comparision;
    }

    @Override
    public String toString() {
        return this.first + "\t" + this.second;
    }
}
