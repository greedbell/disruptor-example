package example;

public class LongEvent {
    private long value;
    private long seq;

    public void setValue(long value) {
        this.value = value;
    }

    public long getValue() {
        return this.value;
    }

    public void clear() {
        this.value = 0;
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }
}
