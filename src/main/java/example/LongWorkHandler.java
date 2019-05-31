package example;

import com.lmax.disruptor.WorkHandler;

import java.util.concurrent.CountDownLatch;

public class LongWorkHandler implements WorkHandler<LongEvent> {
    final private int tag;
    private CountDownLatch latch;

    public LongWorkHandler(int tag) {
        this.tag = tag;
    }

    public LongWorkHandler(int tag, CountDownLatch latch) {
        this.tag = tag;
        this.latch = latch;
    }

    @Override
    public void onEvent(LongEvent event) throws Exception {
        Thread.sleep(1000);
        event.setValue(event.getValue() * 10);
        System.out.println("LongWorkHandler: " + tag + " Event: " + event + " Event Value: " + event.getValue() + " Event Seq: " + event.getSeq() + " Thread: " + Thread.currentThread().getName());
        if (latch != null) {
            latch.countDown();
        }
    }
}
