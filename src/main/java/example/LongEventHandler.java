package example;

import com.lmax.disruptor.EventHandler;

public class LongEventHandler implements EventHandler<LongEvent> {
    @Override
    public void onEvent(LongEvent event, long sequence, boolean endOfBatch) throws Exception {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Event Handler: " + event + "Event value: " + event.getValue() + " sequence: " + sequence + " Thread: " + Thread.currentThread().getName());
    }
}
