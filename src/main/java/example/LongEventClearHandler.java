package example;


import com.lmax.disruptor.EventHandler;

public class LongEventClearHandler implements EventHandler<LongEvent> {
    @Override
    public void onEvent(LongEvent event, long sequence, boolean endOfBatch) throws Exception {
        event.clear();
        System.out.println("Event Clear Handler: " + event + "Event value: " + event.getValue() + " sequence: " + sequence + " Thread: " + Thread.currentThread().getName());
    }
}
