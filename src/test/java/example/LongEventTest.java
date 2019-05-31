package example;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Official Example
 * <p/>
 *
 * @see <a href=https://github.com/LMAX-Exchange/disruptor/wiki/Getting-Started#basic-event-produce-and-consume>Basic Event Produce and Consume</a>
 */
public class LongEventTest {
    /**
     * @see <a href="https://github.com/LMAX-Exchange/disruptor/wiki/Getting-Started#basic-event-produce-and-consume">Publishing Using Translators</a>
     */
    @Test
    public void testLongEvent() {
        // The factory for the event
        LongEventFactory factory = new LongEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024;
        final int DATA_SIZE = 10;

        // Construct the Disruptor
        Disruptor<LongEvent> disruptor = new Disruptor<>(factory, bufferSize, DaemonThreadFactory.INSTANCE);

//        Disruptor<LongEvent> disruptor = new Disruptor(factory, bufferSize, DaemonThreadFactory.INSTANCE, ProducerType.SINGLE, new BlockingWaitStrategy());

        // Connect the handler
        // 保证单个事件按顺序执行
        disruptor.handleEventsWith(new LongEventHandler())
                .then(new LongEventHandler(), new LongEventHandler())
                .then(new LongEventClearHandler());

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();

        LongEventProducer producer = new LongEventProducer(ringBuffer);
        ByteBuffer bb = ByteBuffer.allocate(8);
        for (long l = 0; l < DATA_SIZE; l++) {
            bb.putLong(0, l);
            producer.onData(bb);
        }

        System.out.println("begin halt");
        // 等待所有任务完成
        disruptor.shutdown();
        System.out.println("end halt");
    }

    @Test
    public void testLongEventWithLamdbas() {
        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024;

        // Construct the Disruptor
        Disruptor<LongEvent> disruptor = new Disruptor<>(LongEvent::new, bufferSize, DaemonThreadFactory.INSTANCE);

        // Connect the handler
        disruptor.handleEventsWith((event, sequence, endOfBatch) -> System.out.println("Event: " + event + " Thread: " + Thread.currentThread().getName()),
                (event, sequence, endOfBatch) -> System.out.println("Event: " + event + " Thread: " + Thread.currentThread().getName()),
                (event, sequence, endOfBatch) -> System.out.println("Event: " + event + " Thread: " + Thread.currentThread().getName()));

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();

        ByteBuffer bb = ByteBuffer.allocate(8);
        for (long l = 0; true; l++) {
            bb.putLong(0, l);
            ringBuffer.publishEvent((event, sequence, buffer) -> event.setValue(buffer.getLong(0)), bb);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void testLongEventWithMethod() {
        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024;

        // Construct the Disruptor
        Disruptor<LongEvent> disruptor = new Disruptor<>(LongEvent::new, bufferSize, DaemonThreadFactory.INSTANCE);

        // Connect the handler
        disruptor.handleEventsWith(this::handleEvent, this::handleEvent, this::handleEvent);

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();

        ByteBuffer bb = ByteBuffer.allocate(8);
        for (long l = 0; true; l++) {
            bb.putLong(0, l);
            ringBuffer.publishEvent(this::translate, bb);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleEvent(LongEvent event, long sequence, boolean endOfBatch) {
        System.out.println("Event: " + event + " Thread: " + Thread.currentThread().getName());
    }

    private void translate(LongEvent event, long sequence, ByteBuffer buffer) {
        event.setValue(buffer.getLong(0));
    }

}
