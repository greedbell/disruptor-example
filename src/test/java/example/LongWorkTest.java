package example;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.*;

/**
 * 使用 WorkerPool WorkHandler
 */
public class LongWorkTest {

    @Test
    public void testLongWork() {
        long begin = System.currentTimeMillis();

        // The factory for the event
        LongEventFactory factory = new LongEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024;
        final int DATA_SIZE = 10;

        // 多 producers，线程安全
        Disruptor<LongEvent> disruptor = new Disruptor<>(factory, bufferSize, Executors.defaultThreadFactory());
        // 单 producer，非线程安全
//        Disruptor<LongEvent> disruptor = new Disruptor<>(factory, bufferSize, Executors.defaultThreadFactory(), ProducerType.SINGLE, new BlockingWaitStrategy());

        LongWorkHandler[] handlers = new LongWorkHandler[4];
        for (int i = 0; i < handlers.length; i++) {
            handlers[i] = new LongWorkHandler(i);
        }

        // 保证单个事件按顺序执行，有多少个 handler 就会创建多少个线程
        disruptor.handleEventsWithWorkerPool(handlers)
                .thenHandleEventsWithWorkerPool(new LongWorkHandler(10));
        disruptor.start();

        // 多个生产者
        RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();
        LongEventProducer producer = new LongEventProducer(ringBuffer);
        CountDownLatch countDownLatch = new CountDownLatch(2);
        Thread aThread = new Thread(new ProducerRunnable(producer, countDownLatch));
        Thread bThread = new Thread(new ProducerRunnable(producer, countDownLatch));
        aThread.start();
        bThread.start();

        // 等待生产者完成
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 等待消费者完成
        disruptor.shutdown();

        long end = System.currentTimeMillis();
        System.out.println("end: " + (end - begin));
    }

    @Test
    public void testLongWorkWithExecutor() {
        long begin = System.currentTimeMillis();

        // The factory for the event
        LongEventFactory factory = new LongEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024;

//        RingBuffer<LongEvent> ringBuffer = RingBuffer.createSingleProducer(factory, bufferSize);
        RingBuffer<LongEvent> ringBuffer = RingBuffer.createMultiProducer(factory, bufferSize);

        SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();

        final int EVENT_NUMBER = 20;
        final int HANDLER_NUMBER = 4;
        CountDownLatch latch = new CountDownLatch(EVENT_NUMBER * 2);
        LongWorkHandler[] handlers = new LongWorkHandler[HANDLER_NUMBER];
        for (int i = 0; i < handlers.length; i++) {
            handlers[i] = new LongWorkHandler(i, latch);
        }

        WorkerPool<LongEvent> customer = new WorkerPool<>(ringBuffer,
                sequenceBarrier,
                new LongEventExceptionHandler(),
                handlers);

        ringBuffer.addGatingSequences(customer.getWorkerSequences());

        ThreadPoolExecutor executor = new ThreadPoolExecutor(7, 20, 10, TimeUnit.MINUTES, new LinkedBlockingQueue<>(100));
        customer.start(executor);

        for (int i = 0; i < 20; i++) {
            long seq = ringBuffer.next();

            System.out.println("seq: " + seq);

            ringBuffer.get(seq).setValue((long) i);
            ringBuffer.publish(seq);
        }

        try {
            // 等待所有处理完成
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        customer.halt();

        // 只是不能再提交新任务，等待执行的任务不受影响
        executor.shutdown();

        long end = System.currentTimeMillis();
        System.out.println("end: " + (end - begin));
    }

    private static class ProducerRunnable implements Runnable {
        final LongEventProducer producer;
        final CountDownLatch countDownLatch;

        public ProducerRunnable(LongEventProducer producer, CountDownLatch countDownLatch) {
            this.producer = producer;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            System.out.println("ProducerRunnable: " + this + " Thread: " + Thread.currentThread().getName());
            ByteBuffer bb = ByteBuffer.allocate(8);
            for (long l = 0; l < 10; l++) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                bb.putLong(0, l);
                producer.onData(bb);
            }
            countDownLatch.countDown();
        }
    }
}
