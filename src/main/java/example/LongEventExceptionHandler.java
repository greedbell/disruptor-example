package example;

import com.lmax.disruptor.ExceptionHandler;

public class LongEventExceptionHandler implements ExceptionHandler<LongEvent> {
    @Override
    public void handleEventException(Throwable ex, long sequence, LongEvent event) {
        ex.printStackTrace();
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        ex.printStackTrace();
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        ex.printStackTrace();
    }
}
