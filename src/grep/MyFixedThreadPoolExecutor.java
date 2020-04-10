package grep;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MyFixedThreadPoolExecutor {

    private static int KEEP_ALIVE_TIME = 100;
    private static TimeUnit TIME_UNIT_OF_KEEP_ALIVE_TIME = TimeUnit.MILLISECONDS;

    private AtomicInteger workerCount;
    private int poolSize;
    private BlockingQueue<Runnable> queue;

    private volatile boolean shutDown;

    public MyFixedThreadPoolExecutor(int poolSize) {
        workerCount = new AtomicInteger();
        this.poolSize = poolSize;
        queue = new LinkedBlockingDeque<>();
    }

    private class Worker implements Runnable {
        private Thread t;
        private Runnable task;

        Worker(Runnable task) {
            t = Thread.currentThread();
            this.task = task;
        }

        @Override
        public void run() {
            runWorker(this);
        }

        /*
        public void interrupt() {
            if (t.isAlive()) {
                t.interrupt();
            }
        }
        */
    }

    public void execute(Runnable task) {
        if (shutDown) {
            throw new RejectedExecutionException();
        }
        if (task == null) {
            throw new NullPointerException();
        }
        if (workerCount.get() < poolSize) {
            if (workerCount.incrementAndGet() <= poolSize) {
                Worker w = new Worker(task);
                new Thread(w).start();
                return;
            }
        }
        queue.add(task);
    }

    private void runWorker(Worker w) {
        Runnable task = w.task;
        w.task = null;
        while (task != null) {
            task.run();
            task = getTask();
        }
        workerCount.decrementAndGet();
    }

    public Runnable getTask() {
        Runnable task = null;
        try {
            task = shutDown ? queue.poll() : queue.poll(KEEP_ALIVE_TIME, TIME_UNIT_OF_KEEP_ALIVE_TIME);
        } catch (InterruptedException e) {
            System.out.println(Thread.currentThread() + " : Interrupted while fetching task from queue");
        }
        return task;
    }

    public void shutdown() {
        if (!shutDown) {
            shutDown = true;
            while (workerCount.get() > 0);
        }
    }

    /*
    public List<Runnable> shutdownNow() {
        lock.lock();
        if (!stop) {
            stop = true;
            for (Worker w : workerSet) {
                w.interrupt();
            }
        }
        List<Runnable> remainingTasks = new ArrayList<>();
        queue.drainTo(remainingTasks);
        lock.unlock();
        return remainingTasks;
    }
    */

}