package concurrent;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FixedThreadPool implements ExecutorService {

    private long KEEP_ALIVE_TIME = 100L;
    private TimeUnit KEEP_ALIVE_TIME_UNIT = TimeUnit.MILLISECONDS;

    private int poolSize;

    private AtomicInteger workerCounter = new AtomicInteger();
    private volatile HashSet<Worker> workerSet = new HashSet<>();
    private BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private final Object lock = new Object();
    private State currentState = State.RUNNING;

    public FixedThreadPool(int size) {
        poolSize = size;
    }

    @Override
    public void execute(Runnable command) {
        Objects.requireNonNull(command);
        if(currentState != State.RUNNING) {
            throw new RejectedExecutionException();
        }

        if(workerCounter.get() < poolSize) {
            if (workerCounter.incrementAndGet() <= poolSize) {
                Worker w = new Worker(command);
                synchronized (lock) {
                    workerSet.add(w);
                }
                w.t.start();
            } else {
                workerCounter.decrementAndGet();
            }
        } else {
            taskQueue.add(command);
        }
    }

    @Override
    public void shutdown() {
        if(currentState == State.RUNNING) {
            currentState = State.SHUTDOWN;
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        if(currentState == State.RUNNING || currentState == State.SHUTDOWN) {
            synchronized (lock) {
                if (currentState == State.RUNNING || currentState == State.SHUTDOWN) {
                    currentState = State.SHUTDOWN_NOW;
                    stopAllWorkers();
                    List<Runnable> remainingTasks = new ArrayList<>(taskQueue.size());
                    taskQueue.drainTo(remainingTasks);
                    currentState = State.TERMINATED;
                    lock.notifyAll();
                    return remainingTasks;
                }
            }
        }
        return new ArrayList<>();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        if(currentState == State.TERMINATED) {
            return true;
        }
        synchronized (lock) {
            try {
                lock.wait(unit.toMillis(timeout));
            } catch (InterruptedException e) {
                //
            }
        }
        return false;
    }

    @Override
    public boolean isShutdown() {
        return currentState != State.RUNNING;
    }

    @Override
    public boolean isTerminated() {
        return currentState == State.TERMINATED;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        FutureTask<T> futureTask = new FutureTask<>(Objects.requireNonNull(task));
        execute(futureTask);
        return futureTask;
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        FutureTask<T> futureTask = new FutureTask<>(Objects.requireNonNull(task),result);
        execute(futureTask);
        return futureTask;
    }

    @Override
    public Future<?> submit(Runnable task) {
        return submit(task,null);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        Objects.requireNonNull(tasks);
        int size = tasks.size();
        List<Future<T>> listOfFuture = new ArrayList<>(size);
        for(Callable<T> c : tasks) {
            listOfFuture.add(submit(c));
        }
        for(Future<T> future : listOfFuture) {
            try {
                future.get();
            } catch (ExecutionException e) {
//                System.out.println("ExecutionException occurred !");
            }
        }
        return listOfFuture;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long givenTime = unit.toMillis(timeout) + startTime;

        Objects.requireNonNull(tasks);
        int sz = tasks.size();
        boolean notFinished = false;
        List<Future<T>> listOfFuture = new ArrayList<>(sz);
        for(Callable<T> c : tasks) {
            if(System.currentTimeMillis() > givenTime) {
                notFinished = true;
                break;
            }
            listOfFuture.add(submit(c));
        }

        if(!notFinished) {
            for (Future<T> future : listOfFuture) {
                if (System.currentTimeMillis() > givenTime) {
                    notFinished = true;
                    break;
                }
                try {
                    future.get(givenTime - System.currentTimeMillis(),TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
//                    System.out.println("ExecutionException occurred !");
                } catch (TimeoutException e) {
                    notFinished = true;
                    break;
                }
            }
        }

        if(notFinished) {
            for (Future<T> future : listOfFuture) {
                if(!future.isDone()) {
                    future.cancel(true);
                }
            }
        }
        return listOfFuture;
    }

    //<----------------------------------------- Private Helper Methods ------------------------------------>//

    private void runWorker(Worker w) {
        Runnable task = w.task;
        w.task = null;
        while(task!=null) {
            task.run();
            task = getTask();
        }
        workerCounter.decrementAndGet();
        synchronized (lock) {
            workerSet.remove(w);
            if(workerSet.size()==0 && currentState == State.SHUTDOWN && taskQueue.size()==0) {
                lock.notifyAll();
            }
        }
    }

    private Runnable getTask() {
        Runnable task = null;
        try {
            task = currentState == State.SHUTDOWN ? taskQueue.poll() : taskQueue.poll(KEEP_ALIVE_TIME,KEEP_ALIVE_TIME_UNIT);
        } catch (InterruptedException e) {
//            System.out.println("Interrupted while fetching task from queue !");
        }
        return task;
    }

    private void stopAllWorkers() {
        synchronized (lock) {
            for (Worker w : workerSet) {
                w.interrupt();
            }
            workerSet.clear();
        }
        workerCounter.getAndSet(0);
    }

    private class Worker implements Runnable {
        Thread t;
        Runnable task;

        public Worker(Runnable task) {
            this.task = task;
            t = new Thread(this);
        }

        @Override
        public void run() {
            runWorker(this);
        }

        public void interrupt() {
            if(t.isAlive() && !t.isInterrupted()) {
                t.interrupt();
            }
        }
    }

    private enum State { RUNNING, SHUTDOWN, SHUTDOWN_NOW , TERMINATED }

    //<------------------------------------ NOT Implemented Methods ------------------------------------>//

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return null;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
    }
}
