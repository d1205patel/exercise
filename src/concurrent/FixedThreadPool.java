package concurrent;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FixedThreadPool implements ExecutorService {

    private long KEEP_ALIVE_TIME = 100L;
    private TimeUnit KEEP_ALIVE_TIME_UNIT = TimeUnit.MILLISECONDS;

    private int poolSize;

    private AtomicInteger workerCounter = new AtomicInteger();
    private volatile HashSet<Worker> workerSet = new HashSet<>();
    private BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private Lock mainLock = new ReentrantLock();
    private State currentState = State.RUNNING;
    private Condition termination = mainLock.newCondition();

    public FixedThreadPool(int size) {
        poolSize = size;
    }


    @Override
    public void execute(Runnable command) {
        Objects.requireNonNull(command);
        if(currentState != State.RUNNING) {
            throw new RejectedExecutionException();
        }

        if(workerCounter.getAndIncrement() <= poolSize) {
            Worker w = new Worker(command);
            mainLock.lock();
            workerSet.add(w);
            w.t.start();
            mainLock.unlock();
        } else {
            workerCounter.decrementAndGet();
            taskQueue.add(command);
        }
    }

    @Override
    public void shutdown() {
        if(currentState == State.RUNNING) {
            currentState = State.SHUTDOWN;
            while(true) {
                mainLock.lock();
                if(workerCounter.get() == 0 && currentState == State.SHUTDOWN) {
                    currentState = State.TERMINATED;
                    termination.signalAll();
                    mainLock.unlock();
                    break;
                }
                mainLock.unlock();
            }
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        if(currentState == State.RUNNING || currentState == State.SHUTDOWN) {
            mainLock.lock();
            currentState = State.SHUTDOWN_NOW;
            stopAllWorkers();
            List<Runnable> remainingTasks = new ArrayList<>(taskQueue.size());
            taskQueue.drainTo(remainingTasks);
            currentState = State.TERMINATED;
            termination.signalAll();
            mainLock.unlock();
            return remainingTasks;
        }
        return new ArrayList<>();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        if(currentState == State.TERMINATED) {
            return true;
        }
        else {
            mainLock.lock();
            try {
                termination.await(timeout, unit);
            } catch (InterruptedException e) {
                //
            } finally {
                mainLock.unlock();
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
        mainLock.lock();
        workerSet.remove(w);
        mainLock.unlock();
        workerCounter.decrementAndGet();
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
        mainLock.lock();
        for(Worker w: workerSet) {
            w.interrupt();
        }
        mainLock.unlock();
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
