package concurrent;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FixedThreadPool implements ExecutorService {

    private int poolSize;

    private AtomicInteger workerCounter = new AtomicInteger();
    private volatile HashSet<Worker> workerSet = new HashSet<>();
    private volatile Queue<Runnable> taskQueue = new LinkedList<>();
    private final Object mainLock = new Object();
    private final Object queueLock = new Object();
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
                synchronized (mainLock) {
                    workerSet.add(w);
                }
                w.t.start();
            } else {
                workerCounter.decrementAndGet();
            }
        } else {
            synchronized (queueLock) {
                taskQueue.add(command);
            }
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
            synchronized (mainLock) {
                if (currentState == State.RUNNING || currentState == State.SHUTDOWN) {
                    currentState = State.SHUTDOWN_NOW;
                    for (Worker w : workerSet) {
                        w.interrupt();
                    }
                    workerSet.clear();
                    workerCounter.getAndSet(0);
                    List<Runnable> remainingTasks;
                    synchronized (queueLock) {
                        remainingTasks = new ArrayList<>(taskQueue.size());
                        for (Runnable r : taskQueue) {
                            remainingTasks.add(r);
                        }
                        taskQueue.clear();
                    }
                    currentState = State.TERMINATED;
                    mainLock.notifyAll();
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
        synchronized (mainLock) {
            if(currentState == State.TERMINATED) {
                return true;
            }
            try {
                mainLock.wait(unit.toMillis(timeout));
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

    private class Worker implements Runnable {
        Thread t;
        Runnable task;

        public Worker(Runnable task) {
            this.task = task;
            t = new Thread(this);
        }

        @Override
        public void run() {
            while(currentState==State.RUNNING || task!=null) {
                if(task!=null) {
                    task.run();
                }
                synchronized (queueLock) {
                    task = taskQueue.poll();
                }
            }
            workerCounter.decrementAndGet();
            synchronized (mainLock) {
                workerSet.remove(this);
                if(workerSet.size()==0 && currentState == State.SHUTDOWN && taskQueue.size()==0) {
                    mainLock.notifyAll();
                }
            }
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
