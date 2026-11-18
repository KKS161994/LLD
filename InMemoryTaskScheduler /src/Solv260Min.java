

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Solv260Min {

    enum Status {
        SCHEDULED, RUNNING, COMPLETED, FAILED, CANCELLED
    }

    static class Task {
        final int id;
        final Runnable action;
        final long intervalMs;
        volatile long nextRunMs;

        final AtomicReference<Status> status =
                new AtomicReference<>(Status.SCHEDULED);

        Task(int id, Runnable action, long nextRunMs, long intervalMs) {
            this.id = id;
            this.action = action;
            this.nextRunMs = nextRunMs;
            this.intervalMs = intervalMs;
        }
    }

    static class Scheduler implements AutoCloseable {
        private final Object lock = new Object();

        private final PriorityQueue<Task> pending =
                new PriorityQueue<>(
                    Comparator.comparingLong((Task t) -> t.nextRunMs)
                              .thenComparingInt(t -> t.id)
                );

        private final Map<Integer, Task> tasks =
                new ConcurrentHashMap<>();

        private final AtomicInteger ids = new AtomicInteger();
        private final Semaphore slots;

        private final ExecutorService workers =
                Executors.newVirtualThreadPerTaskExecutor();

        private final Thread schedulerThread;
        private volatile boolean running = true;

        Scheduler(int concurrency) {
            if (concurrency <= 0)
                throw new IllegalArgumentException("concurrency");

            slots = new Semaphore(concurrency);

            schedulerThread = Thread.ofPlatform()
                    .name("scheduler")
                    .start(this::loop);
        }

        int schedule(Runnable action, long delayMs, long intervalMs) {
            Objects.requireNonNull(action);

            if (delayMs < 0 || intervalMs < 0)
                throw new IllegalArgumentException();

            synchronized (lock) {
                if (!running)
                    throw new IllegalStateException("closed");

                int id = ids.incrementAndGet();

                Task task = new Task(
                        id,
                        action,
                        System.currentTimeMillis() + delayMs,
                        intervalMs
                );

                tasks.put(id, task);
                pending.offer(task);
                lock.notifyAll();

                return id;
            }
        }

        boolean cancel(int id) {
            Task task = tasks.get(id);
            if (task == null) return false;

            synchronized (lock) {
                if (!task.status.compareAndSet(
                        Status.SCHEDULED, Status.CANCELLED))
                    return false;

                pending.remove(task);
                lock.notifyAll();
                return true;
            }
        }

        Status status(int id) {
            Task task = tasks.get(id);
            return task == null ? null : task.status.get();
        }

        private void loop() {
            try {
                while (true) {
                    Task task;

                    synchronized (lock) {
                        while (true) {
                            if (!running) return;

                            task = pending.peek();

                            if (task == null) {
                                lock.wait();
                                continue;
                            }

                            long delay =
                                    task.nextRunMs - System.currentTimeMillis();

                            if (delay > 0) {
                                lock.wait(delay);
                                continue;
                            }

                            if (!slots.tryAcquire()) {
                                lock.wait();
                                continue;
                            }

                            pending.poll();

                            if (!task.status.compareAndSet(
                                    Status.SCHEDULED, Status.RUNNING)) {
                                slots.release();
                                continue;
                            }

                            break;
                        }
                    }

                    Task selected = task;

                    try {
                        workers.execute(() -> execute(selected));
                    } catch (RejectedExecutionException e) {
                        selected.status.set(Status.FAILED);
                        slots.release();

                        synchronized (lock) {
                            lock.notifyAll();
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void execute(Task task) {
            try {
                task.action.run();

                synchronized (lock) {
                    if (task.intervalMs > 0 && running) {
                        task.nextRunMs =
                                System.currentTimeMillis() + task.intervalMs;

                        task.status.set(Status.SCHEDULED);
                        pending.offer(task);
                        lock.notifyAll();
                    } else {
                        task.status.set(Status.COMPLETED);
                    }
                }

            } catch (Exception e) {
                task.status.set(Status.FAILED);
            } finally {
                slots.release();

                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        }

        @Override
        public void close() {
            synchronized (lock) {
                running = false;

                for (Task task : pending) {
                    task.status.compareAndSet(
                            Status.SCHEDULED, Status.CANCELLED);
                }

                pending.clear();
                lock.notifyAll();
            }

            schedulerThread.interrupt();
            workers.shutdown();
        }
    }

    public static void main(String[] args) throws Exception {
        try (Scheduler scheduler = new Scheduler(2)) {

            int once = scheduler.schedule(
                    () -> System.out.println("one-time"),
                    100, 0
            );

            int recurring = scheduler.schedule(
                    () -> System.out.println("recurring"),
                    50, 150
            );

            int cancelled = scheduler.schedule(
                    () -> System.out.println("SHOULD NOT RUN"),
                    300, 0
            );

            System.out.println(
                    "cancelled=" + scheduler.cancel(cancelled)
            );

            Thread.sleep(550);

            System.out.println("once=" + scheduler.status(once));
            System.out.println("recurring=" + scheduler.status(recurring));

            System.out.println(
                    "cancel recurring=" + scheduler.cancel(recurring)
            );
        }
    }
}
