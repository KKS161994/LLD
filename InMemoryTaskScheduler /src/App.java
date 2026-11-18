import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class App {

    static enum Status {
        CREATED,
        SCHEDULED,
        RUNNING,
        FAILED,
        COMPLETED,
        CANCELLED,
    }

    static enum Type {
        OneTime,
        Recurring,
    }

    static enum Priority {
        CRITICAL,
        HIGH,

        LOW,
    }

    static class Task {
        int id;
        String idempotencyKey;
        long startTime;
        long nextRun;
        long interval;
        AtomicReference<Status> status;
        Priority priority;
        Runnable task;
        Type type;

        private Task() {
            status = new AtomicReference<>();
        }

        public int getId() {
            return id;
        }

        public String getIdempotencyKey() {
            return idempotencyKey;
        }

        public long getInterval() {
            return interval;
        }

        public long getNextRun() {
            return nextRun;
        }

        public Priority getPriority() {
            return priority;
        }

        public long getStartTime() {
            return startTime;
        }

        public AtomicReference<Status> getStatus() {
            return status;
        }

        public Runnable getTask() {
            return task;
        }

        public void execute() {
            task.run();
        }

        public Type getType() {
            return type;
        }

        public boolean claim() {
            return status.compareAndSet(Status.SCHEDULED, Status.RUNNING);
        }

        public void updateNextRun(long time) {
            this.nextRun = time + interval;
        }

        static class Builder {
            Task task;

            Builder() {
                this.task = new Task();
            }

            Builder setId(int id) {
                task.id = id;
                return this;
            }

            Builder setIdempotencykey(String key) {
                task.idempotencyKey = key;
                return this;
            }

            // this.startTime = startTime;
            // this.interval = interval;
            // this.nextRun = startTime;
            // this.status = new AtomicReference<>();
            // this.status.set(status);
            // this.priority = priority;
            // this.task = task;
            Builder setStartTime(Long time) {
                task.startTime = time;
                task.nextRun = time;
                return this;
            }

            Builder setInterval(Long time) {
                task.interval = time;
                return this;

            }

            Builder setType(Type type) {
                task.type = type;
                return this;

            }

            Builder setPriority(int priority) {

                task.priority = switch (priority) {
                    case 0 -> Priority.CRITICAL;
                    case 1 -> Priority.HIGH;
                    case 2 -> Priority.LOW;
                    default -> throw new IllegalArgumentException(
                            "Invalid priority: " + priority);
                };
                return this;

            }

            Builder setTask(Runnable runnable) {
                this.task.task = runnable;
                return this;
            }

            Task build() {
                task.status.set(Status.CREATED);
                return task;
            }

        }

    }

    static class TaskSchedulerService {
        int capacity;
        private ConcurrentHashMap<String, Integer> idempotencyKeys;
        AtomicInteger idGenerator = new AtomicInteger();
        TaskExecutor taskExecutor;

        TaskSchedulerService(int n) {
            this.capacity = n;
            idempotencyKeys = new ConcurrentHashMap<>();
            this.taskExecutor = new TaskExecutor(n);
        }

        int addTask(String idempotencyKey, long startTime, long interval, int priority, Runnable task, Type type) {

            if (idempotencyKeys.containsKey(idempotencyKey))
                return idempotencyKeys.get(idempotencyKey);

            int id = idempotencyKeys.computeIfAbsent(
                    idempotencyKey,
                    key -> {
                        int newId = idGenerator.incrementAndGet();
                        Task job = new Task.Builder()
                                .setIdempotencykey(idempotencyKey)
                                .setId(newId)
                                .setStartTime(startTime)
                                .setInterval(interval)
                                .setTask(task)
                                .setType(type)
                                .setPriority(priority)
                                .build();
                        taskExecutor.addJob(job);
                        // Create and register the task consistently.
                        return newId;
                    });
            return id;
        }

    }

    static class TaskExecutor implements Closeable {
        int capacity;
        Semaphore permits;
        ExecutorService executorService;
        Comparator<Task> futureQueueComparator;
        Comparator<Task> readyQueueComparator;
        PriorityQueue<Task> readyQueue, futureQueue;
        Map<Integer, Task> tasksMap = new ConcurrentHashMap<>();
        Object lock;
        long currentTime;
        private volatile boolean isRunning = true;
        private final Thread schedulerThread;

        TaskExecutor(int capacity) {
            this.capacity = capacity;
            this.permits = new Semaphore(capacity);
            executorService = Executors.newVirtualThreadPerTaskExecutor();
            futureQueueComparator = Comparator
                    .comparingLong(Task::getNextRun);
            readyQueueComparator = Comparator.comparing(Task::getPriority).thenComparingLong(Task::getNextRun)
                    .thenComparingInt(Task::getId);
            readyQueue = new PriorityQueue<>(readyQueueComparator);
            futureQueue = new PriorityQueue<>(futureQueueComparator);
            lock = new Object();

            schedulerThread = Thread.ofPlatform().name("scheule-loop")
                    .start(this::scheduleLoop);
        }

        boolean addJob(Task task) {
            synchronized (lock) {
                futureQueue.add(task);
                tasksMap.put(task.getId(), task);
                task.status.compareAndSet(Status.CREATED, Status.SCHEDULED);
                lock.notifyAll();
            }
            return true;
        }

        void scheduleLoop() {

            while (isRunning) {
                try {
                    Task selectedJob;
                    synchronized (lock) {
                        currentTime = Instant.now().getEpochSecond();
                        while (!futureQueue.isEmpty() && futureQueue.peek().nextRun <= currentTime) {
                            readyQueue.add(futureQueue.poll());
                        }
                        if (readyQueue.isEmpty()) {
                            if (futureQueue.isEmpty()) {
                                lock.wait();
                            } else {
                                long delaySeconds = futureQueue.peek().getNextRun() - currentTime;

                                if (delaySeconds > 0) {
                                    lock.wait(Math.multiplyExact(delaySeconds, 1000L));
                                }
                            }
                            continue;
                        }

                        if (!permits.tryAcquire()) {
                            lock.wait();
                            continue;
                        }
                        selectedJob = readyQueue.poll();

                        if (!selectedJob.claim()) {
                            permits.release();
                            continue;
                        }

                    }
                    dispatchJob(selectedJob);
                } catch (InterruptedException exception) {
                    isRunning = false;
                }
            }
        }

        void dispatchJob(Task task) {
            try {
                executorService.submit(() -> {
                    try {
                        task.task.run();
                        onSucess(task);
                    } catch (Throwable failure) {
                        task.status.set(Status.FAILED);
                        onFailure(task);

                    } finally {
                        permits.release();
                        synchronized (lock) {
                            lock.notifyAll();
                        }
                    }
                });
            } catch (RejectedExecutionException e) {
                synchronized (lock) {
                    // Restore task eligibility if the executor refused it.
                    permits.release();
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
            }
        }

        private void onFailure(Task task) {

            synchronized (lock) {
                task.status.compareAndSet(Status.RUNNING, Status.FAILED);
                lock.notifyAll();
            }
        }

        boolean cancelTask(int id) {
            Task task = tasksMap.get(id);

            if (task == null) {
                return false;
            }

            return task.status.compareAndSet(
                    Status.SCHEDULED,
                    Status.CANCELLED);
        }

        String getStatus(int id) {
            return tasksMap.get(id).getStatus().get().toString();
        }

        void onSucess(Task task) {
            task.status.set(Status.COMPLETED);
            if (task.getType().equals(Type.OneTime))
                return;
            synchronized (lock) {
                task.updateNextRun(
                        Instant.now().getEpochSecond());

                task.status.set(Status.SCHEDULED);
                futureQueue.offer(task);
                lock.notifyAll();
            }
        }

        @Override
        public void close() throws IOException {
            synchronized (lock) {
                isRunning = false;
                lock.notifyAll();
            }

            schedulerThread.interrupt();
            executorService.shutdown();
        }

    }

}
