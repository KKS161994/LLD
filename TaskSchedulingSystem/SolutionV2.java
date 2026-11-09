package TaskSchedulingSystem;



import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class SolutionV2 {

    enum TaskType {
        ONETIME, RECURRING
    }

    enum TaskStatus {
        SCHEDULED, IN_PROGRESS, COMPLETED, FAILED, CANCELLED
    }

    enum TaskPriority {
        HIGH, MEDIUM, LOW
    }

    interface Listener {
        void postExecute(Task task);
    }

    static class Task {
        final long id;
        final TaskType taskType;
        final long startTime;
        final TaskPriority priority;
        final Runnable action;
        final int maxAttempts;
        final long retryDelaySeconds;

        volatile long nextRun;
        volatile long interval;
        volatile int attemptsMade;

        final AtomicReference<TaskStatus> status =
                new AtomicReference<>(TaskStatus.SCHEDULED);

        Task(long id,
             TaskType taskType,
             long startTime,
             long interval,
             TaskPriority priority,
             Runnable action,
             int maxAttempts,
             long retryDelaySeconds) {

            if (taskType == null || priority == null || action == null) {
                throw new IllegalArgumentException("Missing task fields");
            }
            if (taskType == TaskType.RECURRING && interval <= 0) {
                throw new IllegalArgumentException("Invalid recurrence interval");
            }
            if (maxAttempts < 1 || retryDelaySeconds < 0) {
                throw new IllegalArgumentException("Invalid retry policy");
            }

            this.id = id;
            this.taskType = taskType;
            this.startTime = startTime;
            this.nextRun = startTime;
            this.interval = interval;
            this.priority = priority;
            this.action = action;
            this.maxAttempts = maxAttempts;
            this.retryDelaySeconds = retryDelaySeconds;
        }

        boolean cancel() {
            return status.compareAndSet(
                    TaskStatus.SCHEDULED,
                    TaskStatus.CANCELLED
            );
        }

        boolean claim() {
            return status.compareAndSet(
                    TaskStatus.SCHEDULED,
                    TaskStatus.IN_PROGRESS
            );
        }

        long getNextRun() {
            return nextRun;
        }

        TaskPriority getPriority() {
            return priority;
        }

        long getId() {
            return id;
        }
    }

    static class TaskSchedulerService implements Listener, AutoCloseable {

        private final Map<Long, Task> taskMap =
                new ConcurrentHashMap<>();

        private final Comparator<Task> futureComparator =
                Comparator.comparingLong(Task::getNextRun)
                          .thenComparingLong(Task::getId);

        private final Comparator<Task> readyComparator =
                Comparator.comparingInt(
                        (Task t) -> t.getPriority().ordinal()
                )
                .thenComparingLong(Task::getNextRun)
                .thenComparingLong(Task::getId);

        private final PriorityQueue<Task> futureQueue =
                new PriorityQueue<>(futureComparator);

        private final PriorityQueue<Task> readyQueue =
                new PriorityQueue<>(readyComparator);

        private final AtomicLong jobIdGenerator =
                new AtomicLong();

        private final Object schedulerLock = new Object();
        private final Semaphore permits;
        private final ExecutorService executorService;
        private final Thread schedulerThread;

        private volatile boolean running = true;

        TaskSchedulerService(int capacity) {
            if (capacity <= 0) {
                throw new IllegalArgumentException("Capacity must be positive");
            }

            permits = new Semaphore(capacity);
            executorService = Executors.newVirtualThreadPerTaskExecutor();

            schedulerThread = Thread.ofPlatform()
                    .name("task-scheduler")
                    .start(this::schedulerLoop);
        }

        public long schedule(TaskType type,
                             long startTime,
                             long interval,
                             TaskPriority priority,
                             Runnable action,
                             int maxAttempts,
                             long retryDelaySeconds) {

            if (!running) {
                throw new IllegalStateException("Scheduler is stopped");
            }

            long id = jobIdGenerator.incrementAndGet();

            Task task = new Task(
                    id, type, startTime, interval,
                    priority, action, maxAttempts, retryDelaySeconds
            );

            synchronized (schedulerLock) {
                if (!running) {
                    throw new IllegalStateException("Scheduler is stopped");
                }

                taskMap.put(id, task);
                futureQueue.offer(task);
                schedulerLock.notifyAll();
            }

            return id;
        }

        private void schedulerLoop() {
            while (true) {
                Task selected;

                synchronized (schedulerLock) {
                    try {
                        while (true) {
                            if (!running) {
                                return;
                            }

                            long now = Instant.now().getEpochSecond();

                            // Promote all eligible tasks before selection.
                            while (!futureQueue.isEmpty()
                                    && futureQueue.peek().nextRun <= now) {

                                Task due = futureQueue.poll();

                                if (due.status.get() == TaskStatus.SCHEDULED) {
                                    readyQueue.offer(due);
                                }
                            }

                            // Clean cancelled entries lazily.
                            while (!readyQueue.isEmpty()
                                    && readyQueue.peek().status.get()
                                            != TaskStatus.SCHEDULED) {
                                readyQueue.poll();
                            }

                            if (!readyQueue.isEmpty()
                                    && permits.tryAcquire()) {

                                selected = readyQueue.poll();

                                if (selected.claim()) {
                                    break;
                                }

                                permits.release();
                                continue;
                            }

                            // Wait for a worker or a new task.
                            if (!readyQueue.isEmpty() || futureQueue.isEmpty()) {
                                schedulerLock.wait();
                            } else {
                                long delayMillis =
                                        Math.max(1L,
                                                futureQueue.peek().nextRun * 1000L
                                                - System.currentTimeMillis());

                                schedulerLock.wait(delayMillis);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }

                dispatch(selected);
            }
        }

        private void dispatch(Task task) {
            try {
                executorService.submit(() -> {
                    try {
                        task.attemptsMade++;
                        task.action.run();
                        postExecute(task);
                    } catch (Throwable failure) {
                        onFailure(task, failure);
                    } finally {
                        permits.release();
                        signalScheduler();
                    }
                });
            } catch (RejectedExecutionException e) {
                synchronized (schedulerLock) {
                    // Restore task eligibility if the executor refused it.
                    if (running && task.status.compareAndSet(
                            TaskStatus.IN_PROGRESS,
                            TaskStatus.SCHEDULED)) {
                        readyQueue.offer(task);
                    } else {
                        task.status.compareAndSet(
                                TaskStatus.IN_PROGRESS,
                                TaskStatus.FAILED);
                    }
                    permits.release();
                    schedulerLock.notifyAll();
                }
            }
        }

        @Override
        public void postExecute(Task task) {
            synchronized (schedulerLock) {
                if (task.taskType == TaskType.ONETIME) {
                    task.status.set(TaskStatus.COMPLETED);
                } else {
                    task.attemptsMade = 0;
                    task.nextRun = Instant.now().getEpochSecond()
                            + task.interval;
                    task.status.set(TaskStatus.SCHEDULED);
                    futureQueue.offer(task);
                }

                schedulerLock.notifyAll();
            }
        }

        private void onFailure(Task task, Throwable error) {
            System.err.println(
                    "Task " + task.id + " failed: " + error
            );

            synchronized (schedulerLock) {
                if (task.attemptsMade < task.maxAttempts) {
                    task.nextRun = Instant.now().getEpochSecond()
                            + task.retryDelaySeconds;
                    task.status.set(TaskStatus.SCHEDULED);
                    futureQueue.offer(task);
                } else {
                    task.status.set(TaskStatus.FAILED);
                }

                schedulerLock.notifyAll();
            }
        }

        public boolean cancel(long taskId) {
            Task task = taskMap.get(taskId);

            if (task == null) {
                return false;
            }

            synchronized (schedulerLock) {
                boolean cancelled = task.cancel();

                if (cancelled) {
                    schedulerLock.notifyAll();
                }

                return cancelled;
            }
        }

        public TaskStatus getStatus(long taskId) {
            Task task = taskMap.get(taskId);
            return task == null ? null : task.status.get();
        }

        private void signalScheduler() {
            synchronized (schedulerLock) {
                schedulerLock.notifyAll();
            }
        }

        @Override
        public void close() {
            synchronized (schedulerLock) {
                running = false;
                schedulerLock.notifyAll();
            }

            schedulerThread.interrupt();
            executorService.shutdown();
        }
    }

    public static void main(String[] args) throws Exception {

        try (TaskSchedulerService scheduler =
                     new TaskSchedulerService(2)) {

            long now = Instant.now().getEpochSecond();

            long t1 = scheduler.schedule(
                    TaskType.ONETIME, now + 1, 0,
                    TaskPriority.MEDIUM,
                    () -> System.out.println("T1 executed"),
                    3, 2
            );

            long t2 = scheduler.schedule(
                    TaskType.ONETIME, now + 1, 0,
                    TaskPriority.HIGH,
                    () -> System.out.println("T2 executed"),
                    3, 2
            );

            long t3 = scheduler.schedule(
                    TaskType.RECURRING, now + 2, 3,
                    TaskPriority.LOW,
                    () -> System.out.println("T3 recurring"),
                    2, 1
            );

            Thread.sleep(8000);

            scheduler.cancel(t3);

            System.out.println("T1: " + scheduler.getStatus(t1));
            System.out.println("T2: " + scheduler.getStatus(t2));
            System.out.println("T3: " + scheduler.getStatus(t3));
        }
    }
}
