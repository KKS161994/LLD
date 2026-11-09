package TaskSchedulingSystem;

import java.lang.module.ModuleDescriptor.Builder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import TaskSchedulingSystem.Solution.Task;

public class Solution {
    static enum TaskType {
        ONETIME,
        RECURRING,
    }

    static enum TastSTATUS {
        NOT_STARTED,
        SCHEDULED,
        IN_PROGRESS,
        COMPLETED,
        FAILED,
        CANCELLED,
    }

    static enum TaskPriority {
        HIGH,
        MEDIUM,
        LOW,
    }

    /**
     * Listener
     */
    public interface Listener {
        public void postExecute(Task task);

    }

    // Should use builder ideally
    static class Task {
        long id;
        TaskType taskType;
        long startTime, nextRun;
        long interval = 0;
        int retryAttempt;
        AtomicReference<TastSTATUS> status;
        long endTime;
        TaskPriority taskPriority;
        Map<TaskPriority, Integer> priortyMap = new HashMap<>();
        Runnable task;

        static class Builder {
            Task task;

            Builder() {
                task = new Task();
            }

            public void setTaskType(TaskType taskType) {
                task.taskType = taskType;
            }

            public void setId(Long id) {
                task.id = id;
            }

            public void setStartTime(Long startTime) {
                task.startTime = startTime;
                task.nextRun = startTime;
            }

            public void setInterval(int interval) {
                task.interval = interval;
            }

            public void setPriority(TaskPriority taskPriority) {
                task.taskPriority = taskPriority;
            }

            public void setTask(Runnable runnable) {
                task.task = runnable;
            }

            Task build() {
                return task;
            }

        }

        void updateTaskInterval(int interval) {
            this.interval = interval;
        }

        boolean cancel() {
            TastSTATUS current = status.get();
            if (current == TastSTATUS.COMPLETED)
                return false;
            return status.compareAndSet(current, TastSTATUS.CANCELLED);
        }

        public void execute(Listener listener) {
            task.run();
            listener.postExecute(this);
        }

        public long getNextRun() {
            return nextRun;
        }

        public long getId() {
            return id;
        }

        public TaskPriority getPriority() {
            return taskPriority;
        }

        public void updateNextRun(long currentTime) {
            nextRun = currentTime + interval;
        }
    }

    static class TaskSchedulerService implements Listener {
        ConcurrentHashMap<Long, Task> taskMap = new ConcurrentHashMap<>();

        // PriorityQueue<Task> futureQueue = new PriorityQueue<>();
        Comparator<Task> futureComparator = Comparator.comparingLong(Task::getNextRun)
                .thenComparingLong(Task::getId);
        Comparator<Task> readyComparator = Comparator.comparingInt(
                (Task t) -> t.getPriority().ordinal())
                .thenComparingLong(Task::getNextRun)
                .thenComparingLong(Task::getId);
        PriorityQueue<Task> readyQueue = new PriorityQueue<>(readyComparator);
        PriorityQueue<Task> futureQueue = new PriorityQueue<>(futureComparator);
        AtomicLong jobIdGenerator = new AtomicLong(0);
        private final Semaphore permits;
        long currentSec = 1;
        int capacity = 0;
        ExecutorService executorService;

        TaskSchedulerService(int n) {
            capacity = n;
            permits = new Semaphore(n);

            executorService = Executors.newVirtualThreadPerTaskExecutor();

            startExecute();
        }

        public void startExecute() {

            // Move due tasks into readyQueue.
            // Assumption: only one scheduler thread accesses these queues.
            currentSec = Instant.now().getEpochSecond();

            while (!futureQueue.isEmpty()
                    && futureQueue.peek().getNextRun() <= currentSec) {

                readyQueue.offer(futureQueue.poll());
            }

            // Dispatch ready tasks, up to n concurrently.
            while (!readyQueue.isEmpty()) {

                if (!permits.tryAcquire()) {
                    break;
                }

                Task task = readyQueue.poll();

                if (task == null) {
                    permits.release();
                    break;
                }

                try {
                    executorService.submit(() -> {
                        task.status.set(TastSTATUS.IN_PROGRESS);
                        try {
                            task.execute(this);

                        } catch (Exception e) {
                            System.err.println(
                                    "Task execution failed: " + e.getMessage());

                        } finally {
                            permits.release();
                        }
                    });
                } catch (RejectedExecutionException e) {
                    readyQueue.add(task);
                    permits.release();
                    throw e;
                }
            }
        }

        public boolean addTaskToScheduler(Task task) {
            boolean isTaskAlreadyPresent = taskMap.putIfAbsent(task.id, task) != null ? true : false;
            if (isTaskAlreadyPresent)
                return false;
            futureQueue.add(task);
            return true;
        }

        public boolean updateTask(long id, int interval) {

            Task task = taskMap.get(id);
            if (task == null)
                return false;
            task.interval = interval;
            return true;
        }

        public boolean removeTask(long id) {
            Task task = taskMap.get(id);
            if ((task == null) || (task.status.get() == TastSTATUS.IN_PROGRESS))
                return false;
            task.status.set(TastSTATUS.CANCELLED);
            return true;
        }

        public TastSTATUS geTastSTATUS(long id) {
            Task task = taskMap.get(id);
            if (task == null)
                return null;
            return task.status.get();
        }

        @Override
        public void postExecute(Task task) {
            if (task.taskType.equals(TaskType.ONETIME)) {
                task.status.set(TastSTATUS.COMPLETED);
                return;
            }
            task.status.set(TastSTATUS.SCHEDULED);
            currentSec = Instant.now().getEpochSecond();
            task.updateNextRun(currentSec);
            futureQueue.add(task);
        }
    }

}

// public void startExecute() {
// executorService.submit(() -> {
// try {
// permits.acquire();
// } catch (InterruptedException e) {
// Thread.currentThread().interrupt();
// return;
// }

// while (!futureQueue.isEmpty()
// && futureQueue.peek().getNextRun() <= currentSec) {

// readyQueue.offer(futureQueue.poll());
// }
// Task task = readyQueue.poll();
// try {
// currentSec++;
// task.execute(null);
// } finally {
// permits.release();
// }

// });

// }