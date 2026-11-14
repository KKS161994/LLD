package JobDependencySystem;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Java 21+. In-memory, batch-submitted dependency-aware workflow executor. */
public class SolutionV2 {
    enum JobStatus { CREATED, SCHEDULED, PROCESSING, FAILED, COMPLETED, SKIPPED }
    enum WorkflowStatus { CREATED, PROCESSING, FAILED, COMPLETED }

    record Task(String taskName, Set<String> dependencies, Runnable action) {
        Task {
            Objects.requireNonNull(taskName);
            Objects.requireNonNull(dependencies);
            Objects.requireNonNull(action);
            dependencies = Set.copyOf(dependencies);
        }
    }

    static final class Job {
        final String name;
        final int workflowId;
        final Runnable action;
        final Set<String> dependents = new HashSet<>();
        int remainingDependencies;
        volatile JobStatus status = JobStatus.CREATED;

        Job(String name, int workflowId, Runnable action, int remainingDependencies) {
            this.name = name;
            this.workflowId = workflowId;
            this.action = action;
            this.remainingDependencies = remainingDependencies;
        }
    }

    static final class Workflow {
        final int id;
        final Map<String, Job> jobs;
        final CountDownLatch finished = new CountDownLatch(1);
        volatile WorkflowStatus status = WorkflowStatus.CREATED;
        int terminalJobs;
        boolean hasFailure;

        Workflow(int id, Map<String, Job> jobs) {
            this.id = id;
            this.jobs = jobs;
        }
    }

    static final class WorkflowScheduler implements AutoCloseable {
        private final AtomicInteger nextWorkflowId = new AtomicInteger();
        private final WorkflowExecutor executor;

        WorkflowScheduler(int capacity) {
            executor = new WorkflowExecutor(capacity);
        }

        int addWorkflow(Collection<Task> tasks) {
            Objects.requireNonNull(tasks);
            if (tasks.isEmpty()) throw new IllegalArgumentException("Workflow must contain jobs");
            // Build and validate locally: rejected workflows never enter the executor.
            Map<String, Task> definitions = new LinkedHashMap<>();
            for (Task task : tasks) {
                Objects.requireNonNull(task);
                if (task.taskName().isBlank() || definitions.putIfAbsent(task.taskName(), task) != null)
                    throw new IllegalArgumentException("Blank or duplicate job name: " + task.taskName());
            }
            for (Task task : definitions.values()) {
                for (String dependency : task.dependencies()) {
                    if (!definitions.containsKey(dependency))
                        throw new IllegalArgumentException("Unknown dependency " + dependency + " for " + task.taskName());
                }
            }

            int id = nextWorkflowId.incrementAndGet();
            Map<String, Job> jobs = new LinkedHashMap<>();
            for (Task task : definitions.values())
                jobs.put(task.taskName(), new Job(task.taskName(), id, task.action(), task.dependencies().size()));
            for (Task task : definitions.values())
                for (String dependency : task.dependencies())
                    jobs.get(dependency).dependents.add(task.taskName());

            // Kahn validation on a COPY; runtime dependency counters remain untouched.
            Map<String, Integer> counts = new HashMap<>();
            Queue<String> zero = new ArrayDeque<>();
            for (Job job : jobs.values()) {
                counts.put(job.name, job.remainingDependencies);
                if (job.remainingDependencies == 0) zero.add(job.name);
            }
            int visited = 0;
            while (!zero.isEmpty()) {
                String name = zero.remove();
                visited++;
                for (String dependent : jobs.get(name).dependents)
                    if (counts.merge(dependent, -1, Integer::sum) == 0) zero.add(dependent);
            }
            if (visited != jobs.size()) throw new IllegalArgumentException("Workflow contains a cycle");

            executor.submitWorkflow(new Workflow(id, jobs));
            return id;
        }

        WorkflowStatus getWorkflowStatus(int id) { return executor.getWorkflowStatus(id); }
        JobStatus getJobStatus(int id, String jobName) { return executor.getJobStatus(id, jobName); }
        boolean awaitCompletion(int id, long timeout, TimeUnit unit) throws InterruptedException {
            return executor.awaitCompletion(id, timeout, unit);
        }
        @Override public void close() { executor.close(); }
    }

    static final class WorkflowExecutor implements AutoCloseable {
        private final Object lock = new Object();
        private final Queue<Job> readyQueue = new ArrayDeque<>();
        private final Map<Integer, Workflow> workflows = new HashMap<>();
        private final Semaphore permits;
        private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
        private final Thread schedulerThread;
        private boolean running = true; // accessed under lock

        WorkflowExecutor(int capacity) {
            if (capacity <= 0) throw new IllegalArgumentException("Capacity must be positive");
            permits = new Semaphore(capacity);
            schedulerThread = Thread.ofPlatform().name("workflow-scheduler").start(this::schedulerLoop);
        }

        void submitWorkflow(Workflow workflow) {
            synchronized (lock) {
                if (!running) throw new IllegalStateException("Executor is closed");
                workflows.put(workflow.id, workflow);
                workflow.status = WorkflowStatus.PROCESSING;
                for (Job job : workflow.jobs.values()) {
                    if (job.remainingDependencies == 0) {
                        job.status = JobStatus.SCHEDULED;
                        readyQueue.add(job);
                    }
                }
                lock.notifyAll();
            }
        }

        private Workflow requireWorkflow(int id) {
            Workflow workflow = workflows.get(id); // caller holds lock
            if (workflow == null) throw new IllegalArgumentException("Unknown workflow: " + id);
            return workflow;
        }

        WorkflowStatus getWorkflowStatus(int id) {
            synchronized (lock) { return requireWorkflow(id).status; }
        }

        JobStatus getJobStatus(int id, String jobName) {
            synchronized (lock) {
                Job job = requireWorkflow(id).jobs.get(jobName);
                if (job == null) throw new IllegalArgumentException("Unknown job: " + jobName);
                return job.status;
            }
        }

        boolean awaitCompletion(int id, long timeout, TimeUnit unit) throws InterruptedException {
            Workflow workflow;
            synchronized (lock) { workflow = requireWorkflow(id); }
            return workflow.finished.await(timeout, unit);
        }

        private void schedulerLoop() {
            try {
                while (true) {
                    Job selected;
                    synchronized (lock) {
                        while (running && (readyQueue.isEmpty() || permits.availablePermits() == 0))
                            lock.wait();
                        if (!running) return;
                        // Only scheduler takes permits; workers release and notify under this lock.
                        if (!permits.tryAcquire()) continue;
                        selected = readyQueue.remove();
                        if (selected.status != JobStatus.SCHEDULED) {
                            permits.release();
                            continue;
                        }
                        selected.status = JobStatus.PROCESSING;
                    }
                    try {
                        Job job = selected;
                        workers.execute(() -> runJob(job));
                    } catch (RejectedExecutionException e) {
                        // Prevent a stranded PROCESSING job and leaked permit.
                        onJobFailure(selected, e);
                        synchronized (lock) {
                            permits.release();
                            lock.notifyAll();
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void runJob(Job job) {
            try {
                job.action.run();
                onJobSuccess(job);
            } catch (Throwable error) {
                onJobFailure(job, error);
            } finally {
                synchronized (lock) {
                    permits.release();
                    lock.notifyAll();
                }
            }
        }

        private void onJobSuccess(Job job) {
            synchronized (lock) {
                Workflow workflow = requireWorkflow(job.workflowId);
                job.status = JobStatus.COMPLETED;
                workflow.terminalJobs++;
                for (String name : job.dependents) {
                    Job dependent = workflow.jobs.get(name);
                    if (--dependent.remainingDependencies == 0 && dependent.status == JobStatus.CREATED) {
                        dependent.status = JobStatus.SCHEDULED;
                        readyQueue.add(dependent);
                    }
                }
                finishIfTerminal(workflow);
                lock.notifyAll();
            }
        }

        private void onJobFailure(Job job, Throwable error) {
            System.err.println("Job " + job.name + " failed: " + error);
            synchronized (lock) {
                Workflow workflow = requireWorkflow(job.workflowId);
                job.status = JobStatus.FAILED;
                workflow.terminalJobs++;
                workflow.hasFailure = true;

                // A dependent of a failed/skipped job can never execute. Independent branches continue.
                Queue<String> toSkip = new ArrayDeque<>(job.dependents);
                while (!toSkip.isEmpty()) {
                    Job dependent = workflow.jobs.get(toSkip.remove());
                    if (dependent.status != JobStatus.CREATED) continue;
                    dependent.status = JobStatus.SKIPPED;
                    workflow.terminalJobs++;
                    toSkip.addAll(dependent.dependents);
                }
                finishIfTerminal(workflow);
                lock.notifyAll();
            }
        }

        private void finishIfTerminal(Workflow workflow) {
            if (workflow.terminalJobs == workflow.jobs.size()) {
                workflow.status = workflow.hasFailure ? WorkflowStatus.FAILED : WorkflowStatus.COMPLETED;
                workflow.finished.countDown();
            }
        }

        @Override public void close() {
            synchronized (lock) {
                running = false;
                lock.notifyAll();
            }
            schedulerThread.interrupt();
            try {
                schedulerThread.join();
                // Graceful close: drain accepted work that is already executing.
                workers.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                workers.shutdown();
            }
            // Shutdown is a lifecycle operation, not workflow cancellation.
            // Do not call close() until submitted workflows have completed.
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    public static void main(String[] args) throws Exception {
        try (WorkflowScheduler scheduler = new WorkflowScheduler(4)) {
            List<String> order = Collections.synchronizedList(new ArrayList<>());
            int ok = scheduler.addWorkflow(List.of(
                new Task("A", Set.of(), () -> order.add("A")),
                new Task("B", Set.of(), () -> order.add("B")),
                new Task("C", Set.of("A", "B"), () -> {
                    check(order.containsAll(List.of("A", "B")), "C ran too early");
                    order.add("C");
                }),
                new Task("D", Set.of(), () -> order.add("D")),
                new Task("E", Set.of("C"), () -> order.add("E"))
            ));
            check(scheduler.awaitCompletion(ok, 5, TimeUnit.SECONDS), "Success workflow timed out");
            check(scheduler.getWorkflowStatus(ok) == WorkflowStatus.COMPLETED, "Expected success");
            check(order.indexOf("C") > order.indexOf("A") && order.indexOf("C") > order.indexOf("B"), "Bad order");

            int failed = scheduler.addWorkflow(List.of(
                new Task("A", Set.of(), () -> { throw new IllegalStateException("expected failure"); }),
                new Task("B", Set.of("A"), () -> { throw new AssertionError("B must not run"); }),
                new Task("C", Set.of("B"), () -> { throw new AssertionError("C must not run"); }),
                new Task("D", Set.of(), () -> {})
            ));
            check(scheduler.awaitCompletion(failed, 5, TimeUnit.SECONDS), "Failure workflow timed out");
            check(scheduler.getWorkflowStatus(failed) == WorkflowStatus.FAILED, "Expected failure");
            check(scheduler.getJobStatus(failed, "A") == JobStatus.FAILED, "A must fail");
            check(scheduler.getJobStatus(failed, "B") == JobStatus.SKIPPED, "B must skip");
            check(scheduler.getJobStatus(failed, "C") == JobStatus.SKIPPED, "C must skip");
            check(scheduler.getJobStatus(failed, "D") == JobStatus.COMPLETED, "D must complete");

            try {
                scheduler.addWorkflow(List.of(new Task("X", Set.of("missing"), () -> {})));
                throw new AssertionError("Missing dependency accepted");
            } catch (IllegalArgumentException expected) { /* correct */ }
            try {
                scheduler.addWorkflow(List.of(
                    new Task("X", Set.of("Y"), () -> {}),
                    new Task("Y", Set.of("X"), () -> {})
                ));
                throw new AssertionError("Cycle accepted");
            } catch (IllegalArgumentException expected) { /* correct */ }

            AtomicInteger active = new AtomicInteger();
            AtomicInteger peak = new AtomicInteger();
            List<Task> parallel = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                final int index = i;
                parallel.add(new Task("P" + index, Set.of(), () -> {
                    int n = active.incrementAndGet();
                    peak.accumulateAndGet(n, Math::max);
                    try { Thread.sleep(25); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    finally { active.decrementAndGet(); }
                }));
            }
            int p1 = scheduler.addWorkflow(parallel);
            int p2 = scheduler.addWorkflow(parallel);
            check(scheduler.awaitCompletion(p1, 5, TimeUnit.SECONDS), "Parallel workflow 1 timed out");
            check(scheduler.awaitCompletion(p2, 5, TimeUnit.SECONDS), "Parallel workflow 2 timed out");
            check(peak.get() <= 4, "Exceeded global concurrency: " + peak.get());
            System.out.println("ALL TESTS PASSED; peak concurrency=" + peak.get());
        }
    }
}
