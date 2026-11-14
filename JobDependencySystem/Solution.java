package JobDependencySystem;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Solution {
    enum JobStatus {
        Created,
        Scheduled,
        Processing,
        Failed,
        Completed,
        Skipped
    }

    enum WorkFlowStatus {
        Created,
        Processing,
        Failed,
        Completed,
        Skipped
    }

    static class Task {
        String taskName;
        Set<String> tasks;
        Runnable task;

    }

    static interface CallListener {
        void postExecute(String job, int workfLowId);
    }

    static class Job {
        int id, workflowId;
        String jobName;

        private Set<String> dependencies;
        private Set<String> dependenciesFor;
        Runnable task;
        AtomicReference<JobStatus> status;
        int indegree = 0;

        Job() {
            this.status = new AtomicReference<>();
            this.status.set(JobStatus.Created);
            this.dependencies = new HashSet<>();
            this.dependenciesFor = new HashSet<>();
        }

        boolean claimForScheduling() {
            return status.compareAndSet(JobStatus.Created, JobStatus.Scheduled);
        }

        boolean claim() {
            return status.compareAndSet(JobStatus.Scheduled, JobStatus.Processing);
        }

        void execute(CallListener listener) {
            task.run();
            listener.postExecute(jobName, workflowId);
        }

        public void setDependencies(Set<String> dependencies) {
            this.dependencies = dependencies;
            indegree = dependencies.size();
        }

        public void reduceIndegree() {
            indegree -= 1;
        }

        public boolean isIndegree(int n) {
            return this.indegree == n;
        }

        public void addDependencyFor(String depedency) {
            dependenciesFor.add(depedency);
        }

        public Set<String> getDepsFor() {
            return dependenciesFor;
        }
    }

    static class Workflow {
        int id;
        Set<Job> jobs;
        AtomicReference<WorkFlowStatus> status;
        HashMap<String, Job> taskToJob;
        Map<String, Set<String>> taskToDependecies;
        int flowsCount = 0;

        Workflow(int id, HashMap<String, Job> taskToJob, Map<String, Set<String>> taskToDependecies, Set<Job> jobs) {
            this.id = id;
            this.taskToJob = taskToJob;
            this.taskToDependecies = taskToDependecies;
            status = new AtomicReference<>();
            status.set(WorkFlowStatus.Created);
            this.flowsCount = this.taskToDependecies.size();
            this.jobs = jobs;
        }

        boolean validateForMoreTasks() {
            for (Job job : taskToJob.values()) {
                if (!job.status.compareAndSet(JobStatus.Completed, JobStatus.Completed)
                        || !job.status.compareAndSet(JobStatus.Failed, JobStatus.Failed)
                        || !job.status.compareAndSet(JobStatus.Skipped, JobStatus.Skipped))
                    return false;

            }
            return false;
        }

    }

    static class WorkFlowScheduler {
        WorkFlowExecutor executor;
        AtomicInteger workflowId = new AtomicInteger();
        AtomicInteger jobId = new AtomicInteger();

        HashMap<String, Job> taskToJob;
        HashMap<String, Set<String>> taskToDependecies;

        WorkFlowScheduler(int capactity) {
            executor = WorkFlowExecutor.newInstance(capactity);
            workflowId.set(0);
            jobId.set(0);
        }

        boolean cancelWorkFlow(int workflowId) {
            return executor.cancelWorkflow(workflowId);
        }

        int addWorkFlow(Set<Task> tasks) {
            int workFlowId = workflowId.incrementAndGet();
            Map<String, Set<String>> jobDependencies = new HashMap<>();
            HashMap<String, Job> taskToJob = new HashMap<>();
            Set<Job> jobs = new HashSet<>();
            for (Task task : tasks) {
                Job job = new Job();
                jobDependencies.put(task.taskName, new HashSet<>());
                taskToJob.put(task.taskName, job);
                jobs.add(job);

            }

            for (Task task : tasks) {
                Set<String> dependecies = task.tasks;
                Job job = taskToJob.get(task.taskName);
                for (String dep : dependecies) {
                    // taskToJob.putIfAbsent(dep, new Job());
                    taskToJob.get(dep).addDependencyFor(task.taskName);
                }
                jobDependencies.put(task.taskName, dependecies);
                job.setDependencies(dependecies);
                job.id = jobId.incrementAndGet();
                job.jobName = task.taskName;
                job.task = task.task;
                job.workflowId = workFlowId;
            }

            Workflow workflow = new Workflow(workFlowId, taskToJob, jobDependencies, jobs);
            executor.submitWorkFlow(workflow);
            return workFlowId;
        }

    }

    static class WorkFlowExecutor implements AutoCloseable, CallListener {
        private static WorkFlowExecutor INSTANCE;
        private int capacity;
        private Semaphore semaphore;
        private ExecutorService executorService;
        private final Thread schedulerThread;
        private Queue<Job> queue;
        private Object lock;
        private boolean isRunning = true;
        private Map<Integer, Workflow> workFlowMap = new HashMap<>();

        private WorkFlowExecutor(int capacity) {
            this.capacity = capacity;
            semaphore = new Semaphore(this.capacity);
            queue = new ArrayDeque<>();
            lock = new Object();
            executorService = Executors.newVirtualThreadPerTaskExecutor();
            schedulerThread = Thread.ofPlatform().name("dependency-job-scheduler-thread").start(this::startWork);
        }

        public boolean cancelWorkflow(int workflowId) {
            Workflow workflow = workFlowMap.get(workflowId);
            if (!(workflow.status.get() == WorkFlowStatus.Completed)) {
                synchronized (lock) {
                    workflow.status.set(WorkFlowStatus.Skipped);
                    updateStatusesForJobs(workflow.jobs, JobStatus.Skipped);
                }
                return true;
            }
            return false;
        }

        public String getStatus(int workFlowId, String taskName) {
            Workflow workflow = workFlowMap.get(workFlowId);
            Job job = workflow.taskToJob.get(taskName);
            return "Workflow with ID : " + workFlowId + " current status : " + workflow.status.get().toString() +
                    " Job : " + job.jobName + " Current status : " + job.status.get().toString();
        }

        public static WorkFlowExecutor newInstance(int n) {
            if (INSTANCE == null) {
                INSTANCE = new WorkFlowExecutor(n);
            }
            return INSTANCE;
        }

        public boolean submitWorkFlow(Workflow workflow) {
            List<Job> readyJobsForQueue = new ArrayList<>();
            for (Job job : workflow.taskToJob.values()) {
                if (job.dependencies.size() == 0) {
                    workflow.status.compareAndSet(WorkFlowStatus.Created, WorkFlowStatus.Processing);
                    job.status.compareAndSet(JobStatus.Created, JobStatus.Scheduled);
                    readyJobsForQueue.add(job);
                }
            }
            if (workflow.status.get().equals(WorkFlowStatus.Created)) {
                updateStatusesForJobs(workflow.jobs, JobStatus.Skipped);
                workflow.status.set(WorkFlowStatus.Skipped);
                return false;
            }
            synchronized (lock) {
                workFlowMap.put(workflow.id, workflow);
                queue.addAll(readyJobsForQueue);
            }
            return true;
        }

        public void updateStatusesForJobs(Set<Job> workflowJobs, JobStatus status) {
            JobStatus tempStatus;
            for (Job job : workflowJobs) {
                tempStatus = job.status.get();
                if (tempStatus == JobStatus.Completed || tempStatus == JobStatus.Failed
                        || tempStatus == JobStatus.Skipped)
                    continue;
                job.status.set(status);
            }
        }

        public void startWork() {
            while (true) {
                Job selectedJob = null;
                synchronized (lock) {

                    try {
                        while (queue.peek().status.get() != JobStatus.Scheduled) {
                            queue.poll();
                        }
                        while (isRunning &&
                                (queue.isEmpty() || semaphore.availablePermits() == 0)) {
                            lock.wait();
                        }
                        if (!isRunning)
                            return;

                        while (true) {

                            if (queue.isEmpty()) {

                                break;
                            }

                            if (!queue.isEmpty() && semaphore.tryAcquire()) {

                                selectedJob = queue.poll();
                                Workflow workflow = workFlowMap.get(selectedJob.workflowId);
                                if (workflow.status.get() == WorkFlowStatus.Skipped) {
                                    selectedJob.status.set(JobStatus.Skipped);
                                    semaphore.release();
                                }
                                if (selectedJob.claim()) {
                                    break;
                                }
                                semaphore.release();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                dispatch(selectedJob);
            }
        }

        private void dispatch(Job job) {
            executorService.submit(() -> {
                try {
                    job.execute(this);
                } catch (Throwable error) {
                    onFailure(error, job);
                } finally {
                    semaphore.release();
                    signalScheduler();
                }
            });

        }

        private void onFailure(Throwable error, Job job) {
            System.err.println(
                    "Job " + job.jobName + " failed: " + error);
            synchronized (lock) {
                Workflow workflow = workFlowMap.get(job.workflowId);
                workflow.status.set(WorkFlowStatus.Failed);
                updateStatusesForJobs(workflow.jobs, JobStatus.Failed);
                job.status.set(JobStatus.Failed);
                workFlowMap.get(job.workflowId).status.set(null);
            }
        }

        private void signalScheduler() {
            synchronized (lock) {
                lock.notifyAll();
            }
        }

        @Override
        public void close() throws Exception {
            synchronized (lock) {
                isRunning = false;
                lock.notifyAll();
            }
            schedulerThread.interrupt();
            executorService.close();
        }

        @Override
        public void postExecute(String jobId, int workFlowId) {
            synchronized (lock) {
                Workflow workflow = workFlowMap.get(workFlowId);
                Job job = workflow.taskToJob.get(jobId);
                job.status.set(JobStatus.Completed);
                for (String depFor : job.getDepsFor()) {
                    Job depForJob = workflow.taskToJob.get(depFor);
                    depForJob.reduceIndegree();
                    if (depForJob.isIndegree(0)) {
                        if (depForJob.claimForScheduling()) {
                            queue.add(depForJob);
                        }

                    }
                }
                workflow.flowsCount -= 1;
                if (workflow.flowsCount == 0) {
                    workflow.status.set(WorkFlowStatus.Completed);
                }
            }
        }
    }

}
