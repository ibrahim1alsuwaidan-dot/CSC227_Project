import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;

public class Main {

    static class PCB {
        int processId;
        String state;
        int burstTime;
        int remainingTime;
        int priority;
        int memoryRequired;

        int arrivalOrder;
        int startTime = -1;
        int terminationTime = -1;
        int waitingTime = 0;
        int turnaroundTime = 0;

        int waitingInReady = 0;
        int agingCounter = 0;
        boolean starved = false;

        PCB(int processId, int burstTime, int priority, int memoryRequired, int arrivalOrder) {
            this.processId = processId;
            this.burstTime = burstTime;
            this.remainingTime = burstTime;
            this.priority = priority;
            this.memoryRequired = memoryRequired;
            this.arrivalOrder = arrivalOrder;
            this.state = "NEW";
        }

        @Override
        public String toString() {
            return "P" + processId +
                    " [burst=" + burstTime +
                    ", priority=" + priority +
                    ", memory=" + memoryRequired +
                    "MB, state=" + state + "]";
        }
    }

    static class GanttSegment {
        int processId;
        int startTime;
        int endTime;
        int startBurst;
        int endBurst;

        GanttSegment(int processId, int startTime, int endTime, int startBurst, int endBurst) {
            this.processId = processId;
            this.startTime = startTime;
            this.endTime = endTime;
            this.startBurst = startBurst;
            this.endBurst = endBurst;
        }
    }

    static Queue<PCB> jobQueue = new LinkedList<>();
    static Queue<PCB> readyQueue = new LinkedList<>();

    static final int TOTAL_MEMORY = 2048;
    static int usedMemory = 0;

    static final int TIME_QUANTUM = 5;
    static final int AGING_INTERVAL = 4;

    // هذا القفل يحمي الوصول المشترك إلى jobQueue و readyQueue والذاكرة
    static final Object queueLock = new Object();

    // هذا المتغير يخبر Thread 2 أن Thread 1 انتهى من قراءة الملف
    static boolean readingFinished = false;

    static class JobReader implements Runnable {
        String fileName;

        JobReader(String fileName) {
            this.fileName = fileName;
        }

        @Override
        public void run() {
            readJobsFromFile(fileName);
        }
    }

    static class JobLoader implements Runnable {
        @Override
        public void run() {
            loadJobsToReadyQueue();
        }
    }

    static void readJobsFromFile(String fileName) {
        try {
            File file = new File(fileName);
            Scanner scanner = new Scanner(file);

            int arrivalOrder = 0;

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();

                if (line.isEmpty()) {
                    continue;
                }

                String[] mainParts = line.split(";");
                String[] processParts = mainParts[0].split(":");

                int processId = Integer.parseInt(processParts[0]);
                int burstTime = Integer.parseInt(processParts[1]);
                int priority = Integer.parseInt(processParts[2]);
                int memoryRequired = Integer.parseInt(mainParts[1]);

                PCB process = new PCB(processId, burstTime, priority, memoryRequired, arrivalOrder);

                synchronized (queueLock) {
                    jobQueue.add(process);
                    System.out.println("Thread 1 loaded job: P" + processId);
                    queueLock.notifyAll();
                }

                arrivalOrder++;
            }

            scanner.close();

            synchronized (queueLock) {
                readingFinished = true;
                System.out.println("Thread 1 finished reading job.txt.");
                queueLock.notifyAll();
            }

        } catch (FileNotFoundException e) {
            synchronized (queueLock) {
                readingFinished = true;
                queueLock.notifyAll();
            }
            System.out.println("Error: job.txt file not found.");
        }
    }

    static void loadJobsToReadyQueue() {
        while (true) {
            synchronized (queueLock) {

                while (jobQueue.isEmpty() && !readingFinished) {
                    try {
                        queueLock.wait();
                    } catch (InterruptedException e) {
                        System.out.println("Loader thread was interrupted while waiting.");
                        return;
                    }
                }

                if (jobQueue.isEmpty() && readingFinished) {
                    break;
                }

                PCB process = jobQueue.peek();

                if (usedMemory + process.memoryRequired <= TOTAL_MEMORY) {
                    jobQueue.poll();

                    process.state = "READY";
                    readyQueue.add(process);
                    usedMemory += process.memoryRequired;

                    System.out.println("Thread 2 loaded P" + process.processId +
                            " into Ready Queue. Used memory: " +
                            usedMemory + "/" + TOTAL_MEMORY + " MB");
                } else {
                    System.out.println("Thread 2: Not enough memory for P" + process.processId);
                    break;
                }
            }
        }

        System.out.println("Thread 2 finished loading jobs to Ready Queue.");
    }

    static void runSJF() {
        System.out.println();
        System.out.println("========== SJF Scheduling ==========");

        List<PCB> processes = new ArrayList<>(readyQueue);

        processes.sort(
                Comparator.comparingInt((PCB p) -> p.burstTime)
                        .thenComparingInt(p -> p.arrivalOrder)
        );

        runNonPreemptiveSchedule(processes);
    }

    static void runPriorityScheduling() {
        System.out.println();
        System.out.println("========== Priority Scheduling with Starvation Detection and Aging ==========");
        System.out.println("Note: Smaller priority number means higher priority.");
        System.out.println("Aging: every 4 ms, waiting process priority number decreases by 1.");

        List<PCB> processes = new ArrayList<>(readyQueue);
        List<PCB> notFinished = new ArrayList<>(readyQueue);
        List<GanttSegment> ganttChart = new ArrayList<>();

        int currentTime = 0;

        while (!notFinished.isEmpty()) {
            notFinished.sort(
                    Comparator.comparingInt((PCB p) -> p.priority)
                            .thenComparingInt(p -> p.arrivalOrder)
            );

            PCB runningProcess = notFinished.remove(0);
            runningProcess.state = "RUNNING";

            if (runningProcess.startTime == -1) {
                runningProcess.startTime = currentTime;
            }

            int segmentStartTime = currentTime;
            int startBurst = 0;

            for (int i = 0; i < runningProcess.burstTime; i++) {
                currentTime++;

                int numberOfWaitingProcesses = notFinished.size();

                for (PCB waitingProcess : notFinished) {
                    waitingProcess.waitingInReady++;
                    waitingProcess.agingCounter++;

                    int starvationLimit = numberOfWaitingProcesses * 5;

                    if (numberOfWaitingProcesses > 0 &&
                            waitingProcess.waitingInReady > starvationLimit &&
                            !waitingProcess.starved) {

                        waitingProcess.starved = true;
                        System.out.println("Starvation detected at time " + currentTime +
                                ": P" + waitingProcess.processId +
                                " waited " + waitingProcess.waitingInReady +
                                " ms in Ready Queue.");
                    }

                    if (waitingProcess.agingCounter == AGING_INTERVAL) {
                        if (waitingProcess.priority > 1) {
                            waitingProcess.priority--;

                            System.out.println("Aging applied at time " + currentTime +
                                    ": P" + waitingProcess.processId +
                                    " new priority = " + waitingProcess.priority);
                        }

                        waitingProcess.agingCounter = 0;
                    }
                }
            }

            int endBurst = runningProcess.burstTime;

            runningProcess.remainingTime = 0;
            runningProcess.terminationTime = currentTime;
            runningProcess.turnaroundTime = runningProcess.terminationTime;
            runningProcess.waitingTime = runningProcess.turnaroundTime - runningProcess.burstTime;
            runningProcess.state = "TERMINATED";

            ganttChart.add(new GanttSegment(
                    runningProcess.processId,
                    segmentStartTime,
                    currentTime,
                    startBurst,
                    endBurst
            ));
        }

        printGanttChart(ganttChart);
        printProcessTable(processes);
        printAverages(processes);
        printStarvedProcesses(processes);
    }

    static void runNonPreemptiveSchedule(List<PCB> processes) {
        List<GanttSegment> ganttChart = new ArrayList<>();

        int currentTime = 0;

        for (PCB process : processes) {
            process.state = "RUNNING";
            process.startTime = currentTime;

            int startBurst = 0;
            int endBurst = process.burstTime;

            currentTime += process.burstTime;

            process.terminationTime = currentTime;
            process.turnaroundTime = process.terminationTime;
            process.waitingTime = process.startTime;
            process.remainingTime = 0;
            process.state = "TERMINATED";

            ganttChart.add(new GanttSegment(
                    process.processId,
                    process.startTime,
                    process.terminationTime,
                    startBurst,
                    endBurst
            ));
        }

        printGanttChart(ganttChart);
        printProcessTable(processes);
        printAverages(processes);
    }

    static void runRoundRobin() {
        System.out.println();
        System.out.println("========== Round Robin Scheduling ==========");
        System.out.println("Time Quantum = " + TIME_QUANTUM + " ms");

        Queue<PCB> rrQueue = new LinkedList<>(readyQueue);
        List<PCB> processes = new ArrayList<>(readyQueue);
        List<GanttSegment> ganttChart = new ArrayList<>();

        int currentTime = 0;

        while (!rrQueue.isEmpty()) {
            PCB process = rrQueue.poll();

            process.state = "RUNNING";

            if (process.startTime == -1) {
                process.startTime = currentTime;
            }

            int startTime = currentTime;
            int startBurst = process.burstTime - process.remainingTime;

            int executionTime;

            if (process.remainingTime > TIME_QUANTUM) {
                executionTime = TIME_QUANTUM;
            } else {
                executionTime = process.remainingTime;
            }

            currentTime += executionTime;
            process.remainingTime -= executionTime;

            int endBurst = process.burstTime - process.remainingTime;

            ganttChart.add(new GanttSegment(
                    process.processId,
                    startTime,
                    currentTime,
                    startBurst,
                    endBurst
            ));

            if (process.remainingTime > 0) {
                process.state = "READY";
                rrQueue.add(process);
            } else {
                process.state = "TERMINATED";
                process.terminationTime = currentTime;
                process.turnaroundTime = process.terminationTime;
                process.waitingTime = process.turnaroundTime - process.burstTime;
            }
        }

        printGanttChart(ganttChart);
        printProcessTable(processes);
        printAverages(processes);
    }

    static void printGanttChart(List<GanttSegment> ganttChart) {
        System.out.println();
        System.out.println("Gantt Chart:");

        for (GanttSegment segment : ganttChart) {
            System.out.print("| P" + segment.processId + " ");
        }
        System.out.println("|");

        for (GanttSegment segment : ganttChart) {
            System.out.print(segment.startTime + "     ");
        }

        if (!ganttChart.isEmpty()) {
            System.out.println(ganttChart.get(ganttChart.size() - 1).endTime);
        }

        System.out.println();
        System.out.println("Detailed Gantt Chart:");
        for (GanttSegment segment : ganttChart) {
            System.out.println("P" + segment.processId +
                    " selected from time " + segment.startTime +
                    " to " + segment.endTime +
                    " | burst: " + segment.startBurst +
                    " -> " + segment.endBurst);
        }
    }

    static void printProcessTable(List<PCB> processes) {
        System.out.println();
        System.out.println("Process Table:");
        System.out.printf("%-10s %-10s %-10s %-12s %-18s %-14s %-16s%n",
                "Process", "Burst", "Priority", "Start", "Termination", "Waiting", "Turnaround");

        for (PCB process : processes) {
            System.out.printf("%-10s %-10d %-10d %-12d %-18d %-14d %-16d%n",
                    "P" + process.processId,
                    process.burstTime,
                    process.priority,
                    process.startTime,
                    process.terminationTime,
                    process.waitingTime,
                    process.turnaroundTime);
        }
    }

    static void printAverages(List<PCB> processes) {
        double totalWaiting = 0;
        double totalTurnaround = 0;

        for (PCB process : processes) {
            totalWaiting += process.waitingTime;
            totalTurnaround += process.turnaroundTime;
        }

        double averageWaiting = totalWaiting / processes.size();
        double averageTurnaround = totalTurnaround / processes.size();

        System.out.println();
        System.out.printf("Average Waiting Time: %.2f ms%n", averageWaiting);
        System.out.printf("Average Turnaround Time: %.2f ms%n", averageTurnaround);
    }

    static void printStarvedProcesses(List<PCB> processes) {
        System.out.println();
        System.out.println("Starved Processes:");

        boolean found = false;

        for (PCB process : processes) {
            if (process.starved) {
                System.out.println("P" + process.processId);
                found = true;
            }
        }

        if (!found) {
            System.out.println("No process suffered from starvation.");
        }
    }

    static void printProjectHeader() {
        System.out.println("==============================================");
        System.out.println("CSC227 Operating Systems Project");
        System.out.println("Multithreaded CPU Scheduling Simulator");
        System.out.println("==============================================");
    }

    public static void main(String[] args) {
        printProjectHeader();

        Thread readerThread = new Thread(new JobReader("job.txt"));
        Thread loaderThread = new Thread(new JobLoader());

        // هنا نشغل Thread 1 و Thread 2 معاً
        readerThread.start();
        loaderThread.start();

        try {
            readerThread.join();
            loaderThread.join();
        } catch (InterruptedException e) {
            System.out.println("A thread was interrupted.");
        }

        System.out.println();
        System.out.println("Processes in Ready Queue:");
        for (PCB process : readyQueue) {
            System.out.println(process);
        }

        System.out.println();
        System.out.println("Choose Scheduling Algorithm:");
        System.out.println("1. Shortest Job First (SJF)");
        System.out.println("2. Round Robin (RR)");
        System.out.println("3. Priority Scheduling");

        Scanner input = new Scanner(System.in);
        System.out.print("Enter your choice: ");
        int choice = input.nextInt();

        if (choice == 1) {
            runSJF();
        } else if (choice == 2) {
            runRoundRobin();
        } else if (choice == 3) {
            runPriorityScheduling();
        } else {
            System.out.println("Invalid choice.");
        }

        input.close();
    }
}