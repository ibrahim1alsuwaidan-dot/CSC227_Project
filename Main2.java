import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class Main {

    // ─────────────────────────────────────────────
    //  PCB: Process Control Block
    // ─────────────────────────────────────────────
    static class PCB {
        final int  processId;
        String     state;
        final int  burstTime;
        int        remainingTime;
        int        priority;
        final int  memoryRequired;
        final int  arrivalOrder;     // used to break ties (FCFS order)

        // Timing metrics
        int startTime        = -1;
        int terminationTime  = -1;
        int waitingTime      = 0;
        int turnaroundTime   = 0;

        // Aging / starvation fields (Priority only)
        int     waitingInReady  = 0;   // total ms spent waiting in ready queue
        int     agingCounter    = 0;   // counts ms since last priority boost
        boolean starved         = false;

        PCB(int id, int burst, int priority, int memory, int order) {
            this.processId      = id;
            this.burstTime      = burst;
            this.remainingTime  = burst;
            this.priority       = priority;
            this.memoryRequired = memory;
            this.arrivalOrder   = order;
            this.state          = "NEW";
        }
    }

    // ─────────────────────────────────────────────
    //  Gantt Chart Segment
    // ─────────────────────────────────────────────
    /**
     * Stores one execution slice for the Gantt chart.
     * startBurst / endBurst show how much of the burst was consumed
     * in this particular slice (useful for Round Robin).
     */
    static class GanttSegment {
        final int processId;
        final int startTime;
        final int endTime;
        final int startBurst;   // burst consumed before this slice
        final int endBurst;     // burst consumed after  this slice

        GanttSegment(int id, int start, int end, int sBurst, int eBurst) {
            this.processId  = id;
            this.startTime  = start;
            this.endTime    = end;
            this.startBurst = sBurst;
            this.endBurst   = eBurst;
        }
    }

    // ─────────────────────────────────────────────
    //  Shared Queues & Constants
    // ─────────────────────────────────────────────
    static final List<PCB> jobQueue       = Collections.synchronizedList(new LinkedList<>());
    static final List<PCB> readyQueue     = Collections.synchronizedList(new ArrayList<>());
    static final List<PCB> terminatedList = Collections.synchronizedList(new ArrayList<>());

    static final int           TOTAL_MEMORY   = 2048;
    static final AtomicInteger usedMemory     = new AtomicInteger(0);
    static final int           TIME_QUANTUM   = 5;
    static final int           AGING_INTERVAL = 4;   // boost priority every 4 ms

    static volatile boolean readingFinished = false;
    static final    Object  lock            = new Object();

    // ─────────────────────────────────────────────
    //  Main Entry Point
    // ─────────────────────────────────────────────
    public static void main(String[] args) {

        System.out.println("============================================================");
        System.out.println("       CSC 227 - Multithreaded CPU Scheduling Simulator     ");
        System.out.println("============================================================");

        // Start background threads
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.execute(new JobReader());
        executor.execute(new JobLoader());

        // Wait briefly so that the reader can populate the job queue
        // before we ask the user for their choice
        try { Thread.sleep(300); } catch (InterruptedException ignored) {}

        // Input validation loop
        Scanner sc     = new Scanner(System.in);
        int     choice = -1;

        while (true) {
            System.out.println("\nSelect a scheduling algorithm:");
            System.out.println("  1) Shortest Job First (SJF)");
            System.out.println("  2) Round Robin (RR, q = 5 ms)");
            System.out.println("  3) Priority Scheduling (Non-Preemptive, with Aging)");
            System.out.println("  0) Exit");
            System.out.print("Your choice: ");

            try {
                choice = Integer.parseInt(sc.next().trim());
                if (choice == 0) {
                    System.out.println("Goodbye!");
                    executor.shutdownNow();
                    System.exit(0);
                }
                if (choice >= 1 && choice <= 3) break;
                System.out.println(">> Please enter a number between 0 and 3.");
            } catch (NumberFormatException e) {
                System.out.println(">> Invalid input — please enter a number.");
            }
        }

        // Wait until both threads finish loading everything into the ready queue
        waitForLoad();

        // Run the chosen algorithm
        if      (choice == 1) runSJF();
        else if (choice == 2) runRR();
        else                  runPriority();

        executor.shutdownNow();
        System.out.println("\nSimulation finished.");
    }

    // ─────────────────────────────────────────────
    //  Thread 1 – Job Reader
    //  Reads job.txt, creates PCBs, adds to jobQueue
    // ─────────────────────────────────────────────
    static class JobReader implements Runnable {
        @Override
        public void run() {
            System.out.println("[Thread-1] Starting: reading job.txt ...");
            try (Scanner scanner = new Scanner(new File("job.txt"))) {
                int order = 0;
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine().trim();
                    if (line.isEmpty()) continue;
                    try {
                        // Expected format:  ID:burst:priority;memory
                        String[] parts  = line.split(";");
                        String[] pParts = parts[0].split(":");
                        int id       = Integer.parseInt(pParts[0].trim());
                        int burst    = Integer.parseInt(pParts[1].trim());
                        int priority = Integer.parseInt(pParts[2].trim());
                        int memory   = Integer.parseInt(parts[1].trim());

                        PCB pcb = new PCB(id, burst, priority, memory, order++);
                        synchronized (lock) {
                            jobQueue.add(pcb);
                            lock.notifyAll();
                        }
                        System.out.printf("[Thread-1] Loaded P%d  (burst=%d, priority=%d, memory=%dMB)%n",
                                id, burst, priority, memory);
                    } catch (Exception e) {
                        System.err.println("[Thread-1] Skipping malformed line: " + line);
                    }
                }
            } catch (FileNotFoundException e) {
                System.err.println("[Thread-1] ERROR: job.txt not found!");
            } finally {
                readingFinished = true;
                synchronized (lock) { lock.notifyAll(); }
                System.out.println("[Thread-1] Done reading. Thread terminating.");
            }
        }
    }

    // ─────────────────────────────────────────────
    //  Thread 2 – Job Loader
    //  Moves jobs from jobQueue → readyQueue if memory allows
    // ─────────────────────────────────────────────
    static class JobLoader implements Runnable {
        @Override
        public void run() {
            System.out.println("[Thread-2] Starting: loading jobs into ready queue ...");
            while (true) {
                synchronized (lock) {
                    // Wait if nothing to do yet
                    while (jobQueue.isEmpty() && !readingFinished) {
                        try { lock.wait(100); } catch (InterruptedException e) { return; }
                    }
                    // Exit condition: nothing left anywhere
                    if (jobQueue.isEmpty() && readingFinished) break;

                    // Try to load the next job if memory is available
                    if (!jobQueue.isEmpty()) {
                        PCB next = jobQueue.get(0);
                        if (usedMemory.get() + next.memoryRequired <= TOTAL_MEMORY) {
                            jobQueue.remove(0);
                            usedMemory.addAndGet(next.memoryRequired);
                            next.state = "READY";
                            readyQueue.add(next);
                            System.out.printf("[Thread-2] P%d admitted to ready queue  (memory used: %d/%d MB)%n",
                                    next.processId, usedMemory.get(), TOTAL_MEMORY);
                            lock.notifyAll();
                        }
                        // If not enough memory yet, wait for some process to finish
                        else {
                            try { lock.wait(50); } catch (InterruptedException e) { return; }
                        }
                    }
                }
            }
            System.out.println("[Thread-2] All jobs admitted. Thread terminating.");
        }
    }

    // ─────────────────────────────────────────────
    //  Helper: block until everything is in ready queue
    // ─────────────────────────────────────────────
    static void waitForLoad() {
        synchronized (lock) {
            while (!readingFinished || !jobQueue.isEmpty()) {
                try { lock.wait(100); } catch (InterruptedException ignored) {}
            }
        }
    }

    // ─────────────────────────────────────────────
    //  ALGORITHM 1 – Shortest Job First (Non-Preemptive)
    // ─────────────────────────────────────────────
    static void runSJF() {
        System.out.println("\n[Scheduler] Running: Shortest Job First (SJF)");
        int              currentTime = 0;
        List<GanttSegment> gantt     = new ArrayList<>();

        while (true) {
            if (readyQueue.isEmpty()) {
                if (readingFinished && jobQueue.isEmpty()) break;
                currentTime++;   // CPU idle
                continue;
            }

            // Sort by burst time; tie-break by arrival order
            readyQueue.sort(Comparator
                    .comparingInt((PCB p) -> p.burstTime)
                    .thenComparingInt(p -> p.arrivalOrder));

            PCB process = readyQueue.remove(0);
            process.state = "RUNNING";
            if (process.startTime == -1) process.startTime = currentTime;

            int execStart = currentTime;
            currentTime  += process.burstTime;   // run to completion (non-preemptive)

            gantt.add(new GanttSegment(process.processId, execStart, currentTime, 0, process.burstTime));

            process.terminationTime = currentTime;
            process.turnaroundTime  = process.terminationTime;   // arrival = 0
            process.waitingTime     = process.turnaroundTime - process.burstTime;
            process.state           = "TERMINATED";

            usedMemory.addAndGet(-process.memoryRequired);
            terminatedList.add(process);

            // Notify loader that memory was freed
            synchronized (lock) { lock.notifyAll(); }
        }

        printOutput(gantt, false);
    }

    // ─────────────────────────────────────────────
    //  ALGORITHM 2 – Round Robin (q = 5 ms)
    // ─────────────────────────────────────────────
    static void runRR() {
        System.out.println("\n[Scheduler] Running: Round Robin (q = 5 ms)");
        int              currentTime = 0;
        List<GanttSegment> gantt     = new ArrayList<>();

        // Use a proper FIFO queue for RR
        Queue<PCB> rrQueue = new LinkedList<>(readyQueue);
        readyQueue.clear();

        while (true) {
            if (rrQueue.isEmpty()) {
                if (readingFinished && jobQueue.isEmpty()) break;
                currentTime++;
                // Check if any new process was admitted while CPU was idle
                synchronized (lock) {
                    if (!readyQueue.isEmpty()) {
                        rrQueue.addAll(readyQueue);
                        readyQueue.clear();
                    }
                }
                continue;
            }

            PCB process = rrQueue.poll();
            process.state = "RUNNING";
            if (process.startTime == -1) process.startTime = currentTime;

            int slice      = Math.min(process.remainingTime, TIME_QUANTUM);
            int execStart  = currentTime;
            int burstBefore = process.burstTime - process.remainingTime;

            currentTime           += slice;
            process.remainingTime -= slice;

            gantt.add(new GanttSegment(process.processId, execStart, currentTime,
                    burstBefore, burstBefore + slice));

            if (process.remainingTime > 0) {
                // Not finished — re-queue
                process.state = "READY";
                rrQueue.add(process);
            } else {
                // Finished
                process.terminationTime = currentTime;
                process.turnaroundTime  = process.terminationTime;
                process.waitingTime     = process.turnaroundTime - process.burstTime;
                process.state           = "TERMINATED";

                usedMemory.addAndGet(-process.memoryRequired);
                terminatedList.add(process);
                synchronized (lock) { lock.notifyAll(); }
            }

            // Pick up any newly admitted processes
            synchronized (lock) {
                if (!readyQueue.isEmpty()) {
                    rrQueue.addAll(readyQueue);
                    readyQueue.clear();
                }
            }
        }

        printOutput(gantt, false);
    }

    // ─────────────────────────────────────────────
    //  ALGORITHM 3 – Priority Scheduling (Non-Preemptive) + Aging
    // ─────────────────────────────────────────────
    static void runPriority() {
        System.out.println("\n[Scheduler] Running: Priority Scheduling (Non-Preemptive, with Aging)");
        int              currentTime = 0;
        List<GanttSegment> gantt     = new ArrayList<>();

        while (true) {
            if (readyQueue.isEmpty()) {
                if (readingFinished && jobQueue.isEmpty()) break;
                currentTime++;
                applyAging(currentTime);
                continue;
            }

            // Sort by priority (lowest number = highest priority); tie-break by arrival order
            readyQueue.sort(Comparator
                    .comparingInt((PCB p) -> p.priority)
                    .thenComparingInt(p -> p.arrivalOrder));

            PCB process = readyQueue.remove(0);
            process.state = "RUNNING";
            if (process.startTime == -1) process.startTime = currentTime;

            int execStart = currentTime;

            // Run one ms at a time so aging applies continuously (non-preemptive but time-aware)
            for (int i = 0; i < process.burstTime; i++) {
                currentTime++;
                applyAging(currentTime);
            }

            gantt.add(new GanttSegment(process.processId, execStart, currentTime, 0, process.burstTime));

            process.terminationTime = currentTime;
            process.turnaroundTime  = process.terminationTime;
            process.waitingTime     = process.turnaroundTime - process.burstTime;
            process.state           = "TERMINATED";

            usedMemory.addAndGet(-process.memoryRequired);
            terminatedList.add(process);
            synchronized (lock) { lock.notifyAll(); }
        }

        printOutput(gantt, true);
    }

    // ─────────────────────────────────────────────
    //  Aging Logic (called every ms during Priority scheduling)
    // ─────────────────────────────────────────────
    /**
     * For each process waiting in the ready queue:
     *   - Increment its waiting counter.
     *   - If it has waited more than (N * 5) ms, flag it as starved.
     *   - Every AGING_INTERVAL ms, boost its priority by 1 (decrement number).
     */
    static void applyAging(int currentTime) {
        synchronized (lock) {
            int n = readyQueue.size();
            for (PCB p : readyQueue) {
                p.waitingInReady++;
                p.agingCounter++;

                // Starvation check: waited more than N*5 ms
                if (!p.starved && n > 0 && p.waitingInReady > (n * 5)) {
                    p.starved = true;
                    System.out.printf("  >> Starvation detected: P%d has waited %d ms at time %d ms%n",
                            p.processId, p.waitingInReady, currentTime);
                }

                // Aging: boost priority every AGING_INTERVAL ms
                if (p.agingCounter >= AGING_INTERVAL && p.priority > 1) {
                    p.priority--;
                    p.agingCounter = 0;
                    System.out.printf("  >> Aging applied: P%d priority boosted to %d at time %d ms%n",
                            p.processId, p.priority, currentTime);
                }
            }
        }
    }

    // ─────────────────────────────────────────────
    //  Output Printer
    // ─────────────────────────────────────────────
    /**
     * Prints:
     *   1. Gantt chart with timing and burst progress
     *   2. Process metrics table
     *   3. Average waiting time and average turnaround time
     *   4. (Priority only) List of starved processes
     */
    static void printOutput(List<GanttSegment> gantt, boolean showStarvation) {

        System.out.println("\n============================================================");
        System.out.println("                     SIMULATION RESULTS                    ");
        System.out.println("============================================================");

        // ── 1. Gantt Chart ──────────────────────────────────────────
        System.out.println("\n--- Gantt Chart ---");
        StringBuilder bar    = new StringBuilder();
        StringBuilder timing = new StringBuilder();

        // Top bar
        for (GanttSegment s : gantt) {
            String label = String.format(" P%d [%d-%d] ", s.processId, s.startBurst, s.endBurst);
            bar.append("|").append(label);
        }
        bar.append("|");

        // Time stamps below bar
        timing.append(String.format("%-3d", gantt.get(0).startTime));
        for (GanttSegment s : gantt) {
            int width = String.format(" P%d [%d-%d] ", s.processId, s.startBurst, s.endBurst).length() + 1;
            String t  = String.valueOf(s.endTime);
            // Right-align time at end of each block
            timing.append(String.format("%" + width + "s", t));
        }

        System.out.println(bar);
        System.out.println(timing);

        // ── 2. Process Metrics Table ─────────────────────────────────
        System.out.println("\n--- Process Metrics ---");
        System.out.printf("%-6s %-10s %-12s %-18s %-14s %-14s%n",
                "PID", "Burst(ms)", "Start(ms)", "Termination(ms)", "Waiting(ms)", "Turnaround(ms)");
        System.out.println("-".repeat(78));

        double totalWait = 0, totalTurnaround = 0;

        // Sort output by process ID for readability
        terminatedList.sort(Comparator.comparingInt(p -> p.processId));

        for (PCB p : terminatedList) {
            System.out.printf("%-6d %-10d %-12d %-18d %-14d %-14d%n",
                    p.processId, p.burstTime, p.startTime,
                    p.terminationTime, p.waitingTime, p.turnaroundTime);
            totalWait        += p.waitingTime;
            totalTurnaround  += p.turnaroundTime;
        }

        System.out.println("-".repeat(78));

        // ── 3. Averages ──────────────────────────────────────────────
        int count = terminatedList.size();
        System.out.printf("%nAverage Waiting Time    : %.2f ms%n", totalWait       / count);
        System.out.printf("Average Turnaround Time : %.2f ms%n",   totalTurnaround / count);

        // ── 4. Starvation Report (Priority only) ─────────────────────
        if (showStarvation) {
            System.out.println("\n--- Starvation Report ---");
            boolean any = false;
            for (PCB p : terminatedList) {
                if (p.starved) {
                    System.out.printf("  P%d experienced starvation (waited %d ms in ready queue)%n",
                            p.processId, p.waitingInReady);
                    any = true;
                }
            }
            if (!any) System.out.println("  No process experienced starvation.");
        }

        System.out.println("\n============================================================");
    }
