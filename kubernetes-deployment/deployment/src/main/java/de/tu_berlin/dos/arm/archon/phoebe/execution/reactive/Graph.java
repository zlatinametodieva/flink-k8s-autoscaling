package de.tu_berlin.dos.arm.archon.phoebe.execution.reactive;

import de.tu_berlin.dos.arm.archon.common.data.TimeSeries;
import de.tu_berlin.dos.arm.archon.common.utils.SequenceFSM;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public enum Graph implements SequenceFSM<Context, Graph> {

    START {

        public Graph runStage(Context context) {

            return DEPLOY;
        }
    },
    DEPLOY {

        public Graph runStage(Context context) throws Exception {

            // re-initialize namespace
            context.clientsManager.createNamespace();
            // deploy to k8s using helm
            context.k8s.find("HELM").forEach(context.clientsManager::deployHelm);
            // deploy to k8s using yaml
            context.k8s.find("YAML").forEach(context.clientsManager::deployYaml);
            // waiting for load generators to come online
            new CountDownLatch(1).await(40, TimeUnit.SECONDS);

            return INITIALIZE;
        }
    },
    INITIALIZE {

        public Graph runStage(Context context) throws Exception {

            // get details of target job
            List<String> jobs = context.clientsManager.getJobs();
            if (jobs.size() != 1) throw new IllegalStateException("Invalid number of jobs: " + jobs.size());
            context.job.setJobId(jobs.get(0));
            context.job.addTs(context.clientsManager.getTs(context.job.getJobId(), "RUNNING"));
            LOG.info("Running job with configuration: " + context.job);

            return EXECUTE;
        }
    },
    EXECUTE {

        public Graph runStage(Context context) throws Exception {

            // wait for the length of the experiment runtime
            for (int i = 0; i < context.expLen; i++) {
                new CountDownLatch(1).await(1, TimeUnit.SECONDS);
            }

            LOG.info("Gathering metrics for visualization.");
            Map<String, TimeSeries> metrics = new HashMap<>();
            String dirPath = String.format("%s/%s", context.dataPath, context.job.getJobId());
            long jobStart = context.job.getFirstTs();
            long jobEnd = context.clientsManager.getLatestTs(context.job.getJobId());
            TimeSeries cpuValue = TimeSeries.create(jobStart, jobEnd);
            Map<String, TimeSeries> allCpu = context.clientsManager.getAllCpuLoad("pod", jobStart, jobEnd);
            allCpu.forEach((key, value) -> cpuValue.unalignedMerge(value));
            TimeSeries memValue = TimeSeries.create(jobStart, jobEnd);
            Map<String, TimeSeries> allMem = context.clientsManager.getAllMemUsed("pod", jobStart, jobEnd);
            allMem.forEach((key, value) -> memValue.unalignedMerge(value));
            metrics.put(String.format("%s/cpuLoad", dirPath), cpuValue);
            metrics.put(String.format("%s/memUsed", dirPath), memValue);
            metrics.put(String.format("%s/numWorkers", dirPath), context.clientsManager.getNumWorkers(jobStart, jobEnd));
            metrics.put(String.format("%s/numRestarts", dirPath), context.clientsManager.getNumRestarts(jobStart, jobEnd));
            metrics.put(String.format("%s/delay", dirPath), context.clientsManager.getDelay(jobStart, jobEnd));
            metrics.put(String.format("%s/stateUsage", dirPath), context.clientsManager.getStateMem(jobStart, jobEnd));

            // write metrics to file
            metrics.forEach((name, timeSeries) -> {

                try { TimeSeries.toCSV(name, timeSeries, "timestamp|value", "|"); }
                catch (IOException e) { e.printStackTrace(); }
            });

            LOG.info("Stopping job.");
            context.clientsManager.stopJob(context.job.getJobId());
            context.job.setFinished(true);

            String snapshot = context.clientsManager.prom.takeSnapshot();
            LOG.info("PROMETHEUS: created snapshot " + snapshot);
            context.clientsManager.k8sClient.copySnapshot(context.clientsManager.namespace);

            return STOP;
        }
    },
    STOP {

        public Graph runStage(Context context) throws Exception {

            context.close();
            context.executor.shutdownNow();
            return this;
        }
    };

    public static void start(String propsFile) throws Exception {

        Graph.START.run(Graph.class, Context.create(propsFile));
    }
}
