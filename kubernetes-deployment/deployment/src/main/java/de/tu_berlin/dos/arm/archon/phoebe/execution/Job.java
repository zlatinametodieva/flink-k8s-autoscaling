package de.tu_berlin.dos.arm.archon.phoebe.execution;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Job {

    public static String jarId;

    public final String jobName;
    public final String brokerList;
    public final String consumerTopic;
    public final String producerTopic;
    public final String checkpointInterval;
    public final String windowType;
    public final String windowSize;
    public final String applyJoin;
    public final String applyAggregate;
    public final String entryClass;
    public final List<Long> tsList = new ArrayList<>();
    private int scaleOut;
    private String jobId;
    private String sinkId;
    private boolean isFinished = false;

    public Job(String jobName, String brokerList, String consumerTopic, String producerTopic, String checkpointInterval,
               String windowType, String windowSize, String applyJoin, String applyAggregate, String entryClass) {

        this.jobName = jobName;
        this.brokerList = brokerList;
        this.consumerTopic = consumerTopic;
        this.producerTopic = producerTopic;
        this.checkpointInterval = checkpointInterval;
        this.windowType = windowType;
        this.windowSize = windowSize;
        this.applyJoin = applyJoin;
        this.applyAggregate = applyAggregate;
        this.entryClass = entryClass;
    }

    public int getScaleOut() {

        return scaleOut;
    }

    public Job setScaleOut(int scaleOut) {

        this.scaleOut = scaleOut;
        return this;
    }

    public Job setJobId(String jobId) {

        this.jobId = jobId;
        return this;
    }

    public String getJobId() {

        return this.jobId;
    }

    public Job setSinkId(String sinkId) {

        this.sinkId = sinkId;
        return this;
    }

    public String getSinkId() {

        return sinkId;
    }

    public boolean isFinished() {

        return isFinished;
    }

    public Job setFinished(boolean isFinished) {

        this.isFinished = isFinished;
        return this;
    }

    public Job addTs(long timestamp) {

        this.tsList.add(timestamp);
        return this;
    }

    public List<Long> getTs() {

        return this.tsList;
    }

    public long getFirstTs() {

        if (this.tsList.size() > 0) return this.tsList.get(0);
        else throw new IllegalStateException("Timestamps is empty for job: " + this);
    }

    public long getLastTs() {

        if (this.tsList.size() > 0) return this.tsList.get(this.tsList.size() - 1);
        else throw new IllegalStateException("Timestamps is empty for job: " + this);
    }

    public JsonObject getProgramArgsList() {

        JsonObject body = new JsonObject();
        JsonArray args = new JsonArray();
        args.add(this.jobName);
        args.add(this.brokerList);
        args.add(this.consumerTopic);
        args.add(this.producerTopic);
        args.add(this.checkpointInterval);
        args.add(this.windowType);
        args.add(this.windowSize);
        args.add(this.applyJoin);
        args.add(this.applyAggregate);
        body.add("programArgsList", args);
        // body.addProperty("parallelism", this.scaleOut);
        body.addProperty("entryClass", this.entryClass);
        return body;
    }

    @Override
    public String toString() {
        return "{jobId: " + jobId +
                ", isFinished: " + isFinished +
                ", tsList: [" + this.tsList.stream().map(String::valueOf).collect(Collectors.joining(",")) + "]" +
                '}';
    }
}
