package de.tu_berlin.dos.arm.archon.common.clients.flink.responses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Uptime {

    public static class Timestamps {
        @SerializedName("RUNNING")
        @Expose
        public Long lastRestart;
    }

    @SerializedName("timestamps")
    @Expose
    public Timestamps timestamps;

}
