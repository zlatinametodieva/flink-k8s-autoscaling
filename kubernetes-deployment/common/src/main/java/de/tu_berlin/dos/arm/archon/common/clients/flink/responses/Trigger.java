package de.tu_berlin.dos.arm.archon.common.clients.flink.responses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Trigger {

    @SerializedName("request-id")
    @Expose
    public String requestId;
}
