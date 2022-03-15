package de.tu_berlin.dos.arm.archon.common.clients.flink.responses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Checkpoints {

    public class Completed {

        @SerializedName("status")
        @Expose
        public String status;
        @SerializedName("latest_ack_timestamp")
        @Expose
        public Long latestAckTimestamp;

    }

    static public class Latest {

        @SerializedName("completed")
        @Expose
        public Completed completed;

    }

    public class Summary {

        @SerializedName("state_size")
        @Expose
        public StateSize stateSize;
        @SerializedName("end_to_end_duration")
        @Expose
        public EndToEndDuration endToEndDuration;

    }

    public class StateSize {

        @SerializedName("min")
        @Expose
        public Long min;
        @SerializedName("max")
        @Expose
        public Long max;
        @SerializedName("avg")
        @Expose
        public Long avg;

    }

    public class EndToEndDuration {

        @SerializedName("min")
        @Expose
        public Long min;
        @SerializedName("max")
        @Expose
        public Long max;
        @SerializedName("avg")
        @Expose
        public Long avg;

    }

    @SerializedName("latest")
    @Expose
    public Latest latest;
    @SerializedName("summary")
    @Expose
    public Summary summary;

}