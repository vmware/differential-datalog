package ddlogapi;

/**
 * Configuration of a DDlog program instance.
 */
public class DDlogConfig {
    /**
     * Create default configuration.
     */
    public DDlogConfig() {
        this.numTimelyWorkers = 1;
        this.enableDebugRegions = false;
        this.differentialIdleMergeEffort = 0;
        this.profilingConfig = profilingDisabled();
    }

    public DDlogConfig(int workers) {
        this();
        this.numTimelyWorkers = workers;
    }

    public DDlogConfig(
            int workers,
            boolean enableDebugRegions,
            long differentialIdleMergeEffort,
            ProfilingConfig profilingConfig) {
        this.numTimelyWorkers = workers;
        this.enableDebugRegions = enableDebugRegions;
        this.differentialIdleMergeEffort = differentialIdleMergeEffort;
        this.profilingConfig = profilingConfig;
    }

    /**
     * Logging mode for timely and differential profiling event streams.
     * WARNING: The following constants must match those in ddlog.h
     */
    public enum LogMode {
        // Logging disabled.
        DISABLED(0),
        // Log to socket.
        SOCKET(1),
        // Log to disk.
        DISK(2);

        public final int label;

        private LogMode(int label) {
            this.label = label;
        }
    }

    /**
     * Location to send a timely or differential profiling event stream.
     */
    public static class LogDestination {
        private LogDestination(LogMode mode, String addressStr) {
            this.mode = mode;
            this.addressStr = addressStr;
        }

        public LogMode getMode() {
            return this.mode;
        }

        public String getAddressStr() {
            return this.addressStr;
        }

        // Logging mode.
        private LogMode mode;

        // NULL, if mode == ddlog_log_disabled,
        // Socket address, e.g., "127.0.0.1:51317", if mode == ddlog_log_to_socket,
        // Directory path, e.g., "./timely_trace", if mode == ddlog_log_to_disk.
        private String addressStr;
    }

    public static LogDestination logDisabled() {
        return new LogDestination(LogMode.DISABLED, null);
    }

    public static LogDestination logToSocket(String sockaddr) {
        return new LogDestination(LogMode.SOCKET, sockaddr);
    }

    public static LogDestination logToDisk(String path) {
        return new LogDestination(LogMode.DISK, path);
    }


    /**
     * DDlog profiling mode:
     * WARNING: The following constants must match those in ddlog.h
     */
    public enum ProfilingMode {
        // Profiling disabled.
        DISABLED(0),
        // Enable self-profiler.
        SELF_PROFILING(1),
        // Use external timely dataflow profiler.
        TIMELY_PROFILING(2);

        public final int label;

        private ProfilingMode(int label) {
            this.label = label;
        }
    }

    /**
     *  Profiling configuration.
     */
    public static class ProfilingConfig {
        private ProfilingConfig(
                ProfilingMode mode,
                LogDestination timelyDestination,
                LogDestination timelyProgressDestination,
                LogDestination differentialDestination) {
            this.mode = mode;
            if (timelyDestination == null) {
                throw new NullPointerException("timelyDestination must be non-null");
            }
            if (timelyProgressDestination == null) {
                throw new NullPointerException("timelyProgressDestination must be non-null");
            }
            if (differentialDestination == null) {
                throw new NullPointerException("differentialDestination must be non-null");
            }

            this.timelyDestination = timelyDestination;
            this.timelyProgressDestination = timelyProgressDestination;
            this.differentialDestination = differentialDestination;
        }

        public ProfilingMode getMode() {
            return this.mode;
        }

        public LogDestination getTimelyDestination() {
            return this.timelyDestination;
        }

        public LogDestination getTimelyProgressDestination() {
            return this.timelyProgressDestination;
        }

        public LogDestination getDifferentialDestination() {
            return this.differentialDestination;
        }

        private ProfilingMode mode;

        /* The following fields are only used if (mode == ddlog_timely_profiling) */

        // Destination for the timely log stream.
        private LogDestination timelyDestination;
        // Destination for timely progress logging.
        private LogDestination timelyProgressDestination;
        // Differential for Differential Dataflow events.
        private LogDestination differentialDestination;
    }

    public static ProfilingConfig profilingDisabled() {
        return new ProfilingConfig(ProfilingMode.DISABLED, logDisabled(), logDisabled(), logDisabled());
    }

    public static ProfilingConfig selfProfiling() {
        return new ProfilingConfig(ProfilingMode.SELF_PROFILING, logDisabled(), logDisabled(), logDisabled());
    }

    public static ProfilingConfig timelyProfiling(
            LogDestination timelyDestination,
            LogDestination timelyProgressDestination,
            LogDestination differentialDestination) {
        return new ProfilingConfig(ProfilingMode.TIMELY_PROFILING, timelyDestination, timelyProgressDestination, differentialDestination);
    }

    public int getNumTimelyWorkers() {
        return this.numTimelyWorkers;
    }

    public void setNumTimelyWorkers(int workers) {
        this.numTimelyWorkers = workers;
    }

    public boolean getEnableDebugRegions() {
        return this.enableDebugRegions;
    }

    public void setEnableDebugRegions(boolean enable) {
        this.enableDebugRegions = enable;
    }

    public long getDifferentialIdleMergeEffort() {
        return this.differentialIdleMergeEffort;
    }

    public void setDifferentialIdleMergeEffort(long effort) {
        this.differentialIdleMergeEffort = effort;
    }

    public ProfilingConfig getProfilingConfig() {
        return this.profilingConfig;
    }

    public void setProfilingConfig(ProfilingConfig config) {
        this.profilingConfig = config;
    }

    private int numTimelyWorkers;
    private boolean enableDebugRegions;
    private long differentialIdleMergeEffort;
    private ProfilingConfig profilingConfig;
}
