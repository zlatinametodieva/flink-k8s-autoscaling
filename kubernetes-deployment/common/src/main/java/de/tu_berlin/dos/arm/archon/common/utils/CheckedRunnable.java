package de.tu_berlin.dos.arm.archon.common.utils;

@FunctionalInterface
public interface CheckedRunnable {

    void run() throws Exception;
}
