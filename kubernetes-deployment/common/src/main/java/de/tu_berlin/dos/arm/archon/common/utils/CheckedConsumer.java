package de.tu_berlin.dos.arm.archon.common.utils;

@FunctionalInterface
public interface CheckedConsumer<T> {

    void accept(T t) throws Exception;
}
