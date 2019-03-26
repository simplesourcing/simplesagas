package io.simplesource.saga.action.common;

import lombok.Value;

import java.util.Properties;

@Value
public class TopologyBuildContext<I> {
    public final I buildInput;
    public final Properties config;
}
