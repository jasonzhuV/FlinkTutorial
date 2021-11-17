package com.tutorial.generic;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

@PublicEvolving
@FunctionalInterface
public interface MySerializableTimestampAssigner<T> extends MyTimestampAssigner<T>, Serializable {}
