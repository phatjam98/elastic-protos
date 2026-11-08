package com.phatjam98.elasticprotos.engine;

/**
 * A summary of cluster health.
 *
 * @param status {@code green}, {@code yellow} or {@code red}
 * @param numberOfNodes all nodes
 * @param numberOfDataNodes nodes that hold data; drives the default replica count
 */
public record ClusterHealth(String status, int numberOfNodes, int numberOfDataNodes) {}
