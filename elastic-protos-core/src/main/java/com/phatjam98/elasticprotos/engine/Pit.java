package com.phatjam98.elasticprotos.engine;

import java.time.Duration;

/**
 * An open point in time.
 *
 * @param id the engine's identifier, to be sent with every search that uses it
 * @param keepAlive how long the engine keeps it after each use
 */
public record Pit(String id, Duration keepAlive) {}
