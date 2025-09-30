package com.phatjam98.elasticsearch.utils;

public enum BoolType {
  FILTER("filter"),
  MUST("must"),
  MUST_NOT("must_not"),
  SHOULD("should");

  private final String value;

  BoolType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}