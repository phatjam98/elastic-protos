package com.phatjam98.elasticprotos.codec;

/** What to do with document keys that do not correspond to a proto field when reading back. */
public enum UnknownFieldPolicy {
  /** Drop them silently; the default, since fields removed from a proto linger in old documents. */
  IGNORE,
  /** Fail with a {@link CodecException}. */
  FAIL
}
