package com.phatjam98.elasticprotos.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Helpers for building deterministic document ids from key components. */
public final class DocIds {

  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private DocIds() {}

  /**
   * Joins the components with a hyphen, for example {@code join("my", "special", "key")} yields
   * {@code my-special-key}.
   *
   * @param first the first, required component
   * @param rest any further components in order
   * @return the joined id
   */
  public static String join(String first, String... rest) {
    StringBuilder id = new StringBuilder(first);
    for (String component : rest) {
      id.append('-').append(component);
    }
    return id.toString();
  }

  /**
   * Returns the lower-case hex MD5 digest of {@link #join(String, String...)}, giving a
   * fixed-length id regardless of the size or number of components.
   *
   * @param first the first, required component
   * @param rest any further components in order
   * @return a 32 character hex string
   */
  public static String hash(String first, String... rest) {
    try {
      byte[] digest =
          MessageDigest.getInstance("MD5")
              .digest(join(first, rest).getBytes(StandardCharsets.UTF_8));
      char[] out = new char[digest.length * 2];
      for (int i = 0; i < digest.length; i++) {
        out[i * 2] = HEX[(digest[i] >> 4) & 0xF];
        out[i * 2 + 1] = HEX[digest[i] & 0xF];
      }
      return new String(out);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("MD5 is required by the JVM specification", e);
    }
  }
}
