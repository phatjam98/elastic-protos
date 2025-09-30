package com.phatjam98.elasticsearch.utils;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * Utility used to generate DocId's for Platform resources.
 */
public class DocIdUtils {

  private DocIdUtils() {
  }

  /**
   * Generic DocId builder. It concatenates the provided arguments with a hyphen ("-") as the
   * separator.
   * The method accepts the initial key as the first param followed by as many additional ordered
   * keys as needed.
   * For example:
   * <pre>{@code
   * var key = getDocId("my", "special", "key")
   * }</pre>
   * In this case <b>key</b> would equal <b>my-special-key</b>.
   *
   * @param first String, the first and required param.
   * @param args  String..., any number of additional key components in order.
   * @return String, the concatenated key.
   */
  public static String getDocId(String first, String... args) {
    StringBuilder docId = new StringBuilder(first);
    for (String arg : args) {
      docId.append("-").append(arg);
    }

    return docId.toString();
  }

  /**
   * Generic DocId builder. It generates an MD5 hash of the concatenation of provided arguments.
   * The method accepts the initial key as the first param followed by as many additional ordered
   * keys as needed.
   * For example:
   * <pre>{@code
   * var key = getDocId("my", "special", "key")
   * }</pre>
   * In this case <b>key</b> would equal the MD5 hash of <b>my-special-key</b>.
   *
   * <p>The ID is constructed by concatenating the arguments with a hyphen ("-") and then hashing
   * the resulting string using MD5 to ensure a unique and consistent length ID, regardless of the
   * number or size of the input arguments.
   *
   * @param first String, the first and required param.
   * @param args  String..., any number of additional key components in order.
   * @return String, the MD5 hash of the key generated from the input args.
   */
  public static String getDocIdHash(String first, String... args) {
    return DigestUtils.md5Hex(getDocId(first, args));
  }

  /**
   * Generate DocId for {@link com.phatjam98.protos.core.protos.UserNotification}.
   * <br>
   * Format: <pre>{@code {userId}-{locationId}-{date}}</pre>
   *
   * @param userId     String
   * @param locationId String
   * @param date       int
   * @return String docId for UserNotifications
   */
  public static String getUserNotificationDocId(String userId, String locationId, int date) {
    return getDocId(userId, locationId, Integer.toString(date));
  }

  public static String getNotificationDocId(String locationId, int date) {
    return getDocId(locationId, Integer.toString(date));
  }
}