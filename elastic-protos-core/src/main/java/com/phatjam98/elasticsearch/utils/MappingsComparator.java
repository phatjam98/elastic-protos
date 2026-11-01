package com.phatjam98.elasticsearch.utils;

import co.elastic.clients.elasticsearch._types.mapping.Property;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This takes current and previous Elasticsearch Index Mappings as JSON nodes and Compares
 * to make sure no breaking changes are being introduced and check if further migration steps are
 * required from the bootstrap.
 */
public class MappingsComparator {

  private static final Logger LOGGER = LoggerFactory.getLogger(MappingsComparator.class);

  public static Boolean compareMappings(Map<String, Property> left, Map<String, Property> right,
                                        String path, Boolean isEqual) {
    for (String key : left.keySet()) {
      String newPath = path.isEmpty() ? key : path + "." + key;
      if (!right.containsKey(key)) {
        isEqual = false;
        LOGGER.info("Field missing in second map: {}", newPath);
      } else {
        var val1 = left.get(key);
        var val2 = right.get(key);
        if (val1.isNested() && val2.isNested()) {
          isEqual = compareMappings(val1.nested().properties(), val2.nested().properties(),
              newPath, isEqual);
        } else if (val1._kind() != val2._kind()) {
          LOGGER.info("Different values at path: {} \n\t {} \n\t {}", newPath, val1, val2);
          isEqual = false;
        }
      }
    }

    for (String key : right.keySet()) {
      String newPath = path.isEmpty() ? key : path + "." + key;

      if (!left.containsKey(key)) {
        isEqual = false;
        LOGGER.info("Field missing in first map: {}", newPath);
      }
    }

    return isEqual;
  }
}