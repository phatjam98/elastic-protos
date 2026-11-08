package com.phatjam98.elasticprotos

import com.phatjam98.elasticprotos.util.Json

/**
 * Golden-file assertions. Run {@code ./gradlew test -PupdateGolden=true} to (re)generate the files
 * under src/test/resources/golden, then review the diff before committing.
 */
class Golden {

  static final boolean UPDATE = Boolean.getBoolean('updateGolden')

  static void assertMatches(String name, Object value) {
    File file = new File("src/test/resources/golden/${name}.json")
    String actual = Json.pretty(value) + '\n'
    if (UPDATE) {
      file.parentFile.mkdirs()
      file.text = actual
      return
    }
    assert file.exists(): "Missing golden ${file}; run ./gradlew test -PupdateGolden=true and review it"
    String expected = file.text
    assert expected == actual: "Golden ${name} differs; rerun with -PupdateGolden=true if intended"
  }
}
