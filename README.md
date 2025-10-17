utility-tools
================

A small Java utility library providing:

- Date formatting helpers (date ↔ string)
- CSV reader/writer (no external deps)
- String sanitization helpers
- Basic logging utility

Quick start
-----------

Maven (Java 11+):

1) Build: `mvn -q -DskipTests package`
2) Use JAR from `target/utility-tools-0.1.0.jar`

Manual compile:

- `javac -d out src/main/java/com/utilitytools/*.java`

Utilities
---------

- `com.utilitytools.DateUtils`: Format/parse `LocalDate`/`LocalDateTime` and convert to/from `Date`.
- `com.utilitytools.CsvUtils`: Read/write CSV from/to `Path` or `Reader`/`Writer`, handling quotes and commas.
- `com.utilitytools.TextSanitizer`: Remove diacritics/special chars, trim and normalize whitespace.
- `com.utilitytools.Logger`: Minimal logger with levels (DEBUG/INFO/WARN/ERROR), timestamped output, optional file sink.

Examples
--------

```java
import com.utilitytools.*;
import java.nio.file.Path;
import java.time.*;
import java.util.*;

public class Demo {
  public static void main(String[] args) throws Exception {
    // Date formatting
    String s = DateUtils.format(LocalDate.now(), "yyyy-MM-dd");
    LocalDate d = DateUtils.parseLocalDate(s, "yyyy-MM-dd");

    // CSV read/write
    List<List<String>> rows = Arrays.asList(
      Arrays.asList("id", "name"),
      Arrays.asList("1", "Alice, Bob")
    );
    Path csv = Path.of("example.csv");
    CsvUtils.writeAll(csv, rows);
    List<List<String>> readBack = CsvUtils.readAll(csv);

    // String sanitization
    String cleaned = TextSanitizer.sanitize("  Héllo, Wørld!  "); // "Hello World"

    // Logging
    Logger log = Logger.get("demo").level(Logger.Level.DEBUG);
    log.info("Started with rows=%d cleaned=%s", readBack.size(), cleaned);
  }
}
```

Notes
-----

- CSV parser supports quotes, commas, and newlines inside quoted fields (RFC 4180-style). Quotes inside fields are escaped by doubling ("").
- All utilities are dependency-free; only Java standard library is used.
