package com.utilitytools;

import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CsvUtilsTest {
  @Test
  void writeAndRead_roundTrip_file() throws Exception {
    List<List<String>> rows = Arrays.asList(
        Arrays.asList("id", "name", "notes"),
        Arrays.asList("1", "Alice, Bob", "Line1\nLine2"),
        Arrays.asList("2", "Carol", "She said \"Hello\"")
    );
    Path tmp = Path.of("target/test-csv.csv");
    Files.createDirectories(tmp.getParent());
    CsvUtils.writeAll(tmp, rows);
    List<List<String>> back = CsvUtils.readAll(tmp);
    assertEquals(rows, back);
  }

  @Test
  void writeAndRead_roundTrip_streams() throws Exception {
    List<List<String>> rows = Arrays.asList(
        Arrays.asList("A", "B"),
        Arrays.asList("x", "y,z")
    );
    StringWriter sw = new StringWriter();
    CsvUtils.writeAll(sw, rows);
    String data = sw.toString();
    List<List<String>> back = CsvUtils.readAll(new StringReader(data));
    assertEquals(rows, back);
  }

  @Test
  void escapeField_quotesCommasNewlines() {
    assertEquals("\"a,b\"", CsvUtils.escapeField("a,b"));
    assertEquals("\"a\"\"b\"", CsvUtils.escapeField("a\"b"));
    assertEquals("\"a\nb\"", CsvUtils.escapeField("a\nb"));
  }
}

