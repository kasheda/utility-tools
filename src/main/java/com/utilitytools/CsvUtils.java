package com.utilitytools;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Minimal CSV reader/writer that supports RFC 4180 style CSV:
 * - Commas and newlines inside quoted fields
 * - Quotes inside fields escaped as "" (double quote)
 */
public final class CsvUtils {
  private CsvUtils() {}

  // Convenience defaults
  public static List<List<String>> readAll(Path path) throws IOException {
    return readAll(path, StandardCharsets.UTF_8);
  }

  public static List<List<String>> readAll(Path path, Charset charset) throws IOException {
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(charset, "charset");
    try (Reader r = Files.newBufferedReader(path, charset)) {
      return readAll(r);
    }
  }

  public static List<List<String>> readAll(Reader reader) throws IOException {
    Objects.requireNonNull(reader, "reader");
    PushbackReader in = (reader instanceof PushbackReader) ? (PushbackReader) reader : new PushbackReader(reader, 1);

    List<List<String>> rows = new ArrayList<>();
    List<String> currentRow = new ArrayList<>();
    StringBuilder field = new StringBuilder();
    boolean inQuotes = false;

    int chInt;
    while ((chInt = in.read()) != -1) {
      char ch = (char) chInt;
      if (inQuotes) {
        if (ch == '"') {
          int next = in.read();
          if (next == '"') {
            field.append('"'); // escaped quote
          } else {
            inQuotes = false; // closing quote
            if (next != -1) in.unread(next);
          }
        } else {
          field.append(ch);
        }
      } else {
        if (ch == '"') {
          inQuotes = true;
        } else if (ch == ',') {
          currentRow.add(field.toString());
          field.setLength(0);
        } else if (ch == '\r' || ch == '\n') {
          // End of record (handle CRLF)
          currentRow.add(field.toString());
          field.setLength(0);
          rows.add(currentRow);
          currentRow = new ArrayList<>();
          if (ch == '\r') {
            int next = in.read();
            if (next != '\n' && next != -1) in.unread(next);
          }
        } else {
          field.append(ch);
        }
      }
    }

    // finalize last field/row if any
    if (inQuotes) {
      throw new IOException("Malformed CSV: unterminated quoted field");
    }
    // if there is remaining content or a partially built row, flush it
    if (field.length() > 0 || !currentRow.isEmpty()) {
      currentRow.add(field.toString());
      rows.add(currentRow);
    }
    return rows;
  }

  public static void writeAll(Path path, List<List<String>> rows) throws IOException {
    writeAll(path, rows, StandardCharsets.UTF_8);
  }

  public static void writeAll(Path path, List<List<String>> rows, Charset charset) throws IOException {
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(rows, "rows");
    Objects.requireNonNull(charset, "charset");
    try (Writer w = Files.newBufferedWriter(path, charset)) {
      writeAll(w, rows);
    }
  }

  public static void writeAll(Writer writer, List<List<String>> rows) throws IOException {
    Objects.requireNonNull(writer, "writer");
    Objects.requireNonNull(rows, "rows");
    for (int i = 0; i < rows.size(); i++) {
      List<String> row = rows.get(i);
      writeRow(writer, row);
      if (i < rows.size() - 1) writer.write("\n");
    }
    writer.flush();
  }

  public static void writeRow(Writer writer, List<String> row) throws IOException {
    if (row == null) row = Collections.emptyList();
    for (int i = 0; i < row.size(); i++) {
      if (i > 0) writer.write(',');
      writer.write(escapeField(row.get(i)));
    }
  }

  public static String escapeField(String field) {
    if (field == null) field = "";
    boolean needsQuoting = false;
    for (int i = 0; i < field.length(); i++) {
      char c = field.charAt(i);
      if (c == '"' || c == ',' || c == '\n' || c == '\r') {
        needsQuoting = true;
        break;
      }
    }
    if (!needsQuoting) return field;
    String escaped = field.replace("\"", "\"\"");
    return '"' + escaped + '"';
  }
}

