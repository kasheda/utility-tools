package com.utilitytools;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * Minimal logger with levels and optional file output.
 */
public final class Logger {
  public enum Level { DEBUG, INFO, WARN, ERROR }

  private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  private final String name;
  private volatile Level level = Level.INFO;
  private volatile PrintWriter fileWriter = null;

  private Logger(String name) { this.name = name == null ? "" : name; }

  public static Logger get(String name) { return new Logger(name); }
  public static Logger get(Class<?> cls) { return new Logger(cls == null ? "" : cls.getSimpleName()); }

  public Logger level(Level lvl) { this.level = Objects.requireNonNull(lvl, "level"); return this; }

  public Logger toFile(Path path) throws IOException {
    closeFile();
    if (path != null) {
      Files.createDirectories(path.getParent() == null ? Path.of(".") : path.getParent());
      this.fileWriter = new PrintWriter(Files.newBufferedWriter(path), true);
    }
    return this;
  }

  public void closeFile() {
    if (fileWriter != null) {
      fileWriter.flush();
      fileWriter.close();
      fileWriter = null;
    }
  }

  public void debug(String fmt, Object... args) { log(Level.DEBUG, null, fmt, args); }
  public void info(String fmt, Object... args)  { log(Level.INFO,  null, fmt, args); }
  public void warn(String fmt, Object... args)  { log(Level.WARN,  null, fmt, args); }
  public void error(String fmt, Object... args) { log(Level.ERROR, null, fmt, args); }
  public void error(Throwable t, String fmt, Object... args) { log(Level.ERROR, t, fmt, args); }

  public synchronized void log(Level lvl, Throwable t, String fmt, Object... args) {
    if (lvl.ordinal() < level.ordinal()) return;
    String ts = LocalDateTime.now().format(TS);
    String msg = safeFormat(fmt, args);
    String line = String.format("%s %-5s [%s] %s", ts, lvl.name(), name, msg);

    PrintWriter console = (lvl.ordinal() >= Level.WARN.ordinal())
      ? new PrintWriter(System.err, true)
      : new PrintWriter(System.out, true);
    console.println(line);
    if (t != null) t.printStackTrace(console);

    if (fileWriter != null) {
      fileWriter.println(line);
      if (t != null) t.printStackTrace(fileWriter);
      fileWriter.flush();
    }
  }

  private static String safeFormat(String fmt, Object... args) {
    try { return (fmt == null) ? "" : (args == null || args.length == 0) ? fmt : String.format(fmt, args); }
    catch (Exception e) { return String.valueOf(fmt); }
  }
}

