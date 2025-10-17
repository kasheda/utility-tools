package com.utilitytools;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class LoggerTest {
  private Logger logger;

  @AfterEach
  void cleanup() {
    if (logger != null) logger.closeFile();
  }

  @Test
  void writesToFileAndHonorsLevel() throws Exception {
    Path path = Path.of("target/test-logs/logger.txt");
    Files.createDirectories(path.getParent());
    logger = Logger.get("test").level(Logger.Level.INFO).toFile(path);
    logger.debug("this should not appear");
    logger.info("hello %s", "world");
    logger.warn("warn!");
    logger.error("error!");
    String data = Files.readString(path, StandardCharsets.UTF_8);
    assertFalse(data.contains("this should not appear"));
    assertTrue(data.contains("INFO"));
    assertTrue(data.contains("WARN"));
    assertTrue(data.contains("ERROR"));
  }
}

