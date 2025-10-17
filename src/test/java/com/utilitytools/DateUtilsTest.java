package com.utilitytools;

import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class DateUtilsTest {
  @Test
  void formatAndParseLocalDate_roundTrip() {
    LocalDate d = LocalDate.of(2025, 1, 2);
    String s = DateUtils.format(d, "yyyy/MM/dd");
    assertEquals("2025/01/02", s);
    assertEquals(d, DateUtils.parseLocalDate(s, "yyyy/MM/dd"));
  }

  @Test
  void formatAndParseLocalDateTime_roundTrip() {
    LocalDateTime dt = LocalDateTime.of(2025, 1, 2, 3, 4, 5);
    String s = DateUtils.format(dt); // default pattern
    assertEquals(dt, DateUtils.parseLocalDateTime(s, DateUtils.DEFAULT_DATETIME_PATTERN));
  }

  @Test
  void parseToDate_withZone() {
    LocalDateTime dt = LocalDateTime.of(2025, 1, 2, 3, 4, 5);
    String s = DateUtils.format(dt);
    Date legacy = DateUtils.parseToDate(s, DateUtils.DEFAULT_DATETIME_PATTERN, ZoneId.of("UTC"));
    assertNotNull(legacy);
  }

  @Test
  void tryParse_returnsNullOnFailure() {
    assertNull(DateUtils.tryParseLocalDate("bogus", "yyyy-MM-dd"));
    assertNull(DateUtils.tryParseLocalDateTime("bogus", DateUtils.DEFAULT_DATETIME_PATTERN));
    assertNull(DateUtils.tryParseToDate("bogus", DateUtils.DEFAULT_DATETIME_PATTERN, ZoneId.systemDefault()));
  }
}

