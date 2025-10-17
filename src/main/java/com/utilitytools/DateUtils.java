package com.utilitytools;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Objects;

/**
 * Date/time formatting and parsing helpers using java.time.
 */
public final class DateUtils {
  private DateUtils() {}

  public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd";
  public static final String DEFAULT_DATETIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";

  // Formatting
  public static String format(LocalDate date) {
    return format(date, DEFAULT_DATE_PATTERN);
  }

  public static String format(LocalDate date, String pattern) {
    Objects.requireNonNull(date, "date");
    Objects.requireNonNull(pattern, "pattern");
    return date.format(DateTimeFormatter.ofPattern(pattern));
  }

  public static String format(LocalDateTime dateTime) {
    return format(dateTime, DEFAULT_DATETIME_PATTERN);
  }

  public static String format(LocalDateTime dateTime, String pattern) {
    Objects.requireNonNull(dateTime, "dateTime");
    Objects.requireNonNull(pattern, "pattern");
    return dateTime.format(DateTimeFormatter.ofPattern(pattern));
  }

  public static String format(Date date, ZoneId zone, String pattern) {
    Objects.requireNonNull(date, "date");
    Objects.requireNonNull(zone, "zone");
    Objects.requireNonNull(pattern, "pattern");
    Instant instant = date.toInstant();
    LocalDateTime ldt = LocalDateTime.ofInstant(instant, zone);
    return ldt.format(DateTimeFormatter.ofPattern(pattern));
  }

  // Parsing
  public static LocalDate parseLocalDate(String text, String pattern) {
    Objects.requireNonNull(text, "text");
    Objects.requireNonNull(pattern, "pattern");
    return LocalDate.parse(text, DateTimeFormatter.ofPattern(pattern));
  }

  public static LocalDateTime parseLocalDateTime(String text, String pattern) {
    Objects.requireNonNull(text, "text");
    Objects.requireNonNull(pattern, "pattern");
    return LocalDateTime.parse(text, DateTimeFormatter.ofPattern(pattern));
  }

  public static Date parseToDate(String text, String pattern, ZoneId zone) {
    Objects.requireNonNull(text, "text");
    Objects.requireNonNull(pattern, "pattern");
    Objects.requireNonNull(zone, "zone");
    LocalDateTime ldt = LocalDateTime.parse(text, DateTimeFormatter.ofPattern(pattern));
    Instant instant = ldt.atZone(zone).toInstant();
    return Date.from(instant);
  }

  // Safe parse variants returning null on failure
  public static LocalDate tryParseLocalDate(String text, String pattern) {
    try { return parseLocalDate(text, pattern); } catch (DateTimeParseException e) { return null; }
  }

  public static LocalDateTime tryParseLocalDateTime(String text, String pattern) {
    try { return parseLocalDateTime(text, pattern); } catch (DateTimeParseException e) { return null; }
  }

  public static Date tryParseToDate(String text, String pattern, ZoneId zone) {
    try { return parseToDate(text, pattern, zone); } catch (RuntimeException e) { return null; }
  }
}

