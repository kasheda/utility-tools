package com.utilitytools;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Objects;

/**
 * Utilities for formatting and parsing dates and times using {@code java.time}.
 * <p>
 * This class provides convenience helpers to:
 * <ul>
 *   <li>Format {@link java.time.LocalDate} and {@link java.time.LocalDateTime} using a pattern</li>
 *   <li>Parse text into {@link java.time.LocalDate}, {@link java.time.LocalDateTime}, or legacy {@link java.util.Date}</li>
 *   <li>Lenient "tryParse" variants that return {@code null} instead of throwing</li>
 * </ul>
 */
public final class DateUtils {
  private DateUtils() {}

  public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd";
  public static final String DEFAULT_DATETIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";

  // Formatting
  /**
   * Format a {@link LocalDate} with the default pattern {@code yyyy-MM-dd}.
   * @param date the date to format (non-null)
   * @return formatted date string
   * @throws NullPointerException if {@code date} is null
   */
  public static String format(LocalDate date) {
    return format(date, DEFAULT_DATE_PATTERN);
  }

  /**
   * Format a {@link LocalDate} with a custom pattern.
   * @param date the date to format (non-null)
   * @param pattern DateTimeFormatter pattern (non-null)
   * @return formatted date string
   * @throws NullPointerException if {@code date} or {@code pattern} is null
   */
  public static String format(LocalDate date, String pattern) {
    Objects.requireNonNull(date, "date");
    Objects.requireNonNull(pattern, "pattern");
    return date.format(DateTimeFormatter.ofPattern(pattern));
  }

  /**
   * Format a {@link LocalDateTime} with the default pattern {@code yyyy-MM-dd'T'HH:mm:ss}.
   * @param dateTime the date-time to format (non-null)
   * @return formatted date-time string
   * @throws NullPointerException if {@code dateTime} is null
   */
  public static String format(LocalDateTime dateTime) {
    return format(dateTime, DEFAULT_DATETIME_PATTERN);
  }

  /**
   * Format a {@link LocalDateTime} with a custom pattern.
   * @param dateTime the date-time to format (non-null)
   * @param pattern DateTimeFormatter pattern (non-null)
   * @return formatted date-time string
   * @throws NullPointerException if {@code dateTime} or {@code pattern} is null
   */
  public static String format(LocalDateTime dateTime, String pattern) {
    Objects.requireNonNull(dateTime, "dateTime");
    Objects.requireNonNull(pattern, "pattern");
    return dateTime.format(DateTimeFormatter.ofPattern(pattern));
  }

  /**
   * Format a legacy {@link Date} by converting to {@link LocalDateTime} in a zone and applying a pattern.
   * @param date the legacy date (non-null)
   * @param zone the zone used for conversion from instant (non-null)
   * @param pattern DateTimeFormatter pattern (non-null)
   * @return formatted string
   * @throws NullPointerException if any argument is null
   */
  public static String format(Date date, ZoneId zone, String pattern) {
    Objects.requireNonNull(date, "date");
    Objects.requireNonNull(zone, "zone");
    Objects.requireNonNull(pattern, "pattern");
    Instant instant = date.toInstant();
    LocalDateTime ldt = LocalDateTime.ofInstant(instant, zone);
    return ldt.format(DateTimeFormatter.ofPattern(pattern));
  }

  // Parsing
  /**
   * Parse text into a {@link LocalDate} using the given pattern.
   * @param text input text (non-null)
   * @param pattern DateTimeFormatter pattern (non-null)
   * @return parsed LocalDate
   * @throws java.time.format.DateTimeParseException if parsing fails
   * @throws NullPointerException if any argument is null
   */
  public static LocalDate parseLocalDate(String text, String pattern) {
    Objects.requireNonNull(text, "text");
    Objects.requireNonNull(pattern, "pattern");
    return LocalDate.parse(text, DateTimeFormatter.ofPattern(pattern));
  }

  /**
   * Parse text into a {@link LocalDateTime} using the given pattern.
   * @param text input text (non-null)
   * @param pattern DateTimeFormatter pattern (non-null)
   * @return parsed LocalDateTime
   * @throws java.time.format.DateTimeParseException if parsing fails
   * @throws NullPointerException if any argument is null
   */
  public static LocalDateTime parseLocalDateTime(String text, String pattern) {
    Objects.requireNonNull(text, "text");
    Objects.requireNonNull(pattern, "pattern");
    return LocalDateTime.parse(text, DateTimeFormatter.ofPattern(pattern));
  }

  /**
   * Parse text into a legacy {@link Date} by interpreting text as a {@link LocalDateTime} in a zone.
   * @param text input text (non-null)
   * @param pattern DateTimeFormatter pattern (non-null)
   * @param zone zone used for conversion to instant (non-null)
   * @return parsed Date
   * @throws java.time.format.DateTimeParseException if parsing fails
   * @throws NullPointerException if any argument is null
   */
  public static Date parseToDate(String text, String pattern, ZoneId zone) {
    Objects.requireNonNull(text, "text");
    Objects.requireNonNull(pattern, "pattern");
    Objects.requireNonNull(zone, "zone");
    LocalDateTime ldt = LocalDateTime.parse(text, DateTimeFormatter.ofPattern(pattern));
    Instant instant = ldt.atZone(zone).toInstant();
    return Date.from(instant);
  }

  // Safe parse variants returning null on failure
  /**
   * Try to parse text into {@link LocalDate}; returns {@code null} if parsing fails.
   */
  public static LocalDate tryParseLocalDate(String text, String pattern) {
    try { return parseLocalDate(text, pattern); } catch (DateTimeParseException e) { return null; }
  }

  /**
   * Try to parse text into {@link LocalDateTime}; returns {@code null} if parsing fails.
   */
  public static LocalDateTime tryParseLocalDateTime(String text, String pattern) {
    try { return parseLocalDateTime(text, pattern); } catch (DateTimeParseException e) { return null; }
  }

  /**
   * Try to parse text into a legacy {@link Date}; returns {@code null} if parsing fails.
   */
  public static Date tryParseToDate(String text, String pattern, ZoneId zone) {
    try { return parseToDate(text, pattern, zone); } catch (RuntimeException e) { return null; }
  }
}
