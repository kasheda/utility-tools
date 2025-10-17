package com.utilitytools;

import java.text.Normalizer;
import java.util.Objects;

/**
 * String sanitization helpers: remove diacritics, special characters, and normalize whitespace.
 * <p>
 * Key operations:
 * - {@link #trimAndNormalizeWhitespace(String)}: collapse whitespace to single spaces and trim ends
 * - {@link #removeDiacritics(String)}: strip combining marks (accents)
 * - {@link #removeSpecialChars(String)}: keep only letters, digits, and spaces
 * - {@link #removeSpecialChars(String, String)}: additionally allow literal characters from {@code allowedExtra}
 * - {@link #sanitize(String)}: common pipeline of the above
 */
public final class TextSanitizer {
  private TextSanitizer() {}

  /**
   * Remove leading/trailing whitespace and collapse any consecutive whitespace to a single space.
   * @param input input text (nullable)
   * @return normalized text or {@code null} if input is {@code null}
   */
  public static String trimAndNormalizeWhitespace(String input) {
    if (input == null) return null;
    String trimmed = input.trim();
    return trimmed.replaceAll("\\s+", " ");
  }

  /**
   * Remove diacritical marks (accents) by stripping combining marks.
   * Uses NFD normalization to split base letters and marks, then removes marks.
   * @param input input text (nullable)
   * @return text without combining marks or {@code null} if input is {@code null}
   */
  public static String removeDiacritics(String input) {
    if (input == null) return null;
    String norm = Normalizer.normalize(input, Normalizer.Form.NFD);
    return norm.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
  }

  /**
   * Remove all characters except letters, digits, and spaces. Keeps unicode letters/digits.
   * @param input input text (nullable)
   * @return filtered text or {@code null} if input is {@code null}
   */
  public static String removeSpecialChars(String input) {
    if (input == null) return null;
    return input.replaceAll("[^\\p{L}\\p{Nd} ]+", "");
  }

  /**
   * Remove all characters except letters, digits, spaces, and the characters in {@code allowedExtra} (treated literally).
   * This variant avoids regex escaping and supports full Unicode code points.
   * @param input input text (nullable)
   * @param allowedExtra literal characters to also keep (non-null, may be empty)
   * @return filtered text or {@code null} if input is {@code null}
   * @throws NullPointerException if {@code allowedExtra} is null
   */
  public static String removeSpecialChars(String input, String allowedExtra) {
    if (input == null) return null;
    Objects.requireNonNull(allowedExtra, "allowedExtra");
    StringBuilder sb = new StringBuilder(input.length());
    for (int i = 0; i < input.length(); ) {
      int cp = input.codePointAt(i);
      if (Character.isLetter(cp) || Character.isDigit(cp) || cp == ' ' || containsCodePoint(allowedExtra, cp)) {
        sb.appendCodePoint(cp);
      }
      i += Character.charCount(cp);
    }
    return sb.toString();
  }

  private static boolean containsCodePoint(String s, int cp) {
    for (int i = 0; i < s.length(); ) {
      int c = s.codePointAt(i);
      if (c == cp) return true;
      i += Character.charCount(c);
    }
    return false;
  }

  /**
   * Full sanitize pipeline: remove diacritics, drop special chars, then trim/normalize whitespace.
   * @param input input text (nullable)
   * @return sanitized text or {@code null} if input is {@code null}
   */
  public static String sanitize(String input) {
    if (input == null) return null;
    String s = removeDiacritics(input);
    s = removeSpecialChars(s);
    s = trimAndNormalizeWhitespace(s);
    return s;
  }
}
