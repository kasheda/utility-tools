package com.utilitytools;

import java.text.Normalizer;
import java.util.Objects;

/**
 * String sanitization helpers: remove diacritics, special chars, and normalize whitespace.
 */
public final class TextSanitizer {
  private TextSanitizer() {}

  /**
   * Remove leading/trailing whitespace and collapse any consecutive whitespace to a single space.
   */
  public static String trimAndNormalizeWhitespace(String input) {
    if (input == null) return null;
    String trimmed = input.trim();
    return trimmed.replaceAll("\\s+", " ");
  }

  /**
   * Remove diacritical marks (accents), producing plain ASCII letters where possible.
   */
  public static String removeDiacritics(String input) {
    if (input == null) return null;
    String norm = Normalizer.normalize(input, Normalizer.Form.NFD);
    return norm.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
  }

  /**
   * Remove all characters except letters, digits, and spaces. Keeps unicode letters/digits.
   */
  public static String removeSpecialChars(String input) {
    if (input == null) return null;
    return input.replaceAll("[^\\p{L}\\p{Nd} ]+", "");
  }

  /**
   * Remove all characters except letters, digits, spaces, and the characters in allowedExtra (treated literally).
   * This variant avoids regex escaping and supports full Unicode code points.
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
   */
  public static String sanitize(String input) {
    if (input == null) return null;
    String s = removeDiacritics(input);
    s = removeSpecialChars(s);
    s = trimAndNormalizeWhitespace(s);
    return s;
  }
}

