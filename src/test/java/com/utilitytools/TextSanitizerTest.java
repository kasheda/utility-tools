package com.utilitytools;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TextSanitizerTest {
  @Test
  void trimAndNormalizeWhitespace_basic() {
    assertEquals("a b c", TextSanitizer.trimAndNormalizeWhitespace("  a   b\t c  "));
  }

  @Test
  void removeDiacritics_stripsAccents() {
    assertEquals("Cafe", TextSanitizer.removeDiacritics("Café"));
    assertEquals("Zoete", TextSanitizer.removeDiacritics("Zoëté"));
  }

  @Test
  void removeSpecialChars_defaultAndAllowed() {
    assertEquals("Hello World 123", TextSanitizer.removeSpecialChars("Hello, World! 123"));
    assertEquals("Hello-World_123", TextSanitizer.removeSpecialChars("Hello-World_123", "-_"));
  }

  @Test
  void sanitize_pipeline() {
    assertEquals("Hello World", TextSanitizer.sanitize("  Héllo,  Wørld!  "));
  }
}

