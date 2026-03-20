/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.index.radix;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSearchBound {

  @Test
  public void testCreateBound() {
    SearchBound bound = new SearchBound(2, 5);

    assertEquals(2, bound.getLo());
    assertEquals(5, bound.getHi());
    assertEquals(3, bound.width());
  }

  @Test
  public void testRejectNegativeLo() {
    assertThrows(IllegalArgumentException.class, () -> new SearchBound(-1, 5));
  }

  @Test
  public void testRejectNegativeHi() {
    assertThrows(IllegalArgumentException.class, () -> new SearchBound(0, -1));
  }

  @Test
  public void testRejectHiLessThanLo() {
    assertThrows(IllegalArgumentException.class, () -> new SearchBound(5, 4));
  }

  @Test
  public void testConstructorRejectsNegativeLo() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> new SearchBound(-1, 2));
    assertTrue(e.getMessage().contains("lo"));
  }

  @Test
  public void testConstructorRejectsNegativeHi() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> new SearchBound(0, -1));
    assertTrue(e.getMessage().contains("hi"));
  }

  @Test
  public void testConstructorRejectsHiLessThanLo() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> new SearchBound(3, 2));
    assertTrue(e.getMessage().contains("hi"));
  }

  @Test
  public void testEqualsAndHashCode() {
    SearchBound b1 = new SearchBound(1, 4);
    SearchBound b2 = new SearchBound(1, 4);
    SearchBound b3 = new SearchBound(1, 5);

    assertEquals(b1, b2);
    assertEquals(b1.hashCode(), b2.hashCode());
    assertNotEquals(b1, b3);
    assertNotEquals(b1, null);
    assertNotEquals(b1, "bound");
  }

  @Test
  public void testToStringContainsFields() {
    SearchBound bound = new SearchBound(1, 4);

    String s = bound.toString();

    assertTrue(s.contains("1"));
    assertTrue(s.contains("4"));
  }
}