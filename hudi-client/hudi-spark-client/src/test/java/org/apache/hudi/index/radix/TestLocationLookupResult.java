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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestLocationLookupResult {

  @Test
  void testGetPositionFailsWhenNotFound() {
    LocationLookupResult result = LocationLookupResult.notFound(2, new SearchBound(1, 4));

    assertThrows(IllegalStateException.class, result::getPosition);
  }

  @Test
  public void testFoundResult() {
    SearchBound bound = new SearchBound(2, 5);
    LocationLookupResult result = LocationLookupResult.found(3, bound);

    assertTrue(result.isFound());
    assertEquals(3, result.getPosition());
    assertEquals(3, result.getInsertPosition());
    assertEquals(bound, result.getBound());
  }

  @Test
  public void testNotFoundResult() {
    SearchBound bound = new SearchBound(1, 4);
    LocationLookupResult result = LocationLookupResult.notFound(2, bound);

    assertFalse(result.isFound());
    assertEquals(2, result.getInsertPosition());
    assertEquals(bound, result.getBound());
  }

  @Test
  public void testFoundRejectsNegativePosition() {
    assertThrows(IllegalArgumentException.class,
        () -> LocationLookupResult.found(-1, new SearchBound(0, 1)));
  }

  @Test
  public void testNotFoundRejectsNegativeInsertPosition() {
    assertThrows(IllegalArgumentException.class,
        () -> LocationLookupResult.notFound(-1, new SearchBound(0, 1)));
  }

  @Test
  public void testFoundRejectsNullBound() {
    assertThrows(NullPointerException.class,
        () -> LocationLookupResult.found(0, null));
  }

  @Test
  public void testNotFoundRejectsNullBound() {
    assertThrows(NullPointerException.class,
        () -> LocationLookupResult.notFound(0, null));
  }

  @Test
  public void testEqualsAndHashCode() {
    SearchBound bound = new SearchBound(1, 4);

    LocationLookupResult r1 = LocationLookupResult.found(2, bound);
    LocationLookupResult r2 = LocationLookupResult.found(2, bound);
    LocationLookupResult r3 = LocationLookupResult.notFound(2, bound);

    assertEquals(r1, r2);
    assertEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r3);
    assertNotEquals(r1, null);
    assertNotEquals(r1, "result");
  }

  @Test
  public void testToStringContainsFields() {
    LocationLookupResult result = LocationLookupResult.notFound(2, new SearchBound(1, 4));

    String s = result.toString();

    assertTrue(s.contains("found"));
    assertTrue(s.contains("insertPosition"));
    assertTrue(s.contains("bound"));
  }
}