/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.type;

import org.apache.atlas.type.AtlasBuiltInTypes.AtlasBigDecimalType;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasBigDecimalType {
    private final AtlasBigDecimalType bigDecimalType = new AtlasBigDecimalType();
    private final Object[]            validValues    = {
            null, Byte.valueOf((byte) 1), Short.valueOf((short) 1), Integer.valueOf(1), Long.valueOf(1L), Float.valueOf(1),
            Double.valueOf(1), BigInteger.valueOf(1), BigDecimal.valueOf(1.0), "1.0",
    };

    private final Object[] negativeValues = {
            Byte.valueOf((byte) -1), Short.valueOf((short) -1), Integer.valueOf(-1), Long.valueOf(-1L), Float.valueOf(-1),
            Double.valueOf(-1), BigInteger.valueOf(-1), BigDecimal.valueOf(-1.0), "-1.0",
    };

    private final Object[] invalidValues = {"", "12ab", "abcd", "-12ab"};

    @Test
    public void testBigDecimalTypeDefaultValue() {
        BigDecimal defValue = bigDecimalType.createDefaultValue();

        assertEquals(defValue, BigDecimal.valueOf(0));
    }

    @Test
    public void testBigDecimalTypeIsValidValue() {
        for (Object value : validValues) {
            assertTrue(bigDecimalType.isValidValue(value), "value=" + value);
        }

        for (Object value : negativeValues) {
            assertTrue(bigDecimalType.isValidValue(value), "value=" + value);
        }

        for (Object value : invalidValues) {
            assertFalse(bigDecimalType.isValidValue(value), "value=" + value);
        }
    }

    @Test
    public void testBigDecimalTypeGetNormalizedValue() {
        assertNull(bigDecimalType.getNormalizedValue(null), "value=" + null);

        for (Object value : validValues) {
            if (value == null) {
                continue;
            }

            BigDecimal normalizedValue = bigDecimalType.getNormalizedValue(value);

            assertNotNull(normalizedValue, "value=" + value);

            if (value instanceof BigInteger) {
                assertEquals(normalizedValue, BigDecimal.valueOf(1), "value=" + value);
            } else {
                assertEquals(normalizedValue, BigDecimal.valueOf(1.0), "value=" + value);
            }
        }

        for (Object value : negativeValues) {
            BigDecimal normalizedValue = bigDecimalType.getNormalizedValue(value);

            assertNotNull(normalizedValue, "value=" + value);
            if (value instanceof BigInteger) {
                assertEquals(normalizedValue, BigDecimal.valueOf(-1), "value=" + value);
            } else {
                assertEquals(normalizedValue, BigDecimal.valueOf(-1.0), "value=" + value);
            }
        }

        for (Object value : invalidValues) {
            assertNull(bigDecimalType.getNormalizedValue(value), "value=" + value);
        }
    }

    @Test
    public void testBigDecimalTypeValidateValue() {
        List<String> messages = new ArrayList<>();
        for (Object value : validValues) {
            assertTrue(bigDecimalType.validateValue(value, "testObj", messages));
            assertEquals(messages.size(), 0, "value=" + value);
        }

        for (Object value : negativeValues) {
            assertTrue(bigDecimalType.validateValue(value, "testObj", messages));
            assertEquals(messages.size(), 0, "value=" + value);
        }

        for (Object value : invalidValues) {
            assertFalse(bigDecimalType.validateValue(value, "testObj", messages));
            assertEquals(messages.size(), 1, "value=" + value);
            messages.clear();
        }
    }
}
