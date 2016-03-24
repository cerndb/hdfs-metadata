/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.util;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.db.util.SUtils.Color;

public class SUtilsTest {

	@Test
	public void color() {
		Assert.assertEquals("\u001B[31mHi\u001B[0m", SUtils.color(Color.R, "Hi"));
		Assert.assertEquals("\u001B[37mHi Hi Hi\u001B[0m", SUtils.color(Color.W, "Hi Hi Hi"));
	}
	
	@Test
	public void adjustLengthRightAlignment() {
		String result = SUtils.adjustLength("Hi", 5);
		Assert.assertEquals(5, result.length());
		Assert.assertEquals("Hi   ", result);
		
		result = SUtils.adjustLength("", 5);
		Assert.assertEquals(5, result.length());
		Assert.assertEquals("     ", result);
		
		result = SUtils.adjustLength("", 0);
		Assert.assertEquals(0, result.length());
		Assert.assertEquals("", result);
		
		result = SUtils.adjustLength("", 1);
		Assert.assertEquals(1, result.length());
		Assert.assertEquals(" ", result);
		
		result = SUtils.adjustLength("Hi Hi Hi", 0);
		Assert.assertEquals(0, result.length());
		Assert.assertEquals("", result);
		
		result = SUtils.adjustLength("Hi Hi Hi", 1);
		Assert.assertEquals(1, result.length());
		Assert.assertEquals("H", result);
		
		result = SUtils.adjustLength("Hi Hi Hi", 2);
		Assert.assertEquals(2, result.length());
		Assert.assertEquals("H.", result);
		
		result = SUtils.adjustLength("Hi Hi Hi", 3);
		Assert.assertEquals(3, result.length());
		Assert.assertEquals("H..", result);
		
		result = SUtils.adjustLength("Hi Hi Hi", 4);
		Assert.assertEquals(4, result.length());
		Assert.assertEquals("H...", result);
		
		result = SUtils.adjustLength("Hi Hi Hi", 5);
		Assert.assertEquals(5, result.length());
		Assert.assertEquals("Hi...", result);
		
		result = SUtils.adjustLength("Hi Hi Hi", 8);
		Assert.assertEquals(8, result.length());
		Assert.assertEquals("Hi Hi Hi", result);
	}
	
	@Test
	public void adjustLengthLeftAlignment() {
		String result = SUtils.adjustLength("12", 5, '0', false);
		Assert.assertEquals(5, result.length());
		Assert.assertEquals("00012", result);

		result = SUtils.adjustLength("12345", 4, '0', false);
		Assert.assertEquals(4, result.length());
		Assert.assertEquals("1...", result);
		
		result = SUtils.adjustLength("12345", 3, '0', false);
		Assert.assertEquals(3, result.length());
		Assert.assertEquals("1..", result);
		
		result = SUtils.adjustLength("12345", 2, '0', false);
		Assert.assertEquals(2, result.length());
		Assert.assertEquals("1.", result);
		
		result = SUtils.adjustLength("12345", 1, '0', false);
		Assert.assertEquals(1, result.length());
		Assert.assertEquals("1", result);
	}
	
}
