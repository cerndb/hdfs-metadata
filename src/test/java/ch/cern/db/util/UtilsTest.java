/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.util;

import java.math.BigInteger;

import org.junit.Assert;
import org.junit.Test;


public class UtilsTest {

	public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;
	
	@Test
	public void toInt() {
		try{
			Utils.toInt(new byte[]{1});
			
			Assert.fail();
		}catch(IllegalArgumentException e){}
		
		int test = 1695609641;
		Assert.assertEquals(test, Utils.toInt(BigInteger.valueOf(test).toByteArray()));
		
		test = -12454334;
		Assert.assertEquals(test, Utils.toInt(BigInteger.valueOf(test).toByteArray()));
	}

}
