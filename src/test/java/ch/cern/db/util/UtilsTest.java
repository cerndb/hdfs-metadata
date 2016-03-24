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
