/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.util;

public class Utils {

	public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;
	
	/**
	 * Converts a byte array to an int value
	 * 
	 * @param bytes
	 *            byte array
	 * @return the int value
	 * @throws IllegalArgumentException
	 *             if length is not {@link #SIZEOF_INT}
	 */
	public static int toInt(byte[] bytes) {
		if (SIZEOF_INT > bytes.length)
			throw new IllegalArgumentException("length is not SIZEOF_INT");
		
		int n = 0;
		for (int i = 0; i < + bytes.length; i++) {
			n <<= 8;
			n ^= bytes[i] & 0xFF;
		}
		return n;
	}

}
