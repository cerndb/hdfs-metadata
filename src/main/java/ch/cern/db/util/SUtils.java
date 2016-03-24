package ch.cern.db.util;

/**
 * String utilities
 * @author daniellanzagarcia
 *
 */
public class SUtils {

	public static enum Color{
		BK(30),	//BLACK
		R(31), 	//RED
		G(32), 	//GREEN
		Y(33), 	//YELLOW
		BL(34), //BLUE
		P(35),	//PURPLE
		C(36),	//CYAN
		W(37);	//WHITE
		
		String ansi;
		
		Color(int code){
			ansi = "\u001B[" + code + "m";
		}
		
		@Override
		public String toString() {
			return ansi;
		}
	};
	
	public static final String ANSI_RESET = "\u001B[0m";
	
	public static String color(Color color, String string) {
		return color + string + ANSI_RESET;
	}
	
	public static String adjustLength(String inputString, int requiredLength) {
		if(requiredLength <= 0)
			return "";
		
		if(inputString.length() > requiredLength){
			String tmp = inputString.substring(0, requiredLength <= 3 ? 1 : requiredLength - 3);
			
			while(tmp.length() < requiredLength)
				tmp = tmp.concat(".");
			
			return tmp;
		}

		while(inputString.length() < requiredLength)
			inputString = inputString.concat(" ");
		
		return inputString;
	}
	
}
