package CSCI485ClassProject;

public class Utils {

//  public static void printByteArray(byte[] bytes) {
//    String hexString = String.format("%02X", bytes[0]);
//    for (int i = 1; i < bytes.length; i++) {
//      hexString += String.format(" %02X", bytes[i]);
//    }
//    System.out.println(hexString);
//  }

  public static String byteArray2String(byte[] bytes) {
    String hexString = String.format("%02X", bytes[0]);
    for (int i = 1; i < bytes.length; i++) {
      hexString += String.format(" %02X", bytes[i]);
    }
    return hexString;
  }
}
