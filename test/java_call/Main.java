import external.External;

public class Main {
    public static void main(String[] args) {
         External e = new External();
         e.hello();
         String s = e.concat("Hello,", "World!");
         System.out.println(s);
     }
}
