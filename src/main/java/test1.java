public class test1 {
    public static void main(String[] args) {
        String a="abcd";
        char[] chars = a.toCharArray();
        String b="";
        for(int i=0;i<=chars.length-1;i++){
            if(i%2==1){
                b+=chars[i];
            }
        }
        System.out.println(b);
    }
}
