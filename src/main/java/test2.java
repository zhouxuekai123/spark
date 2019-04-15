import java.math.BigDecimal;

public class test2 {

    public  static double sub(double v1,double v2){
        BigDecimal b1 = new BigDecimal(Double.toString(v1));
        BigDecimal b2 = new BigDecimal(Double.toString(v2));
        return  b1.subtract(b2).doubleValue();
    }
    public static void main(String[] args) {
        double a=10.222222225;
        double b=10.222222229;
        System.out.println(sub(a,b));
        System.out.println(Math.abs(a-b)>0);
    }
}
