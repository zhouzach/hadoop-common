package hbase;

public class RowRange {
    public static void main(String[] args){
        if("!".compareTo( "0") < 0){
            System.out.println("! < 0");
        }
        if("0".compareTo( "438cbdad-cfbd-4d25-abbc-ae3d92b75534") < 0){
            System.out.println("0 < uuid");
        }
        if("~".compareTo( "438cbdad-cfbd-4d25-abbc-ae3d92b75534") > 0){
            System.out.println("~ > uuid");
        }
    }
}
