package application;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Sean McKenna on 10/13/2017.
 */
public class Main {
    public static void main(String[] args) {
        String outputDir = "G:/finance/";

        Connection con = null;
        PreparedStatement pst = null;
        Statement st = null;
        ResultSet rs = null;
        FileWriter fw;

        String url = "jdbc:mysql://10.0.0.8:3306/rates"; //your own database
        String user = "user";
        String password = "password";

        String fileName = "EURUSD_M1";

        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            con = DriverManager.getConnection(url, user, password);
            st = con.createStatement();
            rs = st.executeQuery("SELECT VERSION(), CURRENT_DATE");

            if (rs.next()) System.out.println(rs.getString(1) + ", " + rs.getString(2));

            //select all data from table
            rs = st.executeQuery("select * from EURUSD_M1");
            //colunm count is necessay as the tables are dynamic and we need to figure out the numbers of columns
            int colunmCount = getColumnCount(rs);
            int numRows = getRowCount(rs);
            System.out.println(numRows+" rows...\n-------------------------------------------------|");
            //System.out.print(colunmCount+"------------------------------------------------|\n");
            //System.out.println(colunmCount+"|");
            int per= numRows/50;
            int tempPer = 0;

            try {
                fw = new FileWriter(outputDir+fileName+".csv");


                //this loop is used to add column names at the top of file , if you do not need it just comment this loop
                for(int i=1 ; i<= colunmCount ;i++)
                {
                    fw.append(rs.getMetaData().getColumnName(i));
                    fw.append(",");

                }

                fw.append(System.getProperty("line.separator"));

                while(rs.next())
                {
                    for(int i=1;i<=colunmCount;i++)
                    {

                        //you can update it here by using the column type but i am fine with the data so just converting
                        //everything to string first and then saving
                        if(rs.getObject(i)!=null)
                        {
                            String data= rs.getObject(i).toString();
                            fw.append(data) ;
                            fw.append(",");
                        }
                        else
                        {
                            String data= "null";
                            fw.append(data) ;
                            fw.append(",");
                        }

                    }
                    //new line entered after each row
                    if(tempPer > per){
                        tempPer = 0;
                        System.out.print("x");
                    }else{
                        tempPer +=1;
                    }
                    fw.append(System.getProperty("line.separator"));
                }

                fw.flush();
                fw.close();
                System.out.print("|DONE");
            } catch (IOException e) {
                e.printStackTrace();
            }
            con.close();
        }catch (SQLException ex) {

            Logger lgr = Logger.getLogger(Main.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } finally {

            try {
                if (pst != null) {
                    pst.close();
                }
                if (st != null) {
                    st.close();
                }
                if (con != null) {
                    con.close();
                }
            } catch (SQLException ex) {

                Logger lgr = Logger.getLogger(Main.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
    }

    //to get numbers of rows in a result set
    public static int  getRowCount(ResultSet res) throws SQLException
    {
        res.last();
        int numberOfRows = res.getRow();
        res.beforeFirst();
        return numberOfRows;
    }

    //to get no of columns in result set

    public static int  getColumnCount(ResultSet res) throws SQLException
    {
        return res.getMetaData().getColumnCount();
    }
}
