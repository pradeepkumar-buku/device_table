import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class JDBCConnector {

    public static  String DB_URL ;
    public static  String USER ;
    public static  String PASS;

    public static Connection getConnection()  {
        Connection  connection = null;
        try {
            connection = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void close(Connection connection) {
        try {
           connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
