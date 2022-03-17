import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCConnector {
    public static final String DB_URL = "XX";
    public static final String USER = "XX";
    public static final String PASS = "XX";

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
