import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Main {

    /**
     * Get all records of devices table.
     **/
    public static Map<String, List<String>> getAllRecordsInMap(Connection conn) {
        String QUERY = "SELECT * from development.devices";
        Map<String, List<String>> mapAndroidIdToDeviceId = null;
        Statement stmt;
        ResultSet rs;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(QUERY);
            mapAndroidIdToDeviceId = new TreeMap<>();
            while (rs.next()) {
                String deviceId = rs.getString("device_id");
                String androidId = rs.getString("android_id");
                if (mapAndroidIdToDeviceId.containsKey(androidId)) {
                    mapAndroidIdToDeviceId.get(androidId).add(deviceId);
                } else {
                    List<String> deviceList = new ArrayList<>();
                    deviceList.add(deviceId);
                    mapAndroidIdToDeviceId.put(androidId, deviceList);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return mapAndroidIdToDeviceId;
    }

    /**
     * flag all duplicate record based on android_id except one
     **/

    public static void updateDuplicateRecord(Connection conn, Map<String, List<String>> mapAndroidIdToDeviceId) {
        PreparedStatement updateStmt;
        String updateSQL = "UPDATE development.devices SET is_deleted = true where device_id = ?";
        try {
            updateStmt = conn.prepareStatement(updateSQL);
            for (Map.Entry<String, List<String>> entry : mapAndroidIdToDeviceId.entrySet()) {
                List<String> deviceIdList = entry.getValue();
                if (deviceIdList.size() > 1) {
                    for (int index = 1; index < deviceIdList.size(); index++) {
                        updateStmt.setString(1, deviceIdList.get(index));
                        updateStmt.addBatch();
                        System.out.println(updateStmt);
                    }
                }
            }
            int[] count = updateStmt.executeBatch();
            System.out.println("Duplicate Record update : " + count.length);
            updateStmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        Connection connection = JDBCConnector.getConnection();
        Map<String, List<String>> mapAndroidIdToDeviceId = getAllRecordsInMap(connection);
        updateDuplicateRecord(connection, mapAndroidIdToDeviceId);
        JDBCConnector.close(connection);
    }
}
