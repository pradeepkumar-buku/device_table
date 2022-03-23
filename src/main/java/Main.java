import java.sql.*;
import java.util.*;
import java.util.stream.IntStream;

public class Main {

    /**
     * Get all records of devices table.
     **/
    public static Map<String, List<String>> getAllRecordsInMap(Connection conn) {
        System.out.println("[INFO] Inside gellAllRecordMap method");
        String QUERY = "select d1.device_id, d1.android_id\n" +
                "from public.devices as d1\n" +
                "inner join \n" +
                "(select d2.android_id,count(d2.android_id)\n" +
                "from public.devices d2\n" +
                "group by d2.android_id\n" +
                "having count(d2.android_id)>1) d3\n" +
                "on d3.android_id = d1.android_id;";
        Map<String, List<String>> mapAndroidIdToDeviceId = null;
        Statement stmt;
        ResultSet rs;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(QUERY);
            mapAndroidIdToDeviceId = new TreeMap<>();
            int countRecords = 0;
            int countDuplicateRecords = 0;
            while (rs.next()) {
                String deviceId = rs.getString("device_id");
                String androidId = rs.getString("android_id");
                if (mapAndroidIdToDeviceId.containsKey(androidId)) {
                    mapAndroidIdToDeviceId.get(androidId).add(deviceId);
                    countDuplicateRecords++;
                } else {
                    List<String> deviceList = new ArrayList<>();
                    deviceList.add(deviceId);
                    mapAndroidIdToDeviceId.put(androidId, deviceList);
                }
                countRecords++;
            }

            System.out.println("[INFO] Total Records =" + countRecords);
            System.out.println("[INFO] Total Duplucate Records = " + countDuplicateRecords);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("[INFO] Exiting from gellAllRecordMap method");
        return mapAndroidIdToDeviceId;
    }

    /**
     * flag all duplicate record based on android_id except one
     **/

    public static void updateDuplicateRecord(Connection conn, Map<String, List<String>> mapAndroidIdToDeviceId) {
        System.out.println("[INFO] Inside updateDuplicateRecord method");
        PreparedStatement updateStmt;
        String updateSQL = "UPDATE public.devices SET is_deleted = 'true' where device_id = ? and (is_deleted is null or is_deleted = 'false')";
        try {
            updateStmt = conn.prepareStatement(updateSQL);
            for (Map.Entry<String, List<String>> entry : mapAndroidIdToDeviceId.entrySet()) {
                List<String> deviceIdList = entry.getValue();
                if (deviceIdList.size() > 1) {
                    Collections.sort(deviceIdList);
                    for (int index = 1; index < deviceIdList.size(); index++) {
                        updateStmt.setString(1, deviceIdList.get(index));
                        updateStmt.addBatch();
                    }
                }
            }
            System.out.println("[INFO] Executing batch update");
            int[] count = updateStmt.executeBatch();
            System.out.println("[INFO] Duplicate Record update : " + IntStream.of(count).sum());
            updateStmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("[INFO] Exiting updateDuplicateRecord method");
    }

    public static void main(String[] args) {
        try {
            System.out.println("[INFO] Inside main");
            JDBCConnector.DB_URL = args[0];
            JDBCConnector.USER = args[1];
            JDBCConnector.PASS = args[2];
            if (args.length == 3) {
                Connection connection = JDBCConnector.getConnection();
                if (connection != null) {
                    Map<String, List<String>> mapAndroidIdToDeviceId = getAllRecordsInMap(connection);
                    updateDuplicateRecord(connection, mapAndroidIdToDeviceId);
                }
                JDBCConnector.close(connection);
            } else {
                System.out.println("Please provide valid url, user, password");
            }
        } catch (Exception e) {
            System.out.println("Ended with Exceptions");
            e.printStackTrace();
        }
    }
}

