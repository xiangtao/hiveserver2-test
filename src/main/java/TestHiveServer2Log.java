import org.apache.hive.jdbc.HiveStatement;
import org.junit.Test;

import java.sql.*;
import java.util.List;

/**
 * Created by taox on 17/4/16.
 */
public class TestHiveServer2Log {


    @Test
    public void testHiveServer2JdbcLog2() throws SQLException, ClassNotFoundException {
        Thread logThread = null;
        PreparedStatement stmnt = null;
        try {
            try {
                long start = System.currentTimeMillis();

                Connection conn = getConn();
                String sql = "select count(1) from data_sum.sum_user_uuid_play_day where dt='20170415' and product='tv'";
                stmnt = conn.prepareStatement(sql);
                logThread = new Thread(createLogRunnable(stmnt));
                logThread.setDaemon(true);
                logThread.start();
                ResultSet rs = stmnt.executeQuery();
                logThread.interrupt();
                logThread.join(10 * 1000);
                    try {
                        System.out.println(rs);
                        int count = rs.getRow();
                        long end = System.currentTimeMillis();

                        System.out.print("rows-selected" + count + " "
                                + (end - start));
                    } finally {
                        if (logThread != null) {
                            logThread.join(10 * 1000);
                            showRemainingLogsIfAny(stmnt);
                            logThread = null;
                        }
                        rs.close();
                    }
            } catch (Exception e) {
                System.out.println(e);

            } finally {
                if (logThread != null) {
                    if (!logThread.isInterrupted()) {
                        logThread.interrupt();
                    }
                    logThread.join(10 * 1000);
                    showRemainingLogsIfAny(stmnt);
                }
                if (stmnt != null) {
                    stmnt.close();
                }
            }
        }catch (Exception e){
            System.out.println(e);
        }
    }


    @Test
    public void testHiveServer2JdbcLog() throws SQLException, ClassNotFoundException {
        Thread logThread = null;
        Statement stmnt = null;
        try {
            try {
                long start = System.currentTimeMillis();

                Connection conn = getConn();
                String sql = "select count(1) from data_raw.tbl_play_hour where dt='20170414' and product='0'";
                stmnt = conn.createStatement();
                logThread = new Thread(createLogRunnable(stmnt));
                logThread.setDaemon(true);
                logThread.start();
                boolean hasResults = stmnt.execute(sql);
                logThread.interrupt();
                logThread.join(10 * 1000);

                if (hasResults) {
                    ResultSet rs = stmnt.getResultSet();
                    try {
                        System.out.println(rs);
                        int count = rs.getRow();
                        long end = System.currentTimeMillis();

                        System.out.print("rows-selected" + count + " "
                                + (end - start));
                    } finally {
                        if (logThread != null) {
                            logThread.join(10 * 1000);
                            showRemainingLogsIfAny(stmnt);
                            logThread = null;
                        }
                        rs.close();
                    }
                } else {
                    int count = stmnt.getUpdateCount();
                    long end = System.currentTimeMillis();
                    System.out.print("rows-selected" + count + " "
                            + (end - start));
                }

            } catch (Exception e) {
                System.out.println(e);

            } finally {
                if (logThread != null) {
                    if (!logThread.isInterrupted()) {
                        logThread.interrupt();
                    }
                    logThread.join(10 * 1000);
                    showRemainingLogsIfAny(stmnt);
                }
                if (stmnt != null) {
                    stmnt.close();
                }
            }
        }catch (Exception e){
            System.out.println(e);
        }
    }



    public Connection getConn() throws ClassNotFoundException, SQLException {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Connection conn= DriverManager.getConnection("jdbc:hive2://10.149.13.80:10000/temp", "ide_select", "!@wdiom9A");
            return conn;
    }



    private Runnable createLogRunnable(Statement statement) {
        if (statement instanceof HiveStatement) {
            final HiveStatement hiveStatement = (HiveStatement) statement;

            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    while (hiveStatement.hasMoreLogs()) {
                        try {
                            // fetch the log periodically and output to beeline console
                            for (String log : hiveStatement.getQueryLog()) {
                                System.out.println(log);
                            }
                            Thread.sleep(1000);
                        } catch (SQLException e) {
                            System.out.println(new SQLWarning(e));
                            return;
                        } catch (InterruptedException e) {
                            System.out.println("Getting log thread is interrupted, since query is done!");
                            showRemainingLogsIfAny(hiveStatement);
                            return;
                        }
                    }
                }
            };
            return runnable;
        } else {
            System.out.println("The statement instance is not HiveStatement type: " + statement.getClass());
            return new Runnable() {
                @Override
                public void run() {
                    // do nothing.
                }
            };
        }
    }

    private void showRemainingLogsIfAny(Statement statement) {
        if (statement instanceof HiveStatement) {
            HiveStatement hiveStatement = (HiveStatement) statement;
            List<String> logs;
            do {
                try {
                    logs = hiveStatement.getQueryLog();
                } catch (SQLException e) {
                    System.out.println(new SQLWarning(e));
                    return;
                }
                for (String log : logs) {
                    System.out.println(log);
                }
            } while (logs.size() > 0);
        } else {
            System.out.println("The statement instance is not HiveStatement type: " + statement.getClass());
        }
    }


}
