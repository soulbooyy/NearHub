package com.hmdp.order;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/** Child JVM intentionally exits without closing its transaction; only used with hmdp_order_test. */
public final class CrashBeforeCommitProbe {
    public static void main(String[] args) throws Exception {
        String url = System.getenv("HMDP_TEST_JDBC_URL");
        if (url == null || !url.contains("/hmdp_order_test?")) {
            throw new IllegalArgumentException("Only the isolated test database is allowed");
        }
        Connection connection = DriverManager.getConnection(url,
                System.getenv("MYSQL_USER"), System.getenv("MYSQL_PASSWORD"));
        connection.setAutoCommit(false);
        if (args[0].startsWith("cancel-")) {
            cancelAndCrash(connection, args);
            return;
        }
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE tb_seckill_voucher SET stock=stock-1 WHERE voucher_id=? AND stock>0")) {
            update.setLong(1, Long.parseLong(args[1]));
            if (update.executeUpdate() != 1) throw new IllegalStateException("No stock");
        }
        if (!"after-stock".equals(args[0])) {
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO tb_voucher_order(id,user_id,voucher_id,status,create_time) VALUES (?,?,?,1,CURRENT_TIMESTAMP)")) {
                insert.setLong(1, Long.parseLong(args[3]));
                insert.setLong(2, Long.parseLong(args[2]));
                insert.setLong(3, Long.parseLong(args[1]));
                insert.executeUpdate();
            }
        }
        if ("after-commit".equals(args[0])) connection.commit();
        // No Java finally blocks or JDBC close/rollback: simulate process loss at this exact boundary.
        Runtime.getRuntime().halt(23);
    }
    private static void cancelAndCrash(Connection connection, String[] args) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE tb_voucher_order SET status=4 WHERE id=? AND status=1 AND create_time<=DATE_SUB(CURRENT_TIMESTAMP,INTERVAL 15 MINUTE)")) {
            statement.setLong(1, Long.parseLong(args[3]));
            if (statement.executeUpdate() != 1) throw new IllegalStateException("No expired order");
        }
        if ("cancel-after-cas".equals(args[0])) Runtime.getRuntime().halt(23);
        try (PreparedStatement statement = connection.prepareStatement("UPDATE tb_seckill_voucher SET stock=stock+1 WHERE voucher_id=?")) {
            statement.setLong(1, Long.parseLong(args[1])); statement.executeUpdate();
        }
        if ("cancel-after-stock".equals(args[0])) Runtime.getRuntime().halt(23);
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO tb_order_stock_compensation(order_id,voucher_id,user_id) VALUES (?,?,?)")) {
            statement.setLong(1, Long.parseLong(args[3])); statement.setLong(2, Long.parseLong(args[1]));
            statement.setLong(3, Long.parseLong(args[2])); statement.executeUpdate();
        }
        if ("cancel-after-commit".equals(args[0])) connection.commit();
        Runtime.getRuntime().halt(23);
    }
}
