package distributedTransaction;

import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Arrays;

import org.junit.jupiter.api.Assertions;

public class TwoPhaseCommitTest {
    final private String DATABASE_SHARD1 = "jdbc:mysql://localhost:3306/shard1";
    final private String DATABASE_SHARD2 = "jdbc:mysql://localhost:3306/shard2";
    final private String USERNAME = "root";
    final private String PASSWORD = "";

    @Test
    public void standaloneTransaction() throws SQLException {
        // CREATE TABLE accounts (id int, balance int unsigned);
        // Init: user1 has 100, user2 has 100
        setBalanceOfUser1(100);
        setBalanceOfUser2(100);

        Connection conn1 = DriverManager.getConnection(DATABASE_SHARD1, USERNAME, PASSWORD);
        conn1.setAutoCommit(false);
        Statement stmt1  = conn1.createStatement();

        try {
            stmt1.executeUpdate( "update accounts set balance=balance+200 where id = 2");
            stmt1.executeUpdate( "update accounts set balance=balance-200 where id = 1");
            conn1.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            stmt1.close();
            conn1.close();
        }

        Assertions.assertEquals(100, getBalanceOfUser1());
        Assertions.assertEquals(100, getBalanceOfUser2());
    }

    @Test
    public void distributedTransactionFail() throws SQLException {
        // Init: user3 has 100, user4 has 100.
        // user3 and user4 are in different databases;
        setBalanceOfUser3(100);
        setBalanceOfUser4(100);

        TwoPhaseCommitService.invoke(Arrays.asList(
                new DistributedStatement(DriverManager.getConnection(DATABASE_SHARD2, USERNAME, PASSWORD),
                        "update accounts set balance=balance+200 where id = 4"),
                new DistributedStatement(DriverManager.getConnection(DATABASE_SHARD1, USERNAME, PASSWORD),
                        "update accounts set balance=balance-200 where id = 3")
        ));

        Assertions.assertEquals(100, getBalanceOfUser3());
        Assertions.assertEquals(100, getBalanceOfUser4());
    }

    @Test
    public void distributedTransactionPass() throws SQLException {
        // Init: user3 has 100, user4 has 100.
        // user3 and user4 are in different databases;
        setBalanceOfUser3(100);
        setBalanceOfUser4(100);

        Connection conn1 = DriverManager.getConnection(DATABASE_SHARD1, USERNAME, PASSWORD);
        conn1.setAutoCommit(false);
        Statement stmt1 = conn1.createStatement();
        Connection conn2 = DriverManager.getConnection(DATABASE_SHARD2, USERNAME, PASSWORD);
        conn2.setAutoCommit(false);
        Statement stmt2 = conn2.createStatement();

        try {
            stmt2.executeUpdate( "update accounts set balance=balance+200 where id = 4");
            stmt1.executeUpdate( "update accounts set balance=balance-200 where id = 3");
            conn2.commit();
            conn1.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            stmt1.close();
            conn1.close();
            stmt2.close();
            conn2.close();
        }

        Assertions.assertEquals(100, getBalanceOfUser3());
        Assertions.assertEquals(100, getBalanceOfUser4());
    }

    private void setBalanceOfUser1(int balance) throws SQLException {
        Connection conn = DriverManager.getConnection(DATABASE_SHARD1, USERNAME, PASSWORD);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("update accounts set balance=100 where id = 1");
    }
    private void setBalanceOfUser2(int balance) throws SQLException {
        Connection conn = DriverManager.getConnection(DATABASE_SHARD1, USERNAME, PASSWORD);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("update accounts set balance=100 where id = 2");
    }
    private void setBalanceOfUser3(int balance) throws SQLException {
        Connection conn = DriverManager.getConnection(DATABASE_SHARD1, USERNAME, PASSWORD);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("update accounts set balance=100 where id = 3");
    }
    private void setBalanceOfUser4(int balance) throws SQLException {
        Connection conn = DriverManager.getConnection(DATABASE_SHARD2, USERNAME, PASSWORD);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("update accounts set balance=100 where id = 4");
    }
    private int getBalanceOfUser1() throws SQLException {
        Connection conn = DriverManager.getConnection(DATABASE_SHARD1, USERNAME, PASSWORD);
        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery("select balance from accounts where id=1");
        res.next();
        return res.getInt(1);
    }
    private int getBalanceOfUser2() throws SQLException {
        Connection conn = DriverManager.getConnection(DATABASE_SHARD1, USERNAME, PASSWORD);
        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery("select balance from accounts where id=2");
        res.next();
        return res.getInt(1);
    }
    private int getBalanceOfUser3() throws SQLException {
        Connection conn = DriverManager.getConnection(DATABASE_SHARD1, USERNAME, PASSWORD);
        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery("select balance from accounts where id=3");
        res.next();
        return res.getInt(1);
    }
    private int getBalanceOfUser4() throws SQLException {
        Connection conn = DriverManager.getConnection(DATABASE_SHARD2, USERNAME, PASSWORD);
        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery("select balance from accounts where id=4");
        res.next();
        return res.getInt(1);
    }
}