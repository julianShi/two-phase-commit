package distributedTransaction;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class TwoPhaseCommitService {

    public static void invoke(List<DistributedStatement> distributedStatements) {
        distributedStatements.forEach(distributedStatement -> {
            try {
                distributedStatement.conn.setAutoCommit(false);
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        });

        try {
            distributedStatements.forEach(distributedStatement -> {
                try {
                    Statement stmt = distributedStatement.conn.createStatement();
                    stmt.executeUpdate(distributedStatement.sql);
                } catch (SQLException throwables) {
                    throw new RuntimeException("Failed statement: " + distributedStatement.sql);
                }
            });
            distributedStatements.forEach(distributedStatement -> {
                try {
                    distributedStatement.conn.commit();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            });
        } catch (RuntimeException e) {
            e.printStackTrace();
        } finally {
            distributedStatements.forEach(distributedStatement -> {
                try {
                    distributedStatement.conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            });
        }
    }
}
