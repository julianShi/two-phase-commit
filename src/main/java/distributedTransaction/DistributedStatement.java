package distributedTransaction;

import java.sql.Connection;

public class DistributedStatement {
    public Connection conn;
    public String sql;
    public DistributedStatement(Connection c, String s) {
        conn = c;
        sql = s;
    }
}
