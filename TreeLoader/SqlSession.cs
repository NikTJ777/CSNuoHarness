using System;
using System.Threading;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NuoDb.Data.Client;


namespace NuoTest
{
    class SqlSession : IDisposable
    {
        private Mode mode;
        private Mode commitMode;

        private DbConnection connection;
        private DbTransaction transaction;
        private List<DataRow> batch;
        private List<DbCommand> statements;

        //private static DataSource dataSource;
        private static DbProviderFactory dataSource;
        private static String updateConnectionString;
        private static String queryConnectionString;

        private static ThreadLocal<SqlSession> current = new ThreadLocal<SqlSession>();
        private static Dictionary<SqlSession, String> sessions;
        private static IsolationLevel updateIsolation;

        private static readonly String DBDRIVER = "NuoDB.Data.Client";

        private static Logger log = Logger.getLogger("SqlSession");

        //private SqlSession(Mode mode) {
        //    this.mode = mode;
        //}

        public enum Mode { AUTO_COMMIT, TRANSACTIONAL, BATCH, READ_ONLY };

        public static void init(Dictionary<String, String> properties, int maxThreads)
        {
            sessions = new Dictionary<SqlSession, String>();

            dataSource = DbProviderFactories.GetFactory(properties["dotnet.driver"]);

            DbConnectionStringBuilder connectionStringBuilder = dataSource.CreateConnectionStringBuilder();

            connectionStringBuilder.Add("User", properties["user"]);
            connectionStringBuilder.Add("Password", properties["password"]);
            connectionStringBuilder.Add("Server", properties["url.server"]);
            connectionStringBuilder.Add("Database", properties["url.database"]);
            connectionStringBuilder.Add("Schema", properties["defaultSchema"]);
            connectionStringBuilder.Add("Pooling", "True");
            connectionStringBuilder.Add("MaxLifetime", properties["maxAge"]);

            if (maxThreads > 100)
            {
                connectionStringBuilder.Add("MaxConnections", String.Format("{0}", maxThreads));
            }

            // process any options 
            String options = properties["url.options"];
            if (options.StartsWith("?")) options = options.Substring(1);
            String[] optlist = options.Split("&".ToCharArray());
            foreach (String opt in optlist)
            {
                String[] keyval = opt.Split("=".ToCharArray());
                if (keyval.Length == 2) connectionStringBuilder.Add(keyval[0], keyval[1]);
                else if (keyval.Length == 1) connectionStringBuilder.Add(keyval[0], "true");
            }

            String isolation = properties["default.isolation"];
            if (isolation == null || isolation.Length == 0) isolation = "CONSISTENT_READ";
            switch (isolation)
            {
                case "READ_COMMITTED":
                    //updateIsolation = Connection.TRANSACTION_READ_COMMITTED;
                    updateIsolation = IsolationLevel.ReadCommitted;
                    break;

                case "SERIALIZABLE":
                    //updateIsolation = Connection.TRANSACTION_SERIALIZABLE;
                    updateIsolation = IsolationLevel.Serializable;
                    break;

                case "CONSISTENT_READ":
                    //updateIsolation = TransactionIsolation.TRANSACTION_CONSISTENT_READ;
                    updateIsolation = IsolationLevel.Unspecified;
                    break;

                case "WRITE_COMMITTED":
                    //updateIsolation = TransactionIsolation.TRANSACTION_WRITE_COMMITTED;
                    updateIsolation = IsolationLevel.Unspecified;
                    break;
            }

            updateConnectionString = connectionStringBuilder.ConnectionString;

            // make this one READ_ONLY
            queryConnectionString = connectionStringBuilder.ConnectionString;
        }

        public SqlSession(Mode mode)
        {
            this.mode = mode;
            commitMode = (mode == Mode.AUTO_COMMIT || mode == Mode.READ_ONLY ? Mode.AUTO_COMMIT : Mode.TRANSACTIONAL);

            if (mode == Mode.BATCH && batch == null)
            {
                batch = new List<DataRow>();
            }

            SqlSession session = current.Value;
            if (session != null)
            {
                session.Dispose();
                throw new PersistenceException("Previous session for this thread was not correctly closed");
            }

            //session = new SqlSession(mode);
            current.Value = this;
            sessions.Add(this, Thread.CurrentThread.Name);

            //return session;
        }

        public static void cleanup()
        {
            if (sessions == null || sessions.Count == 0)
                return;

            int released = 0;
            foreach (KeyValuePair<SqlSession, String> entry in sessions)
            {
                log.info(String.Format("cleaning up unclosed session from {0}", entry.Value));
                entry.Key.Dispose();
                released++;
            }

            throw new PersistenceException("{0} unclosed SqlSessions were cleaned up", released);
        }

        public static SqlSession getCurrent()
        {
            SqlSession session = current.Value;
            if (session == null)
                throw new PersistenceException("No current session");

            return session;
        }

        public void rollback()
        {
            if (transaction != null && commitMode != Mode.AUTO_COMMIT)
            {
                try { transaction.Rollback(); }
                catch (Exception e) { }
            }
        }

        public void Dispose()
        {
            closeStatements();
            closeConnection();
            current.Value = null;
            sessions.Remove(this);
        }

        public DbCommand getStatement(String sql)
        {
            if (mode == Mode.BATCH && batch != null && batch.Count > 0)
            {
                throw new PersistenceException("getStatement called in BATCH MODE");
                //batch.Clear();
                //return batch;
            }

            if (statements == null)
            {
                statements = new List<DbCommand>(16);
                //statements = new HashMap<String, DbCommand>(16);
            }

            //DbCommand ps = statements.get(sql);

            //if (ps == null) {
            //int returnMode = (mode == Mode.AUTO_COMMIT ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
            //int returnMode = Statement.RETURN_GENERATED_KEYS;
            //DbCommand ps = connection().prepareStatement(sql);
            DbCommand cmd = Connection().CreateCommand();
            cmd.CommandText = sql;
            statements.Add(cmd);
            //statements.put(sql, ps);
            //} else {
            //    ps.clearParameters();
            //}

            //batch = (mode == Mode.BATCH ? ps : null);

            return cmd;
        }

        public void execute(String script)
        {
            if (script == null || script.Length == 0) return;

            String[] lines = script.Split("@".ToCharArray());

            using (DbTransaction transaction = Connection().BeginTransaction())
            {
                using (DbCommand command = Connection().CreateCommand())
                {
                    foreach (String line in lines)
                    {
                        command.CommandText = line.Trim();
                        command.ExecuteNonQuery();
                    }
                    transaction.Commit();
                }
            }

            /*
            String command = "";
            try (Statement sql = connection().createStatement()) {
                assert sql != null;

                for (String line : lines) {
                    command = line.trim();
                    System.out.println(String.Format("executing statement {0}", command));
                    sql.execute(command);
                }

                System.out.println("commiting...");
                connection().commit();
            } catch (SQLException e) {
                throw new PersistenceException(e, "Error executing SQL: {0}", command);
            }
            */
        }

        /**
         * This method signature is a bit ugly.
         * This entire class should be refactored to use closures.
         */
        public long update(DataRow row, String sql)
        {
            if (mode == Mode.BATCH)
            {
                batch.Add(row);
            }
            else
            {
                using (DbCommand cmd = getStatement(sql))
                {
                    foreach (Object o in row.ItemArray)
                    {
                        cmd.Parameters.Add(o);
                    }

                    return update(cmd);
                }
            }

            return 0;
        }

        public long update(DbCommand update)
        {
            return (long)update.ExecuteScalar();
        }

        protected DbConnection Connection()
        {
            if (connection == null)
            {
                connection = dataSource.CreateConnection();
                connection.ConnectionString = (mode == Mode.READ_ONLY ? queryConnectionString : updateConnectionString);

                /*
                switch (mode)
                {
                    case READ_ONLY:
                        connection.setReadOnly(true);
                        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                        connection.setAutoCommit(true);
                        break;

                    case AUTO_COMMIT:
                        connection.setReadOnly(false);
                        connection.setAutoCommit(true);
                        connection.setTransactionIsolation(updateIsolation);
                        break;
                    default:
                        connection.setReadOnly(false);
                        connection.setAutoCommit(false);
                        connection.setTransactionIsolation(updateIsolation);
                }
                 */

                //connection.ConnectionString = connectionString;
                connection.Open();
            }

            //assert (connection != null);

            return connection;
        }

        protected void closeStatements()
        {
            if (batch != null)
            {
                try
                {
                    NuoDbBulkLoader loader = new NuoDbBulkLoader(updateConnectionString);
                    loader.WriteToServer(batch.ToArray());
                }
                catch (Exception e) { }
                batch.Clear();
            }

            if (statements == null) return;

            foreach (DbCommand ps in statements)
            {
                //for (DbCommand ps : statements.values()) {
                try { ps.Dispose(); }
                catch (Exception e) { }
            }

            statements.Clear();
        }

        protected void closeConnection()
        {
            if (connection != null)
            {
                if (transaction != null && commitMode != Mode.AUTO_COMMIT)
                {
                    try { transaction.Commit(); }
                    catch (/*SQL*/Exception e)
                    {
                        throw new PersistenceException(e, "Error commiting connection");
                    }
                }

                try { connection.Dispose(); }
                catch (Exception e) { }

                connection = null;
            }
        }
    }
}

