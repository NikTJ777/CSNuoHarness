using System;
using System.Threading;
using System.Collections.Generic;
using System.Collections.Concurrent;
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
        private readonly Mode mode;
        private readonly Mode commitMode;
        private readonly SqlSession parent;
        internal readonly CommunicationMode commsMode;

        private DbConnection connection;
        private DbTransaction transaction;
        private List<DataRow> batch;
        internal Dictionary<String, DataTable> BatchTable { get; set; }
        private Dictionary<String, DbCommand> statements;

        //private static DataSource dataSource;
        private static DbProviderFactory dataSource;
        private static String updateConnectionString;
        private static String queryConnectionString;

        private static ThreadLocal<SqlSession> current = new ThreadLocal<SqlSession>();
        private static ConcurrentDictionary<SqlSession, String> sessions;
        private static IsolationLevel updateIsolation;
        internal static CommunicationMode globalCommsMode { get; set; }
        internal static String SpNamePrefix { get; set; }

        private static int lowestRetriableErrorCode = 40000;
        private static int highestRetriableErrorCode = 40999;

        //private static readonly String DBDRIVER = "NuoDB.Data.Client";

        private static Logger log = Logger.getLogger("SqlSession");

        //private SqlSession(Mode mode) {
        //    this.mode = mode;
        //}

        public enum Mode { AUTO_COMMIT, TRANSACTIONAL, BATCH, READ_ONLY };
        public enum CommunicationMode { SQL, CALL, STORED_PROCEDURE };

        public static void init(Dictionary<String, String> properties, int maxThreads)
        {
            sessions = new ConcurrentDictionary<SqlSession, String>(maxThreads, maxThreads);

            // don't use the NuoDb provider that is installed on the system, use the one we
            // pick from the NuGet repository
            //dataSource = DbProviderFactories.GetFactory(properties["dotnet.driver"]);
            dataSource = new NuoDbProviderFactory();

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
            connectionStringBuilder.Add("IsolationLevel", "ReadCommitted"); 
            queryConnectionString = connectionStringBuilder.ConnectionString;
        }

        public static long activeSessions()
        {
            return sessions.Count;
        }

        public SqlSession(Mode mode)
        {
            this.mode = mode;
            //commitMode = (mode == Mode.AUTO_COMMIT || mode == Mode.READ_ONLY ? Mode.AUTO_COMMIT : Mode.TRANSACTIONAL);
            commitMode = (mode == Mode.TRANSACTIONAL ? mode : Mode.AUTO_COMMIT);
            commsMode = (mode == Mode.BATCH ? CommunicationMode.SQL : globalCommsMode);

            if (mode == Mode.BATCH && batch == null)
            {
                batch = new List<DataRow>();
            }

            BatchTable = new Dictionary<String, DataTable>(8);

            // if there is an existing session, then make it our parent
            parent = current.Value;

            //session = new SqlSession(mode);
            current.Value = this;
            sessions.TryAdd(this, Thread.CurrentThread.Name);

            //return session;
        }

        public static void cleanup()
        {
            if (sessions == null || sessions.Count == 0)
                return;

            int released = 0;
            foreach (KeyValuePair<SqlSession, String> entry in sessions)
            {
                log.info("cleaning up unclosed session from {0}", entry.Value);
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
            // only rollback our own tx - parent is a larger scope

            if (transaction != null && commitMode == Mode.TRANSACTIONAL)
            {
                try {
                    transaction.Rollback();
                    transaction.Dispose();
                } catch (Exception) { }
            }

            transaction = null;
        }

        /**
         * something fatal has happened in this session,
         * so release any dead/invalid resources, and be ready to try again.
         * 
         * Does NOT close (commit) resources.
         * The current SqlSession is left active.
         */
        public bool retry(Exception e)
        {
            rollback();
            releaseResources();

            // return true if the operation could be retried
            if (e is NuoDbSqlException) {
                int code = (e as NuoDbSqlException).Code.Code;
                if (code >= lowestRetriableErrorCode && code <= highestRetriableErrorCode)
                    return true;
            }

            return false;
        }

        public void Dispose()
        {
            // make my parent active again (or nothing acive if we are top-level)
            current.Value = parent;

            String name;
            sessions.TryRemove(this, out name);

            try {
                closeResources();
            }
            catch (Exception e) {
                throw new PersistenceException(e, "Error closing session");
            }
            finally {
                releaseResources();
            }
        }

        public DbCommand getStatement(String sql)
        {
            if (mode == Mode.BATCH /* && batch != null && batch.Count > 0 */)
            {
                throw new PersistenceException("getStatement called in BATCH MODE");
                //batch.Clear();
                //return batch;
            }

            // delegate to parent if we have one => caching to happen at 1 level
            if (parent != null)
                return parent.getStatement(sql);

            if (statements == null)
            {
                statements = new Dictionary<String, DbCommand>();
            }

            DbCommand cmd;
            if (!statements.TryGetValue(sql, out cmd))
            {
                //if (ps == null) {
                //int returnMode = (mode == Mode.AUTO_COMMIT ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
                //int returnMode = Statement.RETURN_GENERATED_KEYS;
                //DbCommand ps = connection().prepareStatement(sql);
                cmd = Connection().CreateCommand();
                if (commsMode == CommunicationMode.STORED_PROCEDURE) {
                    cmd.CommandType = CommandType.StoredProcedure;
                }

                cmd.CommandText = sql;
                cmd.Prepare();
                statements[sql] = cmd;
            } else {
                foreach (DbParameter p in cmd.Parameters)
                    p.Value = null;
            }

            //batch = (mode == Mode.BATCH ? ps : null);

            return cmd;
        }

        public void execute(String script)
        {
            if (script == null || script.Length == 0) return;

            String[] lines = script.Split(";".ToCharArray());

            using (DbTransaction transaction = Connection().BeginTransaction())
            {
                using (DbCommand command = Connection().CreateCommand())
                {
                    bool multiLine = false;
                    foreach (String line in lines)
                    {
                        String statement = line.Trim();
                        //Console.WriteLine("statement=" + statement);

                        // skip "GO" commands
                        if (statement.Equals("GO", StringComparison.CurrentCultureIgnoreCase))
                        {
                            multiLine = false;
                            continue;
                        }

                        // assemble multi-statement commands
                        else if (statement.StartsWith("CREATE PROCEDURE ", StringComparison.CurrentCultureIgnoreCase))
                            multiLine = true;

                        else if (statement.Equals("END_PROCEDURE", StringComparison.CurrentCultureIgnoreCase))
                            multiLine = false;

                        if (statement != null)
                            command.CommandText = (command.CommandText.Length == 0 ? statement : command.CommandText + ";\n" + statement);

                        if (multiLine)
                            continue;

                        Console.WriteLine("executing statement {0}", command.CommandText);
                        command.ExecuteNonQuery();
                        command.CommandText = "";
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
                    int offset = 0;
                    int initialCount = cmd.Parameters.Count;
                    if (commsMode == CommunicationMode.STORED_PROCEDURE || commsMode == CommunicationMode.CALL)
                    {
                        //if (cmd.Parameters.Count == 0)
                        //{
                        //    cmd.Parameters.Add(cmd.CreateParameter());
                        //    cmd.Parameters[0].ParameterName = "$id";
                        //    cmd.Parameters[0].Direction = ParameterDirection.InputOutput;
                        //}

                        cmd.Parameters[0].Value = new Int64();
                        offset = 1;
                    }

                    for (int index = 0; index < row.ItemArray.Length; index++)
                    {
                        //if (cmd.Parameters.Count <= index + offset) cmd.Parameters.Add(cmd.CreateParameter());
                        cmd.Parameters[index + offset].Value = row.ItemArray[index];
                    }

                    //log.info("before Prepare() - param count={0}", cmd.Parameters.Count);
                    //for (int x = 0; x < cmd.Parameters.Count; x++) log.info("param[{0}]={1}", x, cmd.Parameters[x].Value);

                    //if (cmd.Parameters.Count > initialCount)
                    //    cmd.Prepare();

                    //log.info("after Prepare() param count={0}", cmd.Parameters.Count);
                    //for (int x = 0; x < cmd.Parameters.Count; x++) log.info("param[{0}]={1}", x, cmd.Parameters[x].Value);
                    
                    return update(cmd);
                }
            }

            return 0;
        }

        public long update(DbCommand update)
        {
            if (commsMode == CommunicationMode.SQL)
            {
                //return (long)update.ExecuteScalar();
                //return (long)update.ExecuteNonQuery();
                using (DbDataReader keys = update.ExecuteReader())
                {
                    return (keys.Read() ? keys.GetInt64(0) : 0);
                }
            }
            else
            {
                //Console.WriteLine("before SP call param[0]=" + update.Parameters[0].Value);
                //for (int x = 0; x < update.Parameters.Count; x++) log.info("param[{0}] = {1}", x, update.Parameters[x].Value);
                update.ExecuteNonQuery();
                //Console.WriteLine("after SP call param[0]=" + update.Parameters[0].Value);
                //for (int x = 0; x < update.Parameters.Count; x++) log.info("returned param[{0}] = {1}", x, update.Parameters[x].Value);

                //log.info("returned params have {0} elements", update.Parameters.Count);
                //for (int x = 0; x < update.Parameters.Count; x++) log.info("returned param[{0}] = {1}", x, update.Parameters[x].Value);

                return 1;
                return Int64.Parse(update.Parameters[0].Value.ToString());
            }
        }

        protected DbConnection Connection()
        {
            // we share our parent's connection - so delegate to parent
            if (parent != null)
            {
                DbConnection conn = parent.Connection();
                if (commitMode == Mode.TRANSACTIONAL && parent.transaction == null)
                {
                    transaction = conn.BeginTransaction(updateIsolation);
                }

                return conn;
            }

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

                if (commitMode == Mode.TRANSACTIONAL) {
                    transaction = connection.BeginTransaction(updateIsolation);
                }
            }

            //assert (connection != null);

            return connection;
        }

        protected void closeResources()
        {
            // only close local resources (do NOT close parent)

            // any batch is local to this SqlSession, so close it now
            if (mode == Mode.BATCH && batch != null)
            {
                long batchStart = Environment.TickCount;
                NuoDbBulkLoader loader = new NuoDbBulkLoader((NuoDbConnection)Connection());
                //{
                loader.DestinationTableName = batch[0].Table.TableName;
                //loader.DestinationTableName = BatchTable.TableName;
                int index = 0;
                foreach (DataColumn c in batch[0].Table.Columns)
                //foreach (DataColumn c in BatchTable.Columns)
                {
                    loader.ColumnMappings.Add(index++, c.ColumnName);
                }
                loader.WriteToServer(batch.ToArray());
                //loader.WriteToServer(BatchTable);
                //}
                long duration = Environment.TickCount - batchStart;
                double rate = (batch.Count > 0 && duration > 0 ? 1000.0 * batch.Count / duration : 0);
                log.info("Batch commit complete duration={0:N0} ms; rate={1:F2} ips", duration, rate);
            }

            // commit any transaction scoped to this session
            if (connection != null && transaction != null && commitMode == Mode.TRANSACTIONAL) {
                transaction.Commit();
            }
        }

        protected void releaseResources()
        {
            // only close local resources - never close parent

            if (batch != null) batch.Clear();
            batch = null;

            if (BatchTable != null) {
                foreach (DataTable table in BatchTable.Values) { table.Clear(); }
                BatchTable.Clear();
            }
            BatchTable = null;

            if (statements != null) {
                //for (DbCommand ps : statements.values()) {
                foreach (DbCommand ps in statements.Values) {
                    try { ps.Dispose(); }
                    catch (Exception) { }
                }

                statements.Clear();
            }

            if (transaction != null) {
                try { transaction.Dispose(); }
                catch (Exception e) {}
            }
            transaction = null;

            if (connection != null) {
                try { connection.Dispose(); }
                catch (Exception) { }
            }
            connection = null;
        }
    }
}

