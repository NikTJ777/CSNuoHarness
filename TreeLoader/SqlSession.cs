using System;
using System.Threading;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadItUp
{
    class SqlSession : IDisposable
    {
        private Mode mode;

        private DbConnection connection;
        private DbTransaction transaction;
        private DbCommand batch;
        private List<DbCommand> statements;

        //private static DataSource dataSource;
        private static ThreadLocal<SqlSession> current = new ThreadLocal<SqlSession>();
        private static Dictionary<SqlSession, String> sessions;

        private static Logger log = Logger.getLogger("SqlSession");

        //private SqlSession(Mode mode) {
        //    this.mode = mode;
        //}

        public enum Mode { AUTO_COMMIT, TRANSACTIONAL, BATCH };

        public static void init(DataSource ds, int maxThreads) {
            dataSource = ds;
            sessions = new Dictionary<SqlSession, String>();
        }

        public SqlSession(Mode mode) {
            this.mode = mode;

            SqlSession session = current.get();
            if (session != null) {
                session.close();
                throw new PersistenceException("Previous session for this thread was not correctly closed");
            }

            //session = new SqlSession(mode);
            current.Value = this;
            sessions.Add(this, Thread.CurrentThread.Name);

            //return session;
        }

        public static void cleanup() {
            if (sessions.Count == 0)
                return;

            int released = 0;
            foreach (KeyValuePair<SqlSession, String> entry in sessions) {
                log.info(String.Format("cleaning up unclosed session from %s", entry.Value));
                entry.Key.Dispose();
                released++;
            }

            throw new PersistenceException("{0} unclosed SqlSessions were cleaned up", released);
        }

        public static SqlSession getCurrent() {
            SqlSession session = current.Value;
            if (session == null)
                throw new PersistenceException("No current session");

            return session;
        }

        public void rollback() {
            if (transaction != null && mode != Mode.AUTO_COMMIT) {
                try { transaction.Rollback(); }
                catch (Exception e) {}
            }
        }

        public override void Dispose() {
            closeStatements();
            closeConnection();
            current.Value = null;
            sessions.Remove(this);
        }

        public DbCommand getStatement(String sql) {
            if (batch != null) {
                batch.Parameters.Clear();
                return batch;
            }

            if (statements == null) {
                statements = new List<DbCommand>(16);
                //statements = new HashMap<String, DbCommand>(16);
            }

            //DbCommand ps = statements.get(sql);

            //if (ps == null) {
                //int returnMode = (mode == Mode.AUTO_COMMIT ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
                //int returnMode = Statement.RETURN_GENERATED_KEYS;
                //DbCommand ps = connection().prepareStatement(sql);
                DbCommand ps = connection().CreateCommand();
                ps.CommandText = sql;
                statements.Add(ps);
                //statements.put(sql, ps);
            //} else {
            //    ps.clearParameters();
            //}

            batch = (mode == Mode.BATCH ? ps : null);

            return ps;
        }

        public void execute(String script) {
            if (script == null || script.Length == 0) return;

            String[] lines = script.Split("@".ToCharArray());

            DbTransaction transaction = connection().BeginTransaction();
            using(DbCommand command = connection().CreateCommand() ) {
                foreach (String line in lines) {
                    command.CommandText = line.Trim();
                    command.ExecuteNonQuery();
                }
                transaction.Commit();
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

        public DbDataReader update(DbCommand statement)
        {
            if (mode == Mode.BATCH) {
                //statement.addBatch();
            } else {
                statement.ExecuteNonQuery();
            }

            return (mode == Mode.AUTO_COMMIT ? statement.getGeneratedKeys() : null);
        }

        protected DbConnection connection()
        {
            if (connection == null) {
                connection = dataSource.getConnection();
            }

            //assert (connection != null);

            return connection;
        }

        protected void closeStatements() {
            if (batch != null) {
                try { batch.; } catch (Exception e) {}
                batch = null;
            }

            if (statements == null) return;

            for (DbCommand ps : statements) {
            //for (DbCommand ps : statements.values()) {
                try { ps.close(); } catch (Exception e) {}
            }

            statements.clear();
        }

        protected void closeConnection()
        {
            if (connection != null) {
                if (mode != Mode.AUTO_COMMIT) {
                    try { connection.commit(); }
                    catch (SQLException e) {
                        throw new PersistenceException(e, "Error commiting JDBC connection");
                    }
                }

                try { connection.close(); } catch (Exception e) {}

                connection = null;
            }
        }

