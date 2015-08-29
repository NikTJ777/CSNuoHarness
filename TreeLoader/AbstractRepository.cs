using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using NuoDb.Data.Client;

namespace NuoTest
{
    abstract class AbstractRepository<T> : Repository<T> where T : Entity
    {
        internal String tableName { get; set; }
        internal String[] columns;
        internal String names;
        internal String replace;

        internal int maxRetry = 3;
        internal int retrySleep = 2000;

        protected static readonly String findSql = "SELECT * from {0} where id = ?";

        protected static readonly String findBySql = "SELECT * from {0} where {1} = ?";

        protected static readonly String persistSql = "INSERT into {0} ({1}) values ({2})";

        protected static readonly String updateSql = "UPDATE {0} set {1} = ({2}) where id = ?";

        protected static readonly String getSql = "SELECT {1} from {0} {2}";

        protected static readonly String callSP = "CALL {0}";

        protected static readonly String SPname = "{0}{1}{2}";

        internal static Logger log = Logger.getLogger("AbstractRepository");

        internal AbstractRepository(String tableName, params String[] columns)
        {
            this.tableName = tableName;
            this.columns = columns;

            StringBuilder nameBuilder = new StringBuilder();
            StringBuilder replaceBuilder = new StringBuilder();
            foreach (String name in columns) {
                if (nameBuilder.Length > 0)
                {
                    nameBuilder.Append(", ");
                    replaceBuilder.Append(", ");
                }

                nameBuilder.Append(name);
                replaceBuilder.Append("?");
            }
            
            names = nameBuilder.ToString();
            replace = replaceBuilder.ToString();
        }

        public T findById(long id)
        {
            //String sql = String.Format(findSql, tableName);
            String sql = formatSql("find");
            DbCommand cmd = SqlSession.getCurrent().getStatement(sql);
            cmd.Parameters[0].Value = id;
            using (DbDataReader row = cmd.ExecuteReader()) {
                try {
                    if (row == null || row.Read() == false)
                    {
                        log.info("No persistent object found for {0} key {1}", tableName, id);
                        return null;
                    }

                    return mapIn(row);
                } catch (Exception e) {
                    log.info("FindById failed due to {0}", e.ToString());
                    return null;
                }
            }
        }


        public long persist(T entity)
        {
            // do not persist an already persistent object
            if (entity.Persistent) {
                throw new PersistenceException("Attempt to persist already persistent object {0}", entity.ToString());
            }

            //String sql = String.Format(persistSql, tableName, names, replace);
            String sql = formatSql("insert");
            SqlSession session = SqlSession.getCurrent();
            for (int retry = 0; ; retry++)
            {

                try {
                    //DataRow update = session.getStatement(sql);

                    //long mapStart = Environment.TickCount;
                    DataRow row = mapOut(entity, session);
                    //log.info("map out complete; time={0} ms", Environment.TickCount - mapStart);

                    //long updateStart = Environment.TickCount;
                    return session.update(row, sql);
                    //log.info("session.update complete; time={0} ms", Environment.TickCount - updateStart);

                } catch (/*NuoDbSQLTransient */Exception te) {
                    if (retry < maxRetry && session.retry(te)) {
                        log.info("Retriable exception in persist: {0}; retrying...", te.ToString());
                        try { Thread.Sleep(retrySleep); } catch (/*Interrupted*/Exception) {}
                        continue;
                    }

                    throw new PersistenceException(te, "Permanent error after {0} retries", maxRetry);
                } /*catch (Exception e) {
                    throw new PersistenceException(e, "Error persisting new Entity {0}", entity.ToString());
                } */
            }
        }

        public void update(long id, String columns, params Object[] values)
        {
            StringBuilder builder = new StringBuilder();
            for (int x = values.Length; x > 0; x--) {
                if (builder.Length > 0) builder.Append(", ");
                builder.Append('?'); //.append(name);
            }

            String args = builder.ToString();

            //String sql = String.Format(updateSql, tableName, columns, args);
            String sql = formatSql("update", columns, args);
            SqlSession session = SqlSession.getCurrent();
            using (DbCommand update = session.getStatement(sql)) {
                try {
                    setParams(update, columns, values);
                    //update.Parameters.Add(update.CreateParameter());
                    update.Parameters[values.Length].Value = id;
                    session.update(update);
                } catch (Exception e) {
                    throw new PersistenceException(e, "Error updating table {0}, id {1}", tableName, id);
                }
            }
        }


        public List<T> findAllBy(String column, params Object[] args)
        {
            List<T> result = new List<T>(1024);
            SqlSession session = SqlSession.getCurrent();

            using (DbDataReader row = queryBy(column, args)) {
                try {
                    while (row != null && row.Read()) {
                        result.Add(mapIn(row));
                    }

                    return result;
                } catch (Exception e) {
                    throw new PersistenceException(e, "Error in find all {0} by {1} = '{2}'", tableName, column, args.ToString());
                }
            }
        }

        public String getValue(String column, String criteria)
        {
            SqlSession session = SqlSession.getCurrent();
            String sql = formatSql("get", column, criteria);

            using (DbCommand cmd = session.getStatement(sql)) {
                try {
                    return cmd.ExecuteScalar().ToString();
                } catch (Exception e) {
                    throw new PersistenceException(e, "Error querying for single value: {0} from {1} {2}",
                            column, tableName, criteria);
                }
            }
        }

        protected DbDataReader queryBy(String column, params Object[] param)
        {
            StringBuilder sql = new StringBuilder().AppendFormat(findBySql, tableName, column);
            for (int px = 1; px < param.Length; px++) {
                sql.AppendFormat(" OR {0} = ?", column);
            }
            DbCommand cmd = SqlSession.getCurrent().getStatement(sql.ToString());
            for (int px = 0; px < param.Length; px++) {
                //cmd.Parameters.Add(cmd.CreateParameter());
                cmd.Parameters[px].Value = param[px];
            }
            //log.info("queryBy {0}", sql.ToString());
            //cmd.Prepare();
            return cmd.ExecuteReader();
        }

        public abstract void init();

        protected abstract T mapIn(DbDataReader reader);

        protected abstract DataRow mapOut(T entity, SqlSession session);

        protected String formatSql(String verb, params String[] args)
        {
            String[] fqTable = tableName.Split(new Char[] {'.'});
            String table = (fqTable.Length > 1 ? fqTable[1] : fqTable[0]);
            if (table.StartsWith("\"")) table = table.Substring(1, table.Length - 2);

            String spname = String.Format(SPname, SqlSession.SpNamePrefix, verb, table);

            switch (SqlSession.interfaceMode)
            {
                case SqlSession.InterfaceMode.SQL:
                    switch (verb)
                    {
                        case "insert":
                            return String.Format(persistSql, tableName, names, replace);

                        case "find":
                            return String.Format(findSql, tableName);

                        case "update":
                            return String.Format(updateSql, tableName, args[0], args[1]);

                        case "get":
                            return String.Format(getSql, tableName, args[0], args[1]);
                    }
                    break;

                case SqlSession.InterfaceMode.CALL:
                    return String.Format(callSP, spname);

                case SqlSession.InterfaceMode.STORED_PROCEDURE:
                    return spname;
            }

            throw new PersistenceException("Invalid call to formatSql: {0}", verb);
        }

        /**
         * set parameters into a PreparedStatement
         *
         * @param sp PreparedStatement - the prepared statement to set the parameters into
         * @param columns String - a comma-separated list of columns to update - in the form "a, b, c"
         * @param values Object[] - the array of values to be set into the prepared statement - one per column name
         *
         * @throws PersistenceException if the number of values is less than the number of column names
         * @throws SQLException if the PreparedStatement throws any exception
         */
        protected void setParams(DbCommand sp, String columns, Object[] values)
        {
            for (int vx = 0; vx < values.Length; vx++) {
                sp.Parameters.Add(sp.CreateParameter());
                sp.Parameters[vx].Value = values[vx];
            }

            /*
            String[] fields = columns.Split(", ");
            if (values.Length < fields.Length)
                throw new PersistenceException("Invalid update request: insufficient values for named columns: {0} < {1}", values.ToString(), columns);

            for (int vx = 0; vx < values.length; vx++) {
                Class type = values[vx].getClass();

                if (type == Int.class) {
                    sp.setInt(vx+1, (Integer) values[vx]);
                }
                else if (type == Long.class) {
                    sp.setLong(vx+1, (Long) values[vx]);
                }
                else if (type == String.class) {
                    sp.setString(vx+1, values[vx].toString());
                }
                else if (type == Boolean.class) {
                    sp.setBoolean(vx+1, (Boolean) values[vx]);
                }
                else if (type == Date.class) {
                    sp.setDate(vx+1, new java.sql.Date(((Date) values[vx]).getTime()));
                }
            }
             */
        }

    
    }

}
