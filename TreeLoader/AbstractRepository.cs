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
        internal DataTable table;

        internal int maxRetry = 3;
        internal int retrySleep = 2000;

        protected static readonly String findSql = "SELECT * from {0} where id = ?";

        protected static readonly String findBySql = "SELECT * from {0} where {1} = ?";

        protected static readonly String persistSql = "INSERT into {0} ({1}) values ({2})";

        protected static readonly String updateSql = "UPDATE {0} set {1} = ({2}) where id = ?";

        protected static readonly String getSql = "SELECT {0} from {1} {2}";

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
            String sql = String.Format(findSql, tableName);
            DbCommand cmd = SqlSession.getCurrent().getStatement(sql);
            cmd.Parameters[0].Value = id;
            using (DbDataReader row = cmd.ExecuteReader()) {
                try {
                    if (row == null || row.Read() == false) return null;

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

            String sql = String.Format(persistSql, tableName, names, replace);
            for (int retry = 0; ; retry++) {
                SqlSession session = SqlSession.getCurrent();

                try {
                    //DataRow update = session.getStatement(sql);

                    DataRow row = mapOut(entity);

                    return session.update(row, sql);

                } catch (/*NuoDbSQLTransient */Exception te) {
                    if (retry < maxRetry) {
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

            String sql = String.Format(updateSql, tableName, columns, args);
            SqlSession session = SqlSession.getCurrent();
            using (DbCommand update = session.getStatement(sql)) {
                try {
                    setParams(update, columns, values);
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

            using (DbCommand sql = session.getStatement(String.Format(getSql, column, tableName, criteria))) {
                try {
                    return sql.ExecuteScalar().ToString();
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
                cmd.Parameters[px].Value = param[px];
            }
            //log.info("queryBy {0}", sql.ToString());
            return cmd.ExecuteReader();
        }

        public abstract void init();

        protected abstract T mapIn(DbDataReader reader);

        protected abstract DataRow mapOut(T entity);

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
