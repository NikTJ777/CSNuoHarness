using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadItUp
{
    abstract class AbstractRepository<T> : Repository<T> where T : Entity
    {
        internal String tableName { get; set; }
        internal String[] columns;
        internal String names;
        internal String replace;

        protected static String findSql = "SELECT * from {0} where id = '{1}'";

        protected static String findBySql = "SELECT * from {0} where {1} = '{2}'";

        protected static String persistSql = "INSERT into {0} ({1}) values ({2})";

        protected static String updateSql = "UPDATE {0} set {1} = ({2}) where id = '{3}'";

        protected static String getSql = "SELECT {0} from {1} {2}";

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
            String sql = String.Format(findSql, tableName, id);
            using (DbDataReader row = SqlSession.getCurrent().getStatement(sql).executeQuery()) {
                if (row == null || row.next() == false) return null;

                return mapIn(row);
            } catch (SQLException e) {
                log.info(String.Format("FindById failed due to {0}", e.toString()));
                return null;
            }
        }


        public override long persist(T entity)
        {

//            if (entity.getPersistent()) {
//                throw new PersistenceException("Attempt to persist already persistent object {0}", entity.ToString());
//            }

            String sql = String.Format(persistSql, tableName, fields, replace);
            for (int retry = 0; ; retry++) {
                SqlSession session = SqlSession.getCurrent();

                try {
                    PreparedStatement update = session.getStatement(sql);

                    mapOut(entity, update);

                    try (ResultSet keys = session.update(update)) {
                        if (keys != null && keys.next()) {
                            return keys.getLong(1);
                        }
                    }

                    return 0;
                } catch (SQLTransientException te) {
                    if (retry < maxRetry) {
                        log.info(String.format("Retriable exception in persist: %s; retrying...", te.toString()));
                        try { Thread.sleep(retrySleep); } catch (InterruptedException e) {}
                        continue;
                    }

                    throw new PersistenceException(te, "Permanent error after %d retries", maxRetry);
                } catch (SQLException e) {
                    throw new PersistenceException(e, "Error persisting new Entity %s", entity.toString());
                }
            }
        }

        public void update(long id, String columns, params Object[] values)
        {
            StringBuilder builder = new StringBuilder();
            for (int x = values.Length; x > 0; x--) {
                if (builder.Length > 0) builder.Append(", ");
                builder.append('?'); //.append(name);
            }

            String params = builder.toString();

            String sql = String.format(updateSql, tableName, columns, params, id);
            SqlSession session = SqlSession.getCurrent();
            try (PreparedStatement update = session.getStatement(sql)) {
                setParams(update, columns, values);
            } catch (SQLException e) {
                throw new PersistenceException(e, "Error updating table %s, id %d", getTableName(), id);
            }
        }


        public override List<T> findAllBy(String column, Object ... param)
        {
            List<T> result = new ArrayList(1024);
            SqlSession session = SqlSession.getCurrent();

            try (ResultSet row = queryBy(column, param)) {
                while (row != null && row.next()) {
                    result.add(mapIn(row));
                }

                return result;
            } catch (SQLException e) {
                throw new PersistenceException(e, "Error in find all %s by %s = '%s'", tableName, column, param.toString());
            }
        }

        public override String getValue(String column, String criteria)
        {
            SqlSession session = SqlSession.getCurrent();

            try (PreparedStatement sql = session.getStatement(String.format(getSql, column, getTableName(), criteria))) {
                try (ResultSet row = sql.executeQuery()) {
                    if (row.next()) {
                        return row.getString(1);
                    } else {
                        throw new PersistenceException("No matching value found: select %s from %s %s",
                            column, getTableName(), criteria);
                    }
                }
            } catch (SQLException e) {
                throw new PersistenceException(e, "Error querying for single value: %s from %s %s",
                        column, getTableName(), criteria);
            }
        }

        protected ResultSet queryBy(String column, Object ... param)
        {
            StringBuilder sql = new StringBuilder().append(String.format(findBySql, tableName, column, param[0].toString()));
            for (int px = 1; px < param.length; px++) {
                sql.append(String.format(" OR %s = '%s'", column, param[px].toString()));
            }

            return SqlSession.getCurrent().getStatement(sql.toString()).executeQuery();
        }

        protected abstract T mapIn();

        protected abstract void mapOut(T entity, DbCommand update);

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
        protected void setParams(PreparedStatement sp, String columns, Object[] values)
        {
            String[] fields = columns.split(", ");
            if (values.length < fields.length)
                throw new PersistenceException("Invalid update request: insufficient values for named columns: %s < %s", Arrays.toString(values), columns);

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
        }

    
    }

}
