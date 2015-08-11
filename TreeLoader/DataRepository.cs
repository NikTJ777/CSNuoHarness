using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{
    class DataRepository : AbstractRepository<Data>
    {
        public DataRepository()
            : base("NuoTest.T_DATA", "groupId", "instanceUID", "name", "description", "path", "active", "regionWeek")
        { }

        //@Override
        public override void init()
        {
            table = new DataTable(tableName);
            table.Columns.Add("groupId", typeof(long));
            table.Columns.Add("instanceUID", typeof(String));
            table.Columns.Add("name", typeof(String));
            table.Columns.Add("description", typeof(String));
            table.Columns.Add("path", typeof(String));
            table.Columns.Add("active", typeof(bool));
            table.Columns.Add("regionWeek", typeof(String));
        }

        /**
         * Check the uniqueness of a set of data rows.
         * Intended to be called prior to committing a set of new Data rows.
         *
         * This method marks any (and all) duplicate Data objects as inactive (leaving the original as active),
         * and returns the total number of unique rows.
         *
         * @param dataRows Map&lt;String, Data&gt;
         *
         * @return the total number of unique rows
         *
         * @throws PersistenceException
         */
        public int checkUniqueness(Dictionary<String, Data> dataRows)
        {
            int total = dataRows.Count();
            if (total == 0) return 0;
            
            Data data = dataRows.ElementAt(0).Value;

            //String sql = String.format(findBySql, getTableName(), "groupId", String.valueOf(data.getGroup()));
            using (DbDataReader existing = queryBy("groupId", data.GroupId)) {
                try {
                    while (existing.Read()) {
                        data = dataRows[existing.GetString(2)];
                        if (data != null) {
                            data.Active = false;
                            total--;
                        }
                    }

                    return total;
                } catch (/*SQL*/ Exception e) {
                    throw new PersistenceException(e, "Error DataRepository.checkUniqueness");
                }
            }
        }

        //@Override
        protected override Data mapIn(DbDataReader row) {
            Data data = new Data(row.GetInt64(0), row.GetString(3));
            data.GroupId = row.GetInt64(1);
            data.InstanceUID = row.GetString(2);
            data.Description = row.GetString(4);
            data.Path = row.GetString(5);
            data.Active = row.GetBoolean(6);
            data.RegionWeek = row.GetString(7);

            return data;
        }

        //@Override
        protected override DataRow mapOut(Data data) {
            DataRow row = table.NewRow();

            row[0] = data.GroupId;
            row[1] = data.InstanceUID;
            row[2] = data.Name;
            row[3] = data.Description;
            row[4] = data.Path;
            row[5] = data.Active;
            row[6] = data.RegionWeek;

            return row;
        }
    }

}
