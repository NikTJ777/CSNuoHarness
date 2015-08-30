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
        Logger dataLog = Logger.getLogger("DataRepository");

        public DataRepository()
            : base("NuoTest.DATA", "groupId", "dataGuid", "instanceUID", "createdDateTime", "acquiredDateTime", "version", "active", "sizeOnDiskMB", "regionWeek")
        { }

        //@Override
        public override void init()
        {   }

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
            long queryStart = Environment.TickCount;
            using (DbDataReader existing = queryBy("groupId", data.GroupId)) {
                dataLog.info("Uniqueness query complete; duration={0} ms", Environment.TickCount - queryStart);
                try {
                    long readStart = Environment.TickCount;
                    //for (int rx = 0; rx < total; rx++) {
                    while (existing.Read()) {
                        //existing.GetString(2);
                        //if (dataRows.TryGetValue("xxyyz", out data))
                        if (dataRows.TryGetValue(existing.GetString(2), out data))
                        {
                            data.Active = false;
                            total--;
                        }
                        
                    }
                    dataLog.info("Uniqueness loop checked {0} items; duration={1} ms", dataRows.Values.Count, Environment.TickCount - readStart);

                    return total;
                } catch (/*SQL*/ Exception e) {
                    throw new PersistenceException(e, "Error DataRepository.checkUniqueness");
                }
            }
        }

        //@Override
        protected override Data mapIn(DbDataReader row) {
            Data data = new Data(row.GetInt64(0), row.GetString(2));
            data.GroupId = row.GetInt64(1);
            data.InstanceUID = row.GetString(3);
            data.CreatedDateTime = row.GetDateTime(4);
            data.AcquiredDateTime = row.GetDateTime(5);
            data.Version = row.GetInt16(6);
            data.Active = row.GetBoolean(7);
            data.SizeOnDiskMB = row.GetFloat(8);
            data.RegionWeek = row.GetString(9);

            return data;
        }

        //@Override
        protected override DataRow mapOut(Data data, SqlSession session) {
            DataTable table;
            if (! session.BatchTable.TryGetValue(tableName, out table))
            {
                table = new DataTable(tableName);
                session.BatchTable[tableName] = table;

                table.Columns.Add("groupId", typeof(long));
                table.Columns.Add("dataGuid", typeof(String));
                table.Columns.Add("instanceUID", typeof(String));
                table.Columns.Add("createdDateTime", typeof(DateTime));
                table.Columns.Add("acquiredDateTime", typeof(DateTime));
                table.Columns.Add("version", typeof(Int16));
                table.Columns.Add("active", typeof(bool));
                table.Columns.Add("sizeOnDiskMB", typeof(float));
                table.Columns.Add("regionWeek", typeof(String));
            }

            DataRow row = table.NewRow();
            //Console.WriteLine("MapOut Data");
            row[0] = data.GroupId;
            row[1] = data.DataGuid;
            row[2] = data.InstanceUID;
            row[3] = data.CreatedDateTime;
            row[4] = data.AcquiredDateTime;
            row[5] = data.Version;
            row[6] = data.Active;
            row[7] = data.SizeOnDiskMB;
            row[8] = data.RegionWeek;

            return row;
        }
    }

}
