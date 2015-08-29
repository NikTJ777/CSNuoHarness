using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{
    class GroupRepository : AbstractRepository<Group>
    {
        public GroupRepository() : base("NuoTest.\"GROUP\"", "eventId", "groupGuid", "description", "dataCount", "dateCreated", "lastUpdated", "region", "week")
        {}

        //@Override
        public override void init()
        {

        }

        //@Override
        protected override Group mapIn(DbDataReader row) {
            Group group = new Group(row.GetInt64(0), row.GetString(2));
            group.EventId = row.GetInt64(1);
            group.Description = row.GetString(3);
            group.DataCount = row.GetInt32(4);
            group.DateCreated = row.GetDateTime(5);
            group.LastUpdated = row.GetDateTime(6);
            group.Region = row.GetString(7);
            group.Week = row.GetInt64(8);

            return group;
        }

        //@Override
        protected override DataRow mapOut(Group group, SqlSession session) {
            DataTable table;
            if (! session.BatchTable.TryGetValue(tableName, out table))
            {
                table = new DataTable(tableName);
                session.BatchTable[tableName] = table;

                table.Columns.Add("eventId", typeof(long));
                table.Columns.Add("groupGuid", typeof(String));
                table.Columns.Add("description", typeof(String));
                table.Columns.Add("dataCount", typeof(int));
                table.Columns.Add("dateCreated", typeof(DateTime));
                table.Columns.Add("lastUpdated", typeof(DateTime));
                table.Columns.Add("region", typeof(String));
                table.Columns.Add("week", typeof(long));
            }

            DataRow row = table.NewRow();
            //Console.WriteLine("GROUP MapOut");
            row[0] = group.EventId;
            row[1] = group.GroupGuid;
            row[2] = group.Description;
            row[3] = group.DataCount;
            row[4] = group.DateCreated;
            row[5] = group.LastUpdated;
            row[6] = group.Region;
            row[7] = group.Week;

            return row;
        }
    }
}
