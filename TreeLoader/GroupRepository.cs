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
        public GroupRepository() : base("NuoTest.T_GROUP", "name", "description", "dataCount", "date", "region", "week")
        {}

        //@Override
        public override void init()
        {
            table = new DataTable(tableName);
            //table.Columns.Add("eventId", typeof(String));
            table.Columns.Add("name", typeof(String));
            table.Columns.Add("description", typeof(String));
            table.Columns.Add("dataCount", typeof(int));
            table.Columns.Add("date", typeof(DateTime));
            table.Columns.Add("region", typeof(String));
            table.Columns.Add("week", typeof(long));

        }

        //@Override
        protected override Group mapIn(DbDataReader row) {
            Group group = new Group(row.GetInt64(0), row.GetString(2));
            group.EventId = row.GetInt64(1);
            group.Description = row.GetString(3);
            group.DataCount = row.GetInt32(4);
            group.Date = row.GetDateTime(5);
            group.Region = row.GetString(6);
            group.Week = row.GetInt64(7);

            return group;
        }

        //@Override
        protected override DataRow mapOut(Group group) {
            DataRow row = table.NewRow();

            //row[0] = group.EventId;
            row[0] = group.Name;
            row[1] = group.Description;
            row[2] = group.DataCount;
            row[3] = group.Date;
            row[4] = group.Region;
            row[5] = group.Week;

            return row;
        }
    }
}
