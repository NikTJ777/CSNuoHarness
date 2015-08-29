using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{
    class OwnerRepository : AbstractRepository<Owner>
    {
        public OwnerRepository() : base("NuoTest.OWNER", "customerId", "ownerGuid", "dateCreated", "lastUpdated", "name", "masterAliasId", "region")
        {}


        //[Override]
        public override void init()
        {   }

        //@Override
        protected override Owner mapIn(DbDataReader row) {
            Owner owner = new Owner(row.GetInt64(0), row.GetInt64(1), row.GetString(2));
            owner.DateCreated = row.GetDateTime(3);
            owner.LastUpdated = row.GetDateTime(4);
            owner.Name = row.GetString(5);
            owner.MasterAliasId = row.GetInt64(6);
            owner.Region = row.GetString(7);

            return owner;
        }

        //@Override
        protected override DataRow mapOut(Owner owner, SqlSession session) {
            DataTable table;
            if (! session.BatchTable.TryGetValue(tableName, out table))
            {
                table = new DataTable(tableName);
                session.BatchTable[tableName] = table;

                table.Columns.Add("customerId", typeof(long));
                table.Columns.Add("ownerGuid", typeof(String));
                table.Columns.Add("dateCreated", typeof(DateTime));
                table.Columns.Add("lastUpdated", typeof(DateTime));
                table.Columns.Add("name", typeof(String));
                table.Columns.Add("masterAliasId", typeof(long));
                table.Columns.Add("region", typeof(String));
            }

            DataRow row = table.NewRow();
            row[0] = owner.CustomerId;
            row[1] = owner.OwnerGuid;
            row[2] = owner.DateCreated;
            row[3] = owner.LastUpdated;
            row[4] = owner.Name;
            row[5] = owner.MasterAliasId;
            row[6] = owner.Region;

            return row;
        }
    }
}
