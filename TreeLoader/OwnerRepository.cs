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
        public OwnerRepository() : base("NuoTest.T_OWNER", "name", "masterAliasId", "region")
        {}


        //[Override]
        public override void init()
        {   }

        //@Override
        protected override Owner mapIn(DbDataReader row) {
            Owner owner = new Owner(row.GetInt64(0), row.GetString(1));
            owner.MasterAlias = row.GetString(2);
            owner.Region = row.GetString(3);

            return owner;
        }

        //@Override
        protected override DataRow mapOut(Owner owner, SqlSession session) {
            DataTable table;
            if (! session.BatchTable.TryGetValue(tableName, out table))
            {
                table = new DataTable(tableName);
                session.BatchTable[tableName] = table;

                table.Columns.Add("name", typeof(String));
                table.Columns.Add("masterAliasId", typeof(String));
                table.Columns.Add("region", typeof(String));
            }

            DataRow row = table.NewRow();
            row[0] = owner.Name;
            row[1] = owner.MasterAlias;
            row[2] = owner.Region;

            return row;
        }
    }
}
