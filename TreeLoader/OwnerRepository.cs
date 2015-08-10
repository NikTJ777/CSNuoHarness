﻿using System;
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
        public void init()
        {
            table = new DataTable(tableName);
            table.Columns.Add("name", typeof(String));
            table.Columns.Add("masterAliasId", typeof(String));
            table.Columns.Add("region", typeof(String));
        }

        //@Override
        public Owner mapIn(DbDataReader row) {
            Owner owner = new Owner(row.GetInt64(0), row.GetString(0));
            owner.masterAlias = row.GetString(1);
            owner.region = row.GetString(2);

            return owner;
        }

        //@Override
        public DataRow mapOut(Owner owner) {
            DataRow row = table.NewRow();
            row[0] = owner.name;
            row[1] = owner.masterAlias;
            row[2] = owner.region;

            return row;
        }
    }
}
