using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{

	class Owner : Entity
	{
        internal long CustomerId { get; set; }
        internal String OwnerGuid { get; set;  }
        internal DateTime DateCreated { get; set; }
        internal DateTime LastUpdated { get; set; }
		internal String Name { get; set; }
		internal long MasterAliasId { get; set; }
        internal String Region { get; set; }

		internal Owner(long id, long customerId, String ownerGuid) : base(id)
        {
            this.CustomerId = customerId;
            this.OwnerGuid = ownerGuid;
        }

        public Owner()
            : base()
        { }
	}
}
