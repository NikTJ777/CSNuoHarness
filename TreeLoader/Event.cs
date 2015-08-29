using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{

	class Event : Entity
	{
        internal long CustomerId { get; set; }
        internal long OwnerId { get; set; }
        internal String EventGuid { get; set; }
        internal String Name { get; set; }
		internal String Description { get; set; }
		internal DateTime DateCreated { get; set; }
        internal DateTime LastUpdated { get; set; }
        internal String Region { get; set; }

		public Event(long id, long customerId, String eventGuid) : base(id)
		{
            this.CustomerId = customerId;
            this.EventGuid = eventGuid;
        }

        public Event()
            : base()
        {}
	}
}
