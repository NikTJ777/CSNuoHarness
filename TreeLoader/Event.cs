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
		internal long OwnerId { get; set; }
		internal String Name { get; set; }
		internal String Description { get; set; }
		internal DateTime Date { get; set; }
		internal String Region { get; set; }

		public Event(long id, String name) : base(id)
		{
            this.Name = name;
        }

        public Event()
            : base()
        {}
	}
}
