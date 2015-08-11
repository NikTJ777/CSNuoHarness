using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{

	class Group : Entity
	{
		internal long EventId { get; set; }
		internal String Name { get; set; }
		internal String Description { get; set; }
		internal int DataCount { get; set; }
		internal DateTime Date { get; set; }
		internal String Region { get; set; }
		internal long Week { get; set; }

		internal Group(long id, String name) : base(id)
		{
            this.Name = name;
        }

        public Group()
            : base()
        { }
	}
}
