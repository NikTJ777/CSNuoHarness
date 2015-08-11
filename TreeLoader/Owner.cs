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

		internal String Name { get; set; }
		internal String MasterAlias { get; set; }
        internal String Region { get; set; }

		internal Owner(long id, String name) : base(id)
        { this.Name = name; }

	}
}
