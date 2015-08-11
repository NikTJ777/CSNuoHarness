using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{

	internal class Data : Table
	{
		internal long GroupId { get; set; }
		internal String InstanceUID { get; set; }
		internal String Name { get; set; }
		internal String Description { get; set; }
		internal String Path { get; set; }
		internal bool Active { get; set; }
		internal String RegionWeek { get; set; }

		internal Data(long id, String name)	: base(id)
		{
            this.Name = name;
        }

	}
}
