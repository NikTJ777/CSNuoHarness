using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{

	internal class Data : Entity
	{
		internal long GroupId { get; set; }
        internal String DataGuid { get; set; }
		internal String InstanceUID { get; set; }
		internal DateTime CreatedDateTime { get; set; }
		internal DateTime AcquiredDateTime { get; set; }
		internal byte Version { get; set; }
		internal bool Active { get; set; }
        internal float SizeOnDiskMB { get; set; }
		internal String RegionWeek { get; set; }

		internal Data(long id, String dataGuid)	: base(id)
		{
            this.DataGuid = dataGuid;
        }

        public Data()
            : base()
        { }

	}
}
