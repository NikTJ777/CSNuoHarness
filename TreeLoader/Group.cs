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
        internal String GroupGuid { get; set; }
        internal String Description { get; set; }
		internal int DataCount { get; set; }
		internal DateTime DateCreated { get; set; }
        internal DateTime LastUpdated { get; set; }
		internal String Region { get; set; }
		internal long Week { get; set; }

		internal Group(long id, String groupGuid) : base(id)
		{
            this.GroupGuid = groupGuid;
        }

        public Group()
            : base()
        { }
	}
}
