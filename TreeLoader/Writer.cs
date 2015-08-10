using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{

	// Writer
	// Template method implementation to wrap each writer with a new connection (from the pool) and a transaction.
	//
	abstract class Writer
	{

		protected DbCommand Command { get; private set; }

		internal Writer(DbCommand command)
		{

			Command = command;
		}

		internal abstract void Write();
		protected abstract void Run();
	}
}
