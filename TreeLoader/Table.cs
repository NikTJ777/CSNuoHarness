using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{

	abstract class Table : Writer
	{

		private static Random random = new Random();

		private IList<Table> children;
		private Counter counter;

		public IList<Table> Children { get { return children; } }

		public Table(DbCommand command)
			: base(command)
		{

			children = new List<Table>();
			counter = CounterFactory.getInstance().getCounter(this.GetType());
			counter.IncrementCreatedCount();
		}

		public virtual String NewGuid() {

			return Guid.NewGuid().ToString();
		}

		public int Random(int limit)
		{

			return Table.random.Next(limit - 1) + 1;
		}

		protected override void Run()
		{

			counter.IncrementWrittenCount();
		}

		internal override void Write()
		{
			
			try {

				Run();
			}

			catch (Exception e) {

				Console.WriteLine(e.Message);
				counter.IncrementExceptionCount();
			}
		}
	}
}
