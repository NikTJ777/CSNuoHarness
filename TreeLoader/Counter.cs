using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadItUp
{

	class Counter
	{

		private static Object counterLock = new Object();
		private static Dictionary<Type, Counter> counters = new Dictionary<Type, Counter>();

		internal static Counter getCounterForType(Type type) {

			return counters.ContainsKey(type) ? counters[type] : null;
		}

		internal static int TotalCreatedCount { get; private set; }
		internal static int TotalExceptionCount { get; private set; }
		internal static int TotalWrittenCount { get; private set; }

		internal int CreatedCount { get; private set; }
		internal int WrittenCount { get; private set; }
		internal int ExceptionCount { get; private set; }

		internal Counter()
		{

			CreatedCount = 0;
			ExceptionCount = 0;
			WrittenCount = 0;
		}

		internal void IncrementCreatedCount()
		{

			lock (counterLock) {

				++CreatedCount;
				++TotalCreatedCount;
			}
		}

		internal void IncrementExceptionCount()
		{

			lock (counterLock) {

				++ExceptionCount;
				++TotalExceptionCount;
			}
		}

		internal void IncrementWrittenCount()
		{

			lock (counterLock) {

				++WrittenCount;
				++TotalWrittenCount;

				if (TotalWrittenCount % 1000 == 0) {

					Console.WriteLine("Wrote {0} total rows.", TotalWrittenCount);
				}
			}
		}
	}
}
