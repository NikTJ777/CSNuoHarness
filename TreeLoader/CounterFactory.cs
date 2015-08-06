using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadItUp
{

	class CounterFactory
	{

		private static CounterFactory factory = new CounterFactory();
		private static Object factoryLock = new Object();

		private CounterFactory()
		{
		}

		internal static CounterFactory getInstance()
		{

			return factory;
		}

		internal Counter getCounter(Type type)
		{

			Counter counter = Counter.getCounterForType(type);

			if (counter == null) {

				lock (factoryLock) {

					counter = Counter.getCounterForType(type);

					if (counter == null) {

						counter = new Counter();
					}
				}
			}

			return counter;
		}
	}
}
