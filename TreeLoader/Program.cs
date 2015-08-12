using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace NuoTest
{
	class Program
	{

		private static readonly int MAXTHREADS = 150;
		private static readonly int TABLE1MAXCHILDREN = 100;
		private static readonly int TABLE2MAXCHILDREN = 100;
		private static readonly int TABLE3MAXCHILDREN = 5000;

		public static readonly String DBDRIVER = "NuoDB.Data.Client";
		
		public static bool keepRunning = true;

		public static String ConnectionString { get; private set; }
		private static String Broker { get; set; }
		private static String Database { get; set; }
		private static String LBTag { get; set; }
		public static int MaxThreads { get; private set; }
		private static String Password { get; set; }
		private static String Schema { get; set; }
		public static int Table1MaxChildren { get; private set; }
		public static int Table2MaxChildren { get; private set; }
		public static int Table3MaxChildren { get; private set; }
		private static String User { get; set; }
		public static Boolean Verbose { get; private set; }

		private IList<Thread> threads;

		private Program()
		{

			MaxThreads = MAXTHREADS;
			Table1MaxChildren = TABLE1MAXCHILDREN;
			Table2MaxChildren = TABLE2MAXCHILDREN;
			Table3MaxChildren = TABLE3MAXCHILDREN;

			threads = new List<Thread>();
		}

		private Boolean Arguments(String[] arguments)
		{

			Boolean result = false;

			foreach (String argument in arguments) {

				if (argument.StartsWith("/") || argument.StartsWith("--")) {

                    Console.WriteLine(String.Format("arg: {0}", argument));

					String[] splits = argument.Split(new Char[] { '=', ':' });
					int value;

					splits[0] = Regex.Replace(splits[0], "^--", "/");
					splits[0] = splits[0].ToLower();

					switch (splits[0]) {

						case "/broker":
							Broker = splits[1];
							break;

						case "/database":
							Database = splits[1];
							break;

						case "/lbtag":
							LBTag = splits[1];
							break;

						case "/maxthreads":
							Int32.TryParse(splits[1], out value);
							MaxThreads = (value > 0 && value < MAXTHREADS) ? value : MAXTHREADS;
							break;

						case "/password":
							Password = splits[1];
							break;

						case "/schema":
							Schema = splits[1];
							break;

						case "/table1maxchildren":
							Int32.TryParse(splits[1], out value);
							Table1MaxChildren = (value > 0 && value < TABLE1MAXCHILDREN) ? value : TABLE1MAXCHILDREN;
							break;

						case "/table2maxchildren":
							Int32.TryParse(splits[1], out value);
							Table2MaxChildren = (value > 0 && value < TABLE2MAXCHILDREN) ? value : TABLE2MAXCHILDREN;
							break;

						case "/table3maxchildren":
							Int32.TryParse(splits[1], out value);
							Table3MaxChildren = (value > 0 && value < TABLE3MAXCHILDREN) ? value : TABLE3MAXCHILDREN;
							break;

						case "/usage":
							Usage();
							break;

						case "/user":
							User = splits[1];
							break;

						case "/verbose":
							Verbose = true;
							break;

						default:
							Usage();
							break;
					}

				} else {

					Usage();
				}
			}

			if (String.IsNullOrEmpty(Broker) || String.IsNullOrEmpty(Database) || String.IsNullOrEmpty(Password) || String.IsNullOrEmpty(Schema) || String.IsNullOrEmpty(User)) {

				Usage();

			}

			return result;
		}

		// BuildSchema
		// Add the tables and stored procedures to the schema, replacing any previous that may exist.
		//
		private void BuildSchema()
		{
			
			DbProviderFactory factory = DbProviderFactories.GetFactory(Program.DBDRIVER);
			DbConnection connection = factory.CreateConnection();

			connection.ConnectionString = Program.ConnectionString;
			connection.Open();

			using (connection) {

				DbCommand command = connection.CreateCommand();

				using (command) {

					SchemaBuilder schemaBuilder = new SchemaBuilder(command);

					schemaBuilder.Write();
				}
			}
		}

		// Exit
		// Pause before exiting.
		//
		private void Exit()
		{

			Console.Write("Press enter to continue: ");
			Console.ReadLine();
		}

		// Initialize
		// Drop and restore all of the load tables.
		//
		private void Initialize()
		{

			// Create the factory, connection, and command.

			DbProviderFactory factory = DbProviderFactories.GetFactory(Program.DBDRIVER);
			DbConnectionStringBuilder connectionStringBuilder = factory.CreateConnectionStringBuilder();

			connectionStringBuilder.Add("Database", Database);

			if (MaxThreads > 100) {

				connectionStringBuilder.Add("MaxConnections", MaxThreads.ToString());
			}

			connectionStringBuilder.Add("Password", Password);
			connectionStringBuilder.Add("Pooling", "True");
			connectionStringBuilder.Add("Schema", Schema);
			connectionStringBuilder.Add("Server", Broker);
			connectionStringBuilder.Add("User", User);

			ConnectionString = connectionStringBuilder.ConnectionString;
		}

		// Launch
		// Launch all the threads and get everyone working.
		//
		private void Launch()
		{

			for (int i = 0; i < MaxThreads; i++) {

				ThreadStart threadDelegate = new ThreadStart(LoadTablesThread.Begin);
				Thread newThread = new Thread(threadDelegate);
				
				threads.Add(newThread);
				newThread.Start();
			}
		}

		// Main
		// Main entry point for the application.
		//
		static void Main(string[] args)
		{

			// Create a new Program and launch.

			//(new Program()).Run(args);
            using (Controller controller = new Controller())
            {
                try
                {
                    controller.configure(args);
                    controller.init();
                    controller.run();
                }
                //catch (InterruptedException e) {
                //    System.out.println("JNuoTest interrupted - exiting");
                //}
                catch (Exception e)
                {
                    Console.WriteLine("Exiting with fatal error: " + e.ToString());
                    //e.printStackTrace(System.out);
                    Console.WriteLine(e.StackTrace.ToString());
                }
            }

		}

		// Report
		// Log the number of rows inserted.
		//
		private void Report()
		{

			Console.WriteLine(String.Format(@"Total Records Created: {0}", Counter.TotalCreatedCount));
			Console.WriteLine(String.Format(@"Total Records Written: {0}", Counter.TotalWrittenCount));
			Console.WriteLine(String.Format(@"Total Database Exceptions Logged: {0}", Counter.TotalExceptionCount));

			Counter table1Counter = CounterFactory.getInstance().getCounter(typeof(Owner));

			Console.WriteLine(String.Format(@"Table1 Records Created: {0}", table1Counter.CreatedCount));
			Console.WriteLine(String.Format(@"Table1 Records Written: {0}", table1Counter.WrittenCount));
			Console.WriteLine(String.Format(@"Table1 Database Exceptions Logged: {0}", table1Counter.ExceptionCount));

			Counter table2Counter = CounterFactory.getInstance().getCounter(typeof(Event));

			Console.WriteLine(String.Format(@"Table2 Records Created: {0}", table2Counter.CreatedCount));
			Console.WriteLine(String.Format(@"Table2 Records Written: {0}", table2Counter.WrittenCount));
			Console.WriteLine(String.Format(@"Table2 Database Exceptions Logged: {0}", table2Counter.ExceptionCount));

			Counter table3Counter = CounterFactory.getInstance().getCounter(typeof(Group));

			Console.WriteLine(String.Format(@"Table3 Records Created: {0}", table3Counter.CreatedCount));
			Console.WriteLine(String.Format(@"Table3 Records Written: {0}", table3Counter.WrittenCount));
			Console.WriteLine(String.Format(@"Table3 Database Exceptions Logged: {0}", table3Counter.ExceptionCount));

			Counter table4Counter = CounterFactory.getInstance().getCounter(typeof(Data));

			Console.WriteLine(String.Format(@"Table4 Records Created: {0}", table4Counter.CreatedCount));
			Console.WriteLine(String.Format(@"Table4 Records Written: {0}", table4Counter.WrittenCount));
			Console.WriteLine(String.Format(@"Table4 Database Exceptions Logged: {0}", table4Counter.ExceptionCount));
		}

		// Run
		// Start and control the sequence of events.
		//
		private void Run(String[] args)
		{

			try {

				Arguments(args);
				Initialize();
				BuildSchema();
				Launch();
				Wait();
				Stop();
				Report();
			}

			catch (Exception e) {

                Console.WriteLine("Error:{0}", e);
				// We should have handled all the exceptions by this point, the only point of an exception is to abort processing.
			}

			finally {

				Exit();
			}
		}

		// Stop
		// Abort all of the running threads - needed to get an accurate report.
		//
		private void Stop()
		{

			foreach (Thread t in threads) {

				t.Abort();
			}
		}

		private void Usage()
		{

			Console.WriteLine(String.Format("Usage: {0} /Broker=name /Database=name [/LBTag=name] [/MaxThreads={1}] /Password=secret /Schema=name [/Table1MaxChildren={2}] [/Table1MaxChildren={3}] [/Table1MaxChildren={4}] [/usage] /User=name [/verbose]", System.AppDomain.CurrentDomain.FriendlyName, MAXTHREADS, TABLE1MAXCHILDREN, TABLE2MAXCHILDREN, TABLE3MAXCHILDREN));
			throw new ArgumentException();
		}

		// Wait
		// Control-C handler exits the wait.
		//
		private void Wait()
		{

			Console.CancelKeyPress += delegate(object sender, ConsoleCancelEventArgs e)
			{

				e.Cancel = true;
				Program.keepRunning = false;
			};

			while (Program.keepRunning) {

				Thread.Sleep(0);
			}
		}
	}
}