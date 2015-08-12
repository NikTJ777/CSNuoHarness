using System;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{

	public class LoadTablesThread
	{

		private DbConnection connection;
		private DbCommand command;

		public LoadTablesThread()
		{
		}

		public static void Begin()
		{

			(new LoadTablesThread()).Run();
		}

		private void Run()
		{
			/*
			DbProviderFactory factory = DbProviderFactories.GetFactory(Program.DBDRIVER);
			DbConnection connection = factory.CreateConnection();

			connection.ConnectionString = Program.ConnectionString;
			connection.Open();

			using (connection) {

				DbCommand command = connection.CreateCommand();

				using (command) {

					for (; ; ) {

						Owner table = new Owner(command);
						Audit audit1 = new Audit(command);
						Audit audit2 = new Audit(command);

						DbTransaction transaction = connection.BeginTransaction();

						table.Write();
						audit1.Write();
						audit2.Write();

						transaction.Commit();
					}
				}
			}
             */
		}
	}
}
