using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadItUp
{

	class Audit : Table
	{

		private static int tableCreatedCount = 0;
		private static int tableWrittenCount = 0;
		private static Object thisLock = new Object();

		public static int TableCreatedCount { get { return tableCreatedCount; } }
	
		public static int TableWrittenCount { get { return tableWrittenCount; } }

		public int AuditId { get; set; }
		
		public DateTime AuditDate { get; set; }
		
		public String ColumnC { get; set; }
		
		public String ColumnD { get; set; }
		
		public String ColumnE { get; set; }

		public Audit(DbCommand command)
			: base(command)
		{

			AuditDate = new DateTime();
			ColumnC = NewGuid();
			ColumnD = NewGuid();
			ColumnE = NewGuid();

			++Audit.tableWrittenCount;
		}

		protected override void Run()
		{

			Command.Parameters.Clear();
			Command.CommandType = CommandType.StoredProcedure;
			Command.CommandText = "INSERTAUDIT";

			DbParameter auditDate = Command.CreateParameter();
			DbParameter columnC = Command.CreateParameter();
			DbParameter columnD = Command.CreateParameter();
			DbParameter columnE = Command.CreateParameter();
			DbParameter auditId = Command.CreateParameter();

			auditDate.ParameterName = "AUDITDATE";
			auditDate.Value = AuditDate;
			Command.Parameters.Add(auditDate);

			columnC.ParameterName = "COLUMNC";
			columnC.Value = ColumnC;
			Command.Parameters.Add(columnC);

			columnD.ParameterName = "COLUMND";
			columnD.Value = ColumnD;
			Command.Parameters.Add(columnD);

			columnE.ParameterName = "COLUMNE";
			columnE.Value = ColumnE;
			Command.Parameters.Add(columnE);

			auditId.ParameterName = "AUDITID";
			auditId.Direction = ParameterDirection.Output;
			Command.Parameters.Add(auditId);

			Command.ExecuteNonQuery();

			if (Program.Verbose) {

				Console.WriteLine(String.Format("Wrote audit record got id {0}", auditId.Value));
			}

			base.Run();
		}
	}
}
