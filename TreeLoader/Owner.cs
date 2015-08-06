using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadItUp
{

	class Owner : Entity
	{

		internal String name { get; set; }
		internal String masterAlias { get; set; }

		internal Owner(long id, String name) : base(id)
        { this.name = name; }

		protected override void Run()
		{

			// Calculate the total number of rows for each table in this write.

			if (Program.Verbose) {

				int table2rows = Children.Count;
				int table3rows = 0;
				int table4rows = 0;

				foreach (Table t in Children) {

					table3rows += t.Children.Count;

					foreach (Table tPrime in t.Children) {

						table4rows += tPrime.Children.Count;
					}
				}

				Console.WriteLine(String.Format("Writing Table1 Rows: 1, Table2 Rows: {0}, Table3 Rows: {1}, Table4 Rows: {2}; Total Rows: {3}", table2rows, table3rows, table4rows, (1 + table2rows + table3rows + table4rows)));
			}

			// Begin by inserting this record.

			Command.Parameters.Clear();
			Command.CommandType = CommandType.StoredProcedure;
			Command.CommandText = "INSERTTABLE1";

			DbParameter table1Id = Command.CreateParameter();
			DbParameter columnB = Command.CreateParameter();
			DbParameter columnC = Command.CreateParameter();
			DbParameter columnD = Command.CreateParameter();
			DbParameter columnE = Command.CreateParameter();
			DbParameter columnF = Command.CreateParameter();
			DbParameter columnG = Command.CreateParameter();
			DbParameter columnH = Command.CreateParameter();
			DbParameter columnI = Command.CreateParameter();
			DbParameter columnJ = Command.CreateParameter();
			DbParameter columnK = Command.CreateParameter();
			DbParameter columnL = Command.CreateParameter();
			DbParameter columnM = Command.CreateParameter();
			DbParameter columnN = Command.CreateParameter();

			table1Id.ParameterName = "TABLE1ID";
			table1Id.Value = Table1Id;
			Command.Parameters.Add(table1Id);

			columnB.ParameterName = "COLUMNB";
			columnB.Value = ColumnB;
			Command.Parameters.Add(columnB);

			columnC.ParameterName = "COLUMNC";
			columnC.Value = ColumnC;
			Command.Parameters.Add(columnC);

			columnD.ParameterName = "COLUMND";
			columnD.Value = ColumnD;
			Command.Parameters.Add(columnD);

			columnE.ParameterName = "COLUMNE";
			columnE.Value = ColumnE;
			Command.Parameters.Add(columnE);

			columnF.ParameterName = "COLUMNF";
			columnF.Value = ColumnF;
			Command.Parameters.Add(columnF);

			columnG.ParameterName = "COLUMNG";
			columnG.Value = ColumnG;
			Command.Parameters.Add(columnG);

			columnH.ParameterName = "COLUMNH";
			columnH.Value = ColumnH;
			Command.Parameters.Add(columnH);

			columnI.ParameterName = "COLUMNI";
			columnI.Value = ColumnI;
			Command.Parameters.Add(columnI);

			columnJ.ParameterName = "COLUMNJ";
			columnJ.Value = ColumnJ;
			Command.Parameters.Add(columnJ);

			columnK.ParameterName = "COLUMNK";
			columnK.Value = ColumnK;
			Command.Parameters.Add(columnK);

			columnL.ParameterName = "COLUMNL";
			columnL.Value = ColumnL;
			Command.Parameters.Add(columnL);

			columnM.ParameterName = "COLUMNM";
			columnM.Value = ColumnM;
			Command.Parameters.Add(columnM);

			columnN.ParameterName = "COLUMNN";
			columnN.Value = ColumnN;
			Command.Parameters.Add(columnN);

			Command.ExecuteNonQuery();

			base.Run();

			foreach (Table t in Children) {

				t.Write();
			}
		}
	}
}
