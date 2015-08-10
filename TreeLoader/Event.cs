using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{

	class Event : Table
	{

		internal String ColumnC { get; set; }
		internal String ColumnD { get; set; }
		internal String ColumnE { get; set; }
		internal String ColumnF { get; set; }
		internal String ColumnG { get; set; }
		internal String ColumnH { get; set; }
		internal String ColumnI { get; set; }
		internal String ColumnJ { get; set; }
		internal String ColumnK { get; set; }
		internal String ColumnL { get; set; }
		internal String ColumnM { get; set; }
		internal String ColumnN { get; set; }
		internal String Table1Id { get; set; }
		internal String Table2Id { get; set; }

		internal Event(String parentId, DbCommand Command)
			: base(Command)
		{

			Table1Id = parentId;
			Table2Id = NewGuid();
			ColumnC = NewGuid();
			ColumnD = NewGuid();
			ColumnE = NewGuid();
			ColumnF = NewGuid();
			ColumnG = NewGuid();
			ColumnH = NewGuid();
			ColumnI = NewGuid();
			ColumnJ = NewGuid();
			ColumnK = NewGuid();
			ColumnL = NewGuid();
			ColumnM = NewGuid();
			ColumnN = NewGuid();

			int totalChildren = Random(Program.Table2MaxChildren);

			for (int i = 0; i < totalChildren; i++) {

				Children.Add(new Group(Table2Id, Command));
			}
		}

		protected override void Run()
		{

			DbParameter table1Id = Command.CreateParameter();
			DbParameter table2Id = Command.CreateParameter();
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

			Command.Parameters.Clear();
			Command.CommandType = CommandType.StoredProcedure;
			Command.CommandText = "INSERTTABLE2";

			table1Id.ParameterName = "TABLE1ID";
			table1Id.Value = Table1Id;
			Command.Parameters.Add(table1Id);

			table2Id.ParameterName = "TABLE2ID";
			table2Id.Value = Table2Id;
			Command.Parameters.Add(table2Id);

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
