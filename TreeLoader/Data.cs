using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadItUp
{

	internal class Data : Table
	{

		internal String ColumnC { get; set; }
		internal String ColumnD { get; set; }
		internal String ColumnE { get; set; }
		internal String ColumnF { get; set; }
		internal String ColumnG { get; set; }
		internal String ColumnH { get; set; }
		internal String ColumnI { get; set; }
		internal String Table3Id { get; set; }
		internal String Table4Id { get; set; }

		internal Data(String parentId, DbCommand Command)
			: base(Command)
		{

			Table3Id = parentId;
			Table4Id = NewGuid();
			ColumnC = NewGuid();
			ColumnD = NewGuid();
			ColumnE = NewGuid();
			ColumnF = NewGuid();
			ColumnG = NewGuid();
			ColumnH = NewGuid();
			ColumnI = NewGuid();
		}

		protected override void Run()
		{

			DbParameter table3Id = Command.CreateParameter();
			DbParameter table4Id = Command.CreateParameter();
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
			Command.CommandText = "INSERTTABLE4";

			table3Id.ParameterName = "TABLE3ID";
			table3Id.Value = Table3Id;
			Command.Parameters.Add(table3Id);

			table4Id.ParameterName = "TABLE4ID";
			table4Id.Value = Table4Id;
			Command.Parameters.Add(table4Id);

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

			Command.ExecuteNonQuery();

			base.Run();
		}
	}
}
