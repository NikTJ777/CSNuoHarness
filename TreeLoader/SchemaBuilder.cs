using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace LoadItUp
{

	// SchemaBuilder
	// Establish the schema in the database.
	//
	class SchemaBuilder : Writer
	{

		public SchemaBuilder(DbCommand command)
			: base(command)
		{
		}

		internal override void Write()
		{

			try {

				Run();
			}

			catch (Exception e) {

				Console.WriteLine(e.Message);
				throw e;
			}
		}

		protected override void Run()
		{

			Command.Parameters.Clear();
			Command.CommandType = CommandType.Text;

			foreach (String ddl in ddlCommands) {

				String pattern = @"[\t]";

				Command.CommandText = Regex.Replace(ddl, pattern, "");
				Command.ExecuteNonQuery();
			}
		}

		private String[] ddlCommands = new String[] {

			@"DROP PROCEDURE INSERTAUDIT IF EXISTS",

			@"DROP TABLE IF EXISTS AUDIT",

			@"DROP PROCEDURE IF EXISTS INSERTDATA",

			@"DROP TABLE IF EXISTS T_DATA",

			@"DROP PROCEDURE IF EXISTS INSERTGROUP",

			@"DROP TABLE IF EXISTS T_GROUP",

			@"DROP PROCEDURE IF EXISTS INSERTEVENT",

			@"DROP TABLE IF EXISTS T_EVENT",

			@"DROP PROCEDURE IF EXISTS INSERTOWNER",

			@"DROP TABLE IF EXISTS T_OWNER",

			@"CREATE TABLE T_OWNER (

				id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				name STRING NOT NULL,
				masterAlias BIGINT NOT NULL
			)",

			@"CREATE TABLE T_EVENT(

				id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				ownerId BIGINT NOT NULL,
				name STRING NOT NULL,
				description STRING NOT NULL,
				date DATE NOT NULL
			)",

			@"CREATE TABLE T_GROUP (

				id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				eventId BIGINT NOT NULL,
				name STRING NOT NULL,
				description STRING NOT NULL,
				dataCount INTEGER NOT NULL,
				date DATE NOT NULL
			)",

			@"CREATE TABLE T_DATA (

				id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				groupId BIGINT NOT NULL,
				instanceUID STRING NOT NULL,
				name STRING NOT NULL,
				description STRING NOT NULL,
				path STRING NOT NULL,
				active SMALLINT NOT NULL
			)",

			@"CREATE TABLE T_AUDIT (

				id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
				date DATE NOT NULL,
				targetTable STRING NOT NULL,
				targetId BIGINT NOT NULL,
				action STRING NOT NULL
			)",

		};
	}
}
