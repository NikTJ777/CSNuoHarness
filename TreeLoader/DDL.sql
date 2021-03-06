
DROP TABLE IF EXISTS AUDIT;
DROP TABLE IF EXISTS TABLE4;
DROP TABLE IF EXISTS TABLE3;
DROP TABLE IF EXISTS TABLE2;
DROP TABLE IF EXISTS TABLE1;

CREATE TABLE TABLE1 (

	TABLE1ID STRING NOT NULL PRIMARY KEY,
	COLUMNB STRING NOT NULL,
	COLUMNC STRING NOT NULL,
	COLUMND STRING NOT NULL,
	COLUMNE STRING NOT NULL,
	COLUMNF STRING NOT NULL,
	COLUMNG STRING NOT NULL,
	COLUMNH STRING NOT NULL,
	COLUMNI STRING NOT NULL,
	COLUMNJ STRING NOT NULL,
	COLUMNK STRING NOT NULL,
	COLUMNL STRING NOT NULL,
	COLUMNM STRING NOT NULL,
	COLUMNN STRING NOT NULL
);

DROP PROCEDURE IF EXISTS INSERTTABLE1;

SET DELIMITER @
CREATE PROCEDURE INSERTTABLE1 (IN TABLE1ID STRING,
							    IN COLUMNB STRING,
							    IN COLUMNC STRING,
							    IN COLUMND STRING,
							    IN COLUMNE STRING,
							    IN COLUMNF STRING,
							    IN COLUMNG STRING,
							    IN COLUMNH STRING,
							    IN COLUMNI STRING,
							    IN COLUMNJ STRING,
							    IN COLUMNK STRING,
							    IN COLUMNL STRING,
							    IN COLUMNM STRING,
							    IN COLUMNN STRING) AS

	INSERT INTO TABLE1 (TABLE1ID, COLUMNB, COLUMNC, COLUMND, COLUMNE, COLUMNF, COLUMNG, COLUMNH, COLUMNI, COLUMNJ, COLUMNK, COLUMNL, COLUMNM, COLUMNN)
		VALUES (TABLE1ID, COLUMNB, COLUMNC, COLUMND, COLUMNE, COLUMNF, COLUMNG, COLUMNH, COLUMNI, COLUMNJ, COLUMNK, COLUMNL, COLUMNM, COLUMNN);

END_PROCEDURE
@
SET DELIMITER ;

CREATE TABLE TABLE2 (

	TABLE2ID STRING NOT NULL PRIMARY KEY,
	TABLE1ID STRING NOT NULL,
	COLUMNC STRING NOT NULL,
	COLUMND STRING NOT NULL,
	COLUMNE STRING NOT NULL,
	COLUMNF STRING NOT NULL,
	COLUMNG STRING NOT NULL,
	COLUMNH STRING NOT NULL,
	COLUMNI STRING NOT NULL,
	COLUMNJ STRING NOT NULL,
	COLUMNK STRING NOT NULL,
	COLUMNL STRING NOT NULL,
	COLUMNM STRING NOT NULL,
	COLUMNN STRING NOT NULL,
	FOREIGN KEY (TABLE1ID) REFERENCES TABLE1 (TABLE1ID)
);

DROP PROCEDURE IF EXISTS INSERTTABLE2;

SET DELIMITER @
CREATE PROCEDURE INSERTTABLE2 (IN TABLE2ID STRING,
							    IN TABLE1ID STRING,
							    IN COLUMNC STRING,
							    IN COLUMND STRING,
							    IN COLUMNE STRING,
							    IN COLUMNF STRING,
							    IN COLUMNG STRING,
							    IN COLUMNH STRING,
							    IN COLUMNI STRING,
							    IN COLUMNJ STRING,
							    IN COLUMNK STRING,
							    IN COLUMNL STRING,
							    IN COLUMNM STRING,
							    IN COLUMNN STRING) AS

	INSERT INTO TABLE2 (TABLE2ID, TABLE1ID, COLUMNC, COLUMND, COLUMNE, COLUMNF, COLUMNG, COLUMNH, COLUMNI, COLUMNJ, COLUMNK, COLUMNL, COLUMNM, COLUMNN)
		VALUES (TABLE2ID, TABLE1ID, COLUMNC, COLUMND, COLUMNE, COLUMNF, COLUMNG, COLUMNH, COLUMNI, COLUMNJ, COLUMNK, COLUMNL, COLUMNM, COLUMNN);

END_PROCEDURE
@
SET DELIMITER ;

CREATE TABLE TABLE3 (

	TABLE3ID STRING NOT NULL PRIMARY KEY,
	TABLE2ID STRING NOT NULL,
	COLUMNC STRING NOT NULL,
	COLUMND STRING NOT NULL,
	COLUMNE STRING NOT NULL,
	COLUMNF STRING NOT NULL,
	COLUMNG STRING NOT NULL,
	COLUMNH STRING NOT NULL,
	COLUMNI STRING NOT NULL,
	FOREIGN KEY (TABLE2ID) REFERENCES TABLE2 (TABLE2ID)
);

DROP PROCEDURE IF EXISTS INSERTTABLE3;

SET DELIMITER @
CREATE PROCEDURE INSERTTABLE3 (IN TABLE3ID STRING,
							    IN TABLE2ID STRING,
							    IN COLUMNC STRING,
							    IN COLUMND STRING,
							    IN COLUMNE STRING,
							    IN COLUMNF STRING,
							    IN COLUMNG STRING,
							    IN COLUMNH STRING,
							    IN COLUMNI STRING) AS

	INSERT INTO TABLE3 (TABLE3ID, TABLE2ID, COLUMNC, COLUMND, COLUMNE, COLUMNF, COLUMNG, COLUMNH, COLUMNI)
		VALUES (TABLE3ID, TABLE2ID, COLUMNC, COLUMND, COLUMNE, COLUMNF, COLUMNG, COLUMNH, COLUMNI);

END_PROCEDURE
@
SET DELIMITER ;

CREATE TABLE TABLE4 (

	TABLE4ID STRING NOT NULL PRIMARY KEY,
	TABLE3ID STRING NOT NULL,
	COLUMNC STRING NOT NULL,
	COLUMND STRING NOT NULL,
	COLUMNE STRING NOT NULL,
	COLUMNF STRING NOT NULL,
	COLUMNG STRING NOT NULL,
	COLUMNH STRING NOT NULL,
	COLUMNI STRING NOT NULL,
	FOREIGN KEY (TABLE3ID) REFERENCES TABLE3 (TABLE3ID)
);

DROP PROCEDURE IF EXISTS INSERTTABLE4;

SET DELIMITER @
CREATE PROCEDURE INSERTTABLE4 (IN TABLE4ID STRING,
							    IN TABLE3ID STRING,
							    IN COLUMNC STRING,
							    IN COLUMND STRING,
							    IN COLUMNE STRING,
							    IN COLUMNF STRING,
							    IN COLUMNG STRING,
							    IN COLUMNH STRING,
							    IN COLUMNI STRING) AS

	INSERT INTO TABLE4 (TABLE4ID, TABLE3ID, COLUMNC, COLUMND, COLUMNE, COLUMNF, COLUMNG, COLUMNH, COLUMNI)
		VALUES (TABLE4ID, TABLE3ID, COLUMNC, COLUMND, COLUMNE, COLUMNF, COLUMNG, COLUMNH, COLUMNI);

END_PROCEDURE
@
SET DELIMITER ;

CREATE TABLE AUDIT (

	AUDITID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	AUDITDATE DATE NOT NULL,
	COLUMNC STRING NOT NULL,
	COLUMND STRING NOT NULL,
	COLUMNE STRING NOT NULL
);

DROP PROCEDURE IF EXISTS INSERTAUDIT;

SET DELIMITER @
CREATE PROCEDURE INSERTAUDIT (IN AUDITDATE DATE,
							    IN COLUMNC STRING,
							    IN COLUMND STRING,
							    IN COLUMNE STRING) AS

	INSERT INTO AUDIT (AUDITDATE, COLUMNC, COLUMND, COLUMNE)
		VALUES (AUDITDIATE, COLUMNC, COLUMND, COLUMNE);

	AUDITID = LAST_INSERT_ID();

END_PROCEDURE
@
SET DELIMITER ;
