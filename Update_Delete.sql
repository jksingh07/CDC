# Commands for CDC - Change Data Capture

Select * from jaskaran_schema.Persons;

update jaskaran_schema.Persons set FullName = 'ABC XYZ' where PersonId = 7;

INSERT INTO jaskaran_schema.Persons VALUES (130,'Alica Bing','New York');
INSERT INTO jaskaran_schema.Persons VALUES (131,'Malinda Bing','Detroit');
INSERT INTO jaskaran_schema.Persons VALUES (132,'Chandler Bing','Portland');

update jaskaran_schema.Persons set FullName = 'ABC XYZ' where PersonId = 8;

DELETE FROM jaskaran_schema.Persons where PersonId = 10;

INSERT INTO jaskaran_schema.Persons VALUES (133,'Jaskaran Luthra','Windsor');
INSERT INTO jaskaran_schema.Persons VALUES (134,'Sahil Soni','Windsor');

INSERT INTO jaskaran_schema.Persons VALUES (135,'Jazzy','Windsor');
INSERT INTO jaskaran_schema.Persons VALUES (136,'Rahul','Windsor');

INSERT INTO jaskaran_schema.Persons VALUES (137,'Alex','Windsor');
INSERT INTO jaskaran_schema.Persons VALUES (138,'Raz','Windsor');