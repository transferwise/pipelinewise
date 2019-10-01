-- create fresh replication slot
SELECT pg_drop_replication_slot('pipelinewise_postgres_source_db');
SELECT slot_name, lsn, lsn - '0/0'::pg_lsn as lsn FROM pg_create_logical_replication_slot('pipelinewise_postgres_source_db', 'wal2json');

-- create structures
DROP SCHEMA IF EXISTS logical1 CASCADE;

CREATE SCHEMA logical1;

CREATE TABLE logical1.logical1_table1(
    cid serial NOT NULL,
    cvarchar varchar,
    PRIMARY KEY (cid)
);

CREATE TABLE logical1.logical1_table2(
    cid serial NOT NULL,
    cvarchar varchar,
    PRIMARY KEY (cid)
);

CREATE TABLE logical1.logical1_edgydata (LIKE public.edgydata INCLUDING INDEXES);


DROP SCHEMA IF EXISTS logical2 CASCADE;

CREATE SCHEMA logical2;

CREATE TABLE logical2.logical2_table1(
    cid serial NOT NULL,
    cvarchar varchar,
    PRIMARY KEY (cid)
);



DROP SCHEMA IF EXISTS logical3 CASCADE;

CREATE SCHEMA logical3;

CREATE TABLE logical3.logical3_table1(
    cid serial NOT NULL,
    cvarchar varchar,
    PRIMARY KEY (cid)
);

-- activity stream
INSERT INTO logical1.logical1_edgydata SELECT * FROM public.edgydata;

INSERT INTO logical1.logical1_table1 (cvarchar) VALUES ('inserted row');

INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('inserted row');

INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('inserted row');

BEGIN;
    INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('inserted row');
    INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('delete later');
    INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('inserted row');
    INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('delete later');
    INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('inserted row');
    INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('inserted row');
COMMIT;

INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('inserted row');

INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('inserted row');

ALTER TABLE logical1.logical1_table1 ADD COLUMN cvarchar2 varchar ;

INSERT INTO logical1.logical1_table1 (cvarchar, cvarchar2) VALUES ('inserted row', 'inserted row');

INSERT INTO logical1.logical1_table1 (cvarchar, cvarchar2) VALUES ('inserted row', 'inserted row');

INSERT INTO logical1.logical1_table1 (cvarchar, cvarchar2) VALUES ('inserted row', 'inserted row');

DELETE FROM logical1.logical1_table2 WHERE cvarchar = 'delete later';

UPDATE logical1.logical1_table2 SET cvarchar = 'updated row';

BEGIN;
    INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('inserted row');
    INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('inserted row');
    INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('inserted row');
COMMIT;

INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('inserted row');

INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('inserted row');

INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('inserted row');

INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('inserted row');

INSERT INTO logical1.logical1_table2 (cvarchar) VALUES
    ('inserted row'),
    ('inserted row'),
    ('inserted row')
;

DELETE FROM logical1.logical1_table2 WHERE cvarchar='inserted row';

INSERT INTO logical2.logical2_table1 (cvarchar) VALUES ('inserted row');
INSERT INTO logical2.logical2_table1 (cvarchar) VALUES ('inserted row');
INSERT INTO logical2.logical2_table1 (cvarchar) VALUES ('inserted row');

UPDATE logical2.logical2_table1 SET cvarchar = 'updated row';

INSERT INTO logical3.logical3_table1 (cvarchar) VALUES ('inserted row');
INSERT INTO logical3.logical3_table1 (cvarchar) VALUES ('inserted row');
INSERT INTO logical3.logical3_table1 (cvarchar) VALUES ('inserted row');

DROP SCHEMA IF EXISTS logical3 CASCADE;
