-- ====================================
-- Create fresh replication slot
-- ====================================
SELECT pg_drop_replication_slot('pipelinewise_postgres_source_db');
SELECT slot_name, lsn, lsn - '0/0'::pg_lsn as lsn FROM pg_create_logical_replication_slot('pipelinewise_postgres_source_db', 'wal2json');

-- ====================================
-- Schema logical1
-- ====================================
DROP SCHEMA IF EXISTS logical1 CASCADE;
CREATE SCHEMA logical1;

CREATE TABLE logical1.logical1_table1(
    cid serial NOT NULL,
    cvarchar varchar,
    PRIMARY KEY (cid)
);

INSERT INTO logical1.logical1_table1 (cvarchar) VALUES ('one please');

ALTER TABLE logical1.logical1_table1
ADD COLUMN cvarchar2 varchar
;

INSERT INTO logical1.logical1_table1 (cvarchar, cvarchar2) VALUES ('two please', 'two please');

CREATE TABLE logical1.logical1_table2(
    cid serial NOT NULL,
    cvarchar varchar,
    PRIMARY KEY (cid)
);

INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('one please');
INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('two please');
INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('three please');

BEGIN;
INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('four please');
INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('five please');
INSERT INTO logical1.logical1_table2 (cvarchar) VALUES ('six please');
COMMIT;

INSERT INTO logical1.logical1_table2 (cvarchar) VALUES
    ('seven please'),
    ('eight please'),
    ('nine please')
;

DELETE FROM logical1.logical1_table2 WHERE cvarchar='four please';

-- ====================================
-- Schema logical2
-- ====================================
DROP SCHEMA IF EXISTS logical2 CASCADE;
CREATE SCHEMA logical2;

CREATE TABLE logical2.logical2_table1(
    cid serial NOT NULL,
    cvarchar varchar,
    PRIMARY KEY (cid)
);

INSERT INTO logical2.logical2_table1 (cvarchar) VALUES ('one please');
INSERT INTO logical2.logical2_table1 (cvarchar) VALUES ('two please');
INSERT INTO logical2.logical2_table1 (cvarchar) VALUES ('three please');

UPDATE logical2.logical2_table1 SET cvarchar = 'I meant eight, please';

-- ====================================
-- Schema logical3
-- ====================================
DROP SCHEMA IF EXISTS logical3 CASCADE;
CREATE SCHEMA logical3;

CREATE TABLE logical3.logical3_table1(
    cid serial NOT NULL,
    cvarchar varchar,
    PRIMARY KEY (cid)
);

INSERT INTO logical3.logical3_table1 (cvarchar) VALUES ('one please');
INSERT INTO logical3.logical3_table1 (cvarchar) VALUES ('two please');
INSERT INTO logical3.logical3_table1 (cvarchar) VALUES ('three please');

UPDATE logical3.logical3_table1 SET cvarchar = 'I meant eight, please';

DROP SCHEMA IF EXISTS logical3 CASCADE;
