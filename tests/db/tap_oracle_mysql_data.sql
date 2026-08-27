SET NAMES utf8mb4;
SET TIME_ZONE = '+00:00';

DROP TABLE IF EXISTS iceberg_events;
CREATE TABLE iceberg_events (
  id BIGINT NOT NULL,
  payload JSON,
  body LONGTEXT,
  binary_value VARBINARY(32),
  unsigned_value BIGINT UNSIGNED,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

INSERT INTO iceberg_events
  (id, payload, body, binary_value, unsigned_value, updated_at) VALUES
  (1, JSON_OBJECT('nested', JSON_OBJECT('value', '初'), 'empty', JSON_ARRAY()),
   'first', UNHEX('0001FF'), 18446744073709551615, '2026-08-19 10:00:00.000001'),
  (2, JSON_OBJECT('large', REPEAT('x', 70000)),
   CONCAT('large-text-', REPEAT('y', 70000)), UNHEX('CAFE'), 0,
   '2026-08-19 10:00:01.000002'),
  (3, NULL, '', NULL, NULL, '2026-08-19 10:00:02.000003');

DROP TABLE IF EXISTS iceberg_incremental;
CREATE TABLE iceberg_incremental (
  id BIGINT NOT NULL,
  value_text VARCHAR(255),
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

INSERT INTO iceberg_incremental VALUES
  (1, 'incremental-one', '2026-08-19 10:00:00.000001'),
  (2, 'incremental-two', '2026-08-19 10:00:01.000002');

DROP TABLE IF EXISTS iceberg_full_reload;
CREATE TABLE iceberg_full_reload (
  id BIGINT NOT NULL,
  value_text VARCHAR(255),
  PRIMARY KEY (id)
) ENGINE=InnoDB;

INSERT INTO iceberg_full_reload VALUES
  (1, 'full-one'),
  (2, 'full-two');
