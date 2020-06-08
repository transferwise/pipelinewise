--
-- Postgres as a target data store requires the pgcrypto extension to apply transformation
-- and obfuscation rules. The required DIGEST function available in the pgcrypto extension
--
CREATE EXTENSION IF NOT EXISTS pgcrypto;
