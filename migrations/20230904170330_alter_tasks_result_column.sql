-- Write your migrate up statements here
ALTER TABLE tasks MODIFY COLUMN result MEDIUMBLOB;
---- create above / drop below ----
ALTER TABLE tasks MODIFY COLUMN result BLOB;
-- Write your migrate down statements here. If this migration is irreversible
-- Then delete the separator line above.
