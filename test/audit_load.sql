DROP DATABASE IF EXISTS audit_load_test;
CREATE DATABASE audit_load_test;
USE audit_load_test;

CREATE TABLE audit_table
(
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(255),
    type INT,
    PRIMARY KEY(id)
);

DELIMITER $$

DROP PROCEDURE IF EXISTS load_proc$$
CREATE PROCEDURE load_proc(
    IN num_loops INT
)
BEGIN
	DECLARE i INT DEFAULT 0;
	WHILE i <= num_loops DO
		INSERT INTO audit_table (name, type)
            SELECT CONCAT("AUDIT", i) AS name, 1 AS type;
        DELETE FROM audit_table WHERE id = LAST_INSERT_ID();
		SET i = i + 1;
	END WHILE;
END$$

DELIMITER ;

-- audit_sess_connect_attrs=1

SET GLOBAL audit_record_objs='audit_load_test.*';
SET GLOBAL audit_record_cmds='insert,update,delete,select';
SET GLOBAL audit_json_file_full_durability_mode=0;
SET GLOBAL audit_json_file_bufsize=1; -- 0 - default, 1 - no buffering, 262144 - max
SET GLOBAL audit_json_file_sync=1;
