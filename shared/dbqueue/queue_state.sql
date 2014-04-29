-- This table is required by the queue user to keep queue pointer state.

CREATE TABLE IF NOT EXISTS queue_state (
  `queue_name` VARCHAR(32) NOT NULL COMMENT 'the name of the queue',
  `pointer_name` VARCHAR(32) COMMENT 'special value "in" for writer pointer, all other value for read pointers',
  `table_name` VARCHAR(32) NOT NULL COMMENT 'table name of queue_name_xxxx',
  `pos_id` INT DEFAULT 0 COMMENT 'pos_id in side the table',
  `odometer` BIGINT DEFAULT 0 COMMENT 'tracking for number of entries',
  `mtime` TIMESTAMP NOT NULL,

  PRIMARY KEY (`queue_name`, `pointer_name`)
) ENGINE=InnoDB
