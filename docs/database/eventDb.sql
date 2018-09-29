CREATE TABLE IF NOT EXISTS `events_msg` (
  `TXID` char(64) NOT NULL,
  `ECODE` char(64) NOT NULL,
  `EMESSAGE` VARCHAR(512) NOT NULL,
  `ETIME` bigint(20) NOT NULL,
`CHAINID` CHAR(64) NOT NULL,
`ISPUSHED` int NOT NULL,
`TXIP` char(64) NOT NULL,
`TOTALNODES` int NOT NULL,

  PRIMARY KEY (`TXID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;