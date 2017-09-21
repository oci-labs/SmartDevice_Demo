use smartdevice_repo;

DROP TABLE IF EXISTS valve;
CREATE TABLE `valve` (
  `fab_date` date DEFAULT NULL,
  `ship_date` date DEFAULT NULL,
  `sku` varchar(50) DEFAULT NULL,
  `station_num` int(11) DEFAULT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `valve_sn` int(11) NOT NULL,
  `ccl` int(11) DEFAULT NULL,
  `manifold_sn` int(11) DEFAULT NULL,
  PRIMARY KEY (`valve_sn`)
);

DROP TABLE IF EXISTS valve_status;
CREATE TABLE `valve_status` (
  `cc` int(11) DEFAULT NULL,
  `ccl` int(11) DEFAULT NULL,
  `input` char(1) DEFAULT NULL,
  `leak` char(1) DEFAULT NULL,
  `manifold_sn` int(11) NOT NULL,
  `p_fault` char(1) DEFAULT NULL,
  `pp` float DEFAULT NULL,
  `station_num` int(11) NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `valve_sn` int(11) NOT NULL,
  PRIMARY KEY (`manifold_sn`,`station_num`,`valve_sn`)
);

DROP TABLE IF EXISTS valve_alert;
CREATE TABLE `valve_alert` (
  `alert_type` varchar(255) NOT NULL,
  `detection_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `valve_sn` int(11) NOT NULL,
  `manifold_sn` int(11) DEFAULT NULL,
  `station_num` int(11) DEFAULT NULL,
  'needs_notification' boolean DEFAULT TRUE,
  PRIMARY KEY (`valve_sn`,`alert_type`)
);
