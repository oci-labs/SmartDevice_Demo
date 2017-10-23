DROP TRIGGER IF EXISTS faults_insert;
DROP TRIGGER IF EXISTS faults_update;

DELIMITER //

CREATE TRIGGER faults_insert AFTER INSERT ON valve_status
FOR EACH ROW
BEGIN
  /* Insert a cycle count alert if one does not exist. */
  IF new.cc >= new.ccl AND (select count(*) from valve_alert where valve_sn=new.valve_sn and alert_type='cycle count') = 0 THEN
    INSERT INTO valve_alert (valve_sn, manifold_sn, station_num, detection_time, alert_type, needs_notification) VALUES (new.valve_sn, new.manifold_sn, new.station_num, new.timestamp, 'cycle count', true);
  END IF;

  IF new.p_fault != 'N' THEN
    INSERT INTO valve_alert (valve_sn, manifold_sn, station_num, detection_time, alert_type, needs_notification) VALUES (new.valve_sn, new.manifold_sn, new.station_num, new.timestamp, 'pressure fault', true);
  END IF;

  IF new.leak != 'N' THEN
    INSERT INTO valve_alert (valve_sn, manifold_sn, station_num, detection_time, alert_type, needs_notification) VALUES (new.valve_sn, new.manifold_sn, new.station_num, new.timestamp, 'leak', true);
  END IF;

END //

CREATE TRIGGER faults_update AFTER UPDATE ON valve_status
FOR EACH ROW
BEGIN
  /* Insert a cycle count alert if one does not exist. */
  IF new.cc >= old.ccl AND (select count(*) from valve_alert where valve_sn=old.valve_sn and alert_type='cycle count') = 0 THEN
    INSERT INTO valve_alert (valve_sn, manifold_sn, station_num, detection_time, alert_type, needs_notification) VALUES (new.valve_sn, new.manifold_sn, new.station_num, new.timestamp, 'cycle count', true);
  END IF;

  IF old.p_fault = 'N' AND new.p_fault != 'N' THEN
    INSERT INTO valve_alert (valve_sn, manifold_sn, station_num, detection_time, alert_type, needs_notification) VALUES (new.valve_sn, new.manifold_sn, new.station_num, new.timestamp, 'pressure fault', true);
  ELSEIF old.p_fault != 'N' AND new.p_fault = 'N' THEN
    DELETE FROM valve_alert where valve_sn=new.valve_sn and alert_type='pressure fault';
  END IF;

  IF old.leak = 'N' AND new.leak != 'N' THEN
    INSERT INTO valve_alert (valve_sn, manifold_sn, station_num, detection_time, alert_type, needs_notification) VALUES (new.valve_sn, new.manifold_sn, new.station_num, new.timestamp, 'leak', true);
  ELSEIF old.leak != 'N' AND new.leak = 'N' THEN
    DELETE FROM valve_alert where valve_sn=new.valve_sn and alert_type='leak';
  END IF;

END //

DELIMITER ;