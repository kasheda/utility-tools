CREATE OR REPLACE FUNCTION "AP_CONS_LN_ACCT_SCHEDULE" (var_pi_stream IN NUMBER)
  RETURN NUMBER AS
  var_l_count            NUMBER;
  var_l_count1           NUMBER;
  var_l_dist_count       NUMBER;
  var_l_consis_no        NUMBER := 0;
  var_mig_date           DATE := cbsfchost.pk_ba_global.dat_last_process; /*nvl(ap_get_data_mig_param('MIG_DATE'),
                                  cbsfchost.pk_ba_global.dat_last_process);*/
  var_l_dat_process      DATE := cbsfchost.pk_ba_global.dat_process;
  var_l_dat_last_process DATE := cbsfchost.pk_ba_global.dat_last_process;
  var_l_function_name    VARCHAR2(100) := 'AP_CONS_LN_ACCT_SCHEDULE';
  var_l_table_name       VARCHAR2(100) := 'CO_LN_ACCT_SCHEDULE';
  var_up_month           VARCHAR2(4) := to_char(cbsfchost.dateadd(cbsfchost.mm,
                                                                  1,
                                                                  var_mig_date),
                                                'MMYY');
BEGIN
  --    EXECUTE IMMEDIATE 'Alter session enable parallel dml';
  --    EXECUTE IMMEDIATE 'Alter session enable parallel query';
  --    ORA_RAISERROR(SQLCODE, 'In ap_cons_ln_acct_schedule ' || SQLERRM, 93);
  ap_bb_mig_log_string('Started #' || var_l_function_name || '# Stream = ' ||
                       var_pi_stream);
  --    var_mig_date        := nvl(ap_get_data_mig_param('MIG_DATE'), cbsfchost.pk_ba_global.dat_last_process);

  DELETE FROM co_ln_consis
   WHERE upper(nam_table) = upper(var_l_table_name);

  DELETE FROM co_ln_consis_acct
   WHERE cod_consis_no >= 23001
     AND cod_consis_no <= 23999;

  COMMIT;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE z_co_ln_acct_schedule_detls_amt_instal_outst DROP ALL STORAGE';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE z_co_ln_acct_schedule_amt_instal DROP ALL STORAGE';

  ap_bb_mig_log_string('00000 #' || var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --Beginning of function

  var_l_consis_no := 23000; --100s
  BEGIN
    INSERT /*+enable_parallel_dml append NOLOGGING parallel 4*/
    INTO z_co_ln_acct_schedule_detls_amt_instal_outst
      SELECT *
        FROM (SELECT cod_acct_no,
                     ROW_NUMBER() OVER(PARTITION BY cod_acct_no ORDER BY cod_acct_no, date_instal DESC) rown,
                     ctr_stage_no,
                     ctr_instal,
                     date_instal,
                     amt_instal_outst
                FROM co_ln_acct_schedule_detls)
       WHERE rown = 2;
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.LN_UTL.RAISE_ERR(sqlcode,
                                 'Insert failed for z_co_ln_acct_schedule_detls_amt_instal_outst ',
                                 cbsfchost.lineno);
  END;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  --4s
  BEGIN
    INSERT /*+enable_parallel_dml append NOLOGGING parallel 4*/
    INTO z_co_ln_acct_schedule_amt_instal
      SELECT *
        FROM (SELECT cod_acct_no,
                     ROW_NUMBER() OVER(PARTITION BY cod_acct_no ORDER BY cod_acct_no, ctr_stage_no DESC) rown,
                     ctr_stage_no,
                     dat_stage_start,
                     dat_stage_end,
                     amt_instal,
                     migration_source
                FROM co_ln_acct_schedule)
       WHERE rown = 2;
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.LN_UTL.RAISE_ERR(sqlcode,
                                 'Insert failed for z_co_ln_acct_schedule_detls_amt_instal_outst ',
                                 cbsfchost.lineno);
  END;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23001;
  /*CTR_STAGE_NO must be grater than zero : consis 23001*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule
     WHERE ctr_stage_no <= 0
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule
     WHERE ctr_stage_no <= 0
       AND migration_source = 'BRNET';

  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23001,
       'LN',
       var_l_table_name,
       'CTR_STAGE_NO',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'CTR_STAGE_NO must be grater than zero');
  end;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23001,
       'MFI',
       var_l_table_name,
       'CTR_STAGE_NO',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'CTR_STAGE_NO must be grater than zero');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23001, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule a
       WHERE ctr_stage_no <= 0
         AND a.migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23001, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule a
       WHERE ctr_stage_no <= 0
         AND a.migration_source = 'BRNET';

  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23003;
  /*consis 23002: CTR_STAGE_NO must be grater than zero*/
  --    BEGIN
  --        SELECT /*+ PARALLEL(4) */
  --            COUNT(1)
  --        INTO var_l_count
  --        FROM
  --            co_ln_acct_schedule
  --        WHERE
  --            ctr_stage_no > 0;
  --
  --    END;
  --    BEGIN
  --        INSERT INTO co_ln_consis (
  --            cod_consis_no,
  --            nam_module,
  --            nam_table,
  --            nam_column,
  --            nam_consis_func,
  --            consis_count,
  --            desc_cons
  --        ) VALUES (
  --            23002,
  --            'LN',
  --            var_l_table_name,
  --            'CTR_STAGE_NO',
  --            'ap_cons_ln_acct_schedule',
  --            var_l_count,
  --            'CTR_STAGE_NO must be grater than zero'
  --        );
  --
  --    END;
  --
  --    IF ( var_l_count > 0 ) THEN
  --        INSERT INTO co_ln_consis_acct (
  --            cod_consis_no,
  --            cod_acct_no
  --        )
  --            SELECT /*+ PARALLEL(4) */
  --                23002,
  --                cod_acct_no
  --            FROM
  --                co_ln_acct_schedule
  --            WHERE
  --                ctr_stage_no > 0;
  --
  --    END IF;
  /*consis 23003: NAM_STAGE should not be other than (IOI , EMI , PMI , MORA)*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule
     WHERE nam_stage NOT IN ('IOI', 'EMI', 'PMI', 'MOR', 'EPI', 'VPI') --VPI CAPRI_CHANGE
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule
     WHERE nam_stage NOT IN ('IOI', 'EMI', 'PMI', 'MOR', 'EPI', 'VPI') --VPI CAPRI_CHANGE
       AND migration_source = 'BRNET';

  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23003,
       'LN',
       var_l_table_name,
       'NAM_STAGE',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'NAM_STAGE should not be other than IOI,EMI,PMI,MOR,EPI');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23003,
       'MFI',
       var_l_table_name,
       'NAM_STAGE',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'NAM_STAGE should not be other than IOI,EMI,PMI,MOR,EPI');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23003, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule a
       WHERE nam_stage NOT IN ('IOI', 'EMI', 'PMI', 'MOR', 'EPI')
         AND a.migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23003, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule a
       WHERE nam_stage NOT IN ('IOI', 'EMI', 'PMI', 'MOR', 'EPI')
         AND a.migration_source = 'BRNET';

  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23004;
  /*consis 23004: DAT_STAGE_START should not be null*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule
     WHERE (dat_stage_start IS NULL OR
           dat_stage_start IN ('01-Jan-1800', '01-Jan-1950'))
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule
     WHERE (dat_stage_start IS NULL OR
           dat_stage_start IN ('01-Jan-1800', '01-Jan-1950'))
       AND migration_source = 'BRNET';
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23004,
       'LN',
       var_l_table_name,
       'DAT_STAGE_START',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'DAT_STAGE_START should not be null or default date.');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23004,
       'MFI',
       var_l_table_name,
       'DAT_STAGE_START',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'DAT_STAGE_START should not be null or default date.');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23004, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE (dat_stage_start IS NULL OR
             dat_stage_start IN ('01-Jan-1800', '01-Jan-1950'))
         and rownum <= 10 --sample 10 records esaf_changes;
         AND migration_source = 'CBS';

  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23004, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE (dat_stage_start IS NULL OR
             dat_stage_start IN ('01-Jan-1800', '01-Jan-1950'))
         and rownum <= 10 --sample 10 records esaf_changes;
         AND migration_source = 'BRNET';

  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23005;
  /*consis 23005: DAT_STAGE_END should not be null*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule
     WHERE (dat_stage_end IS NULL OR
           dat_stage_end IN ('01-Jan-1800', '01-Jan-1950'))
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule
     WHERE (dat_stage_end IS NULL OR
           dat_stage_end IN ('01-Jan-1800', '01-Jan-1950'))
       AND migration_source = 'BRNET';
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23005,
       'LN',
       var_l_table_name,
       'DAT_STAGE_END',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'DAT_STAGE_END should not be null or default date');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23005,
       'BRNET',
       var_l_table_name,
       'DAT_STAGE_END',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'DAT_STAGE_END should not be null or default date');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23005, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE (dat_stage_end IS NULL OR
             dat_stage_end IN ('01-Jan-1800', '01-Jan-1950'))
         and rownum <= 10
         AND migration_source = 'CBS'; --sample 10 records esaf_changes;;

  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23005, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE (dat_stage_end IS NULL OR
             dat_stage_end IN ('01-Jan-1800', '01-Jan-1950'))
         and rownum <= 10
         AND migration_source = 'BRNET'; --sample 10 records esaf_changes;;

  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23006;
  /*consis 23006: DAT_STAGE_END should not be less than DAT_STAGE_START*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule
     WHERE dat_stage_end < dat_stage_start
       AND migration_source = 'CBS';
    --WHERE dat_stage_end <= dat_stage_start;/*esaf_changes*/

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule
     WHERE dat_stage_end < dat_stage_start
       AND migration_source = 'BRNET';

  END;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23006;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23006,
       'LN',
       var_l_table_name,
       'DAT_STAGE_END',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'DAT_STAGE_END should not be less than or equal to DAT_STAGE_START');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23006,
       'MFI',
       var_l_table_name,
       'DAT_STAGE_END',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'DAT_STAGE_END should not be less than or equal to DAT_STAGE_START');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23006, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE dat_stage_end <= dat_stage_start
         and rownum <= 10 --sample 10 records esaf_changes;
         AND migration_source = 'CBS';

  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23006, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE dat_stage_end <= dat_stage_start
         and rownum <= 10 --sample 10 records esaf_changes;
         AND migration_source = 'BRNET';

  END IF;

  /*consis 23007: AMT_PRINC_REPAY should not be less than zero
      mandatory- need this value. Disbursement amount based on Stage.

  Note Value as per STAGE:
  IOI/MOR --> 0
  EPI --> amt >0 actual disbursement amt
  PMI --> 0 if all Instalment paid on time as per schedule, else remaining principal balance at PMI stage Bank to provide.

      */
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23007;

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule
     WHERE amt_princ_repay < 0
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule
     WHERE amt_princ_repay < 0
       AND migration_source = 'BRNET';

  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23007,
       'LN',
       var_l_table_name,
       'AMT_PRINC_REPAY',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'AMT_PRINC_REPAY should not be less than zero');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23007,
       'MFI',
       var_l_table_name,
       'AMT_PRINC_REPAY',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'AMT_PRINC_REPAY should not be less than zero');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23007, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE amt_princ_repay < 0
         AND migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23007, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE amt_princ_repay < 0
         AND migration_source = 'BRNET';
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23008;
  /*consis 23008: CTR_STAGE_TERM must be grater than zero*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule
     WHERE ctr_stage_term < 0
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule
     WHERE ctr_stage_term < 0
       AND migration_source = 'BRNET';

  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23008,
       'LN',
       var_l_table_name,
       'CTR_STAGE_TERM',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'CTR_STAGE_TERM should not be less than zero.');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23008,
       'MFI',
       var_l_table_name,
       'CTR_STAGE_TERM',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'CTR_STAGE_TERM should not be less than zero.');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23008, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule a
       WHERE ctr_stage_term < 0
         AND a.migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23008, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule a
       WHERE ctr_stage_term < 0
         AND a.migration_source = 'BRNET';

  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23009;
  /*consis 23009: AMT_ARREAR_CAP must be equals to AMT_INT for MOR*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE amt_arrear_cap <> amt_int
       AND nam_stage = 'MOR'
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE amt_arrear_cap <> amt_int
       AND nam_stage = 'MOR'
       AND migration_source = 'BRNET';

  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23009,
       'LN',
       var_l_table_name,
       'AMT_ARREAR_CAP',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'AMT_ARREAR_CAP must be equals to AMT_INT for MOR');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23009,
       'MFI',
       var_l_table_name,
       'AMT_ARREAR_CAP',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'AMT_ARREAR_CAP must be equals to AMT_INT for MOR');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23009, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE amt_arrear_cap <> amt_int
         AND nam_stage = 'MOR'
         AND migration_source = 'CBS';

  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23009, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE amt_arrear_cap <> amt_int
         AND nam_stage = 'MOR'
         AND migration_source = 'BRNET';

  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23010;
  /*consis 23010: AMT_INSTAL must be 0 for IOI,MOR,PMI*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE amt_instal <> 0
       AND nam_stage IN ('IOI', 'MOR', 'PMI')
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE amt_instal <> 0
       AND nam_stage IN ('IOI', 'MOR', 'PMI')
       AND migration_source = 'BRNET';

  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23010,
       'LN',
       var_l_table_name,
       'AMT_INSTAL',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'AMT_INSTAL must be 0 for IOI,MOR,PMI');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23010,
       'MFI',
       var_l_table_name,
       'AMT_INSTAL',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'AMT_INSTAL must be 0 for IOI,MOR,PMI');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23010, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE amt_instal <> 0
         AND nam_stage IN ('IOI', 'MOR', 'PMI')
         AND migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23010, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE amt_instal <> 0
         AND nam_stage IN ('IOI', 'MOR', 'PMI')
         AND migration_source = 'BRNET';

  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23011;
  /*consis 23011: AMT_INSTAL SHOULD NOT BE ZERO for EPI*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE amt_instal = 0
       AND nam_stage IN ('EPI', 'EMI')
       AND dat_stage_start <= var_mig_date
       AND dat_stage_end >= var_mig_date
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE amt_instal = 0
       AND nam_stage IN ('EPI', 'EMI')
       AND dat_stage_start <= var_mig_date
       AND dat_stage_end >= var_mig_date
       AND migration_source = 'BRNET';
    /*
     COUNT(a.cod_Acct_no)
      INTO var_l_count
      FROM co_ln_acct_schedule a, co_Ln_Acct_Dtls b
     WHERE a.cod_Acct_no = b.cod_Acct_no
       AND b.flg_mnt_status = 'A'
       AND b.Cod_Acct_Stat not in (1, 5, 11)
       AND amt_instal = 0
       AND nam_stage IN ('EPI', 'EMI')
    */
    /*and b.cod_Acct_no not in
    (select cod_Acct_no from ln_consis_acct_exception_23011)*/
    --; /*esaf_changes*/

  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23011,
       'LN',
       var_l_table_name,
       'AMT_INSTAL',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'AMT_INSTAL SHOULD NOT BE ZERO for EPI/EMI');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23011,
       'MFI',
       var_l_table_name,
       'AMT_INSTAL',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'AMT_INSTAL SHOULD NOT BE ZERO for EPI/EMI');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23011, a.cod_acct_no, a.migration_source
        FROM co_ln_acct_schedule a
       WHERE amt_instal = 0
         AND nam_stage IN ('EPI', 'EMI')
         and rownum <= 10 --sample 10 records esaf_changes;
         AND migration_source = 'CBS';

    /* esaf_changes
      from co_ln_acct_schedule a, co_Ln_Acct_Dtls b
     WHERE a.cod_Acct_no = b.cod_Acct_no
       AND b.flg_mnt_status = 'A'
       AND b.Cod_Acct_Stat not in (1, 5)
       AND amt_instal = 0
       AND nam_stage IN ('EPI', 'EMI');
    */
  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23011, a.cod_acct_no, a.migration_source
        FROM co_ln_acct_schedule a
       WHERE amt_instal = 0
         AND nam_stage IN ('EPI', 'EMI')
         and rownum <= 10 --sample 10 records esaf_changes;
         AND migration_source = 'BRNET';

    /* esaf_changes
      from co_ln_acct_schedule a, co_Ln_Acct_Dtls b
     WHERE a.cod_Acct_no = b.cod_Acct_no
       AND b.flg_mnt_status = 'A'
       AND b.Cod_Acct_Stat not in (1, 5)
       AND amt_instal = 0
       AND nam_stage IN ('EPI', 'EMI');
    */
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23012;
  /*consis 23012: DAT_FIRST_INSTAL must be equals to 01-jan-1950 for IOI,MOR,PMI */
  /*
      Note Value as per STAGE:
  IOI/MOR --> 1-Jan-1950
  EPI --> EPI Date
  PMI --> 1-Jan-1950.
  Bank to provide from legacy for EMI stage. Rest stages to be defaulted to 01011950

      */
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE (dat_first_instal <> TO_DATE('01-jan-1950'))
       AND nam_stage IN ('IOI', 'MOR', 'PMI')
       AND migration_source = 'CBS';
    --OR dat_first_instal <> TO_DATE('01-jan-1800')

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE (dat_first_instal <> TO_DATE('01-jan-1950'))
       AND nam_stage IN ('IOI', 'MOR', 'PMI')
       AND migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23012,
       'LN',
       var_l_table_name,
       'DAT_FIRST_INSTAL',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'DAT_FIRST_INSTAL must be equals to 01-jan-1950 for IOI,MOR,PMI');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23012,
       'MFI',
       var_l_table_name,
       'DAT_FIRST_INSTAL',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'DAT_FIRST_INSTAL must be equals to 01-jan-1950 for IOI,MOR,PMI');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23012, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE (dat_first_instal <> TO_DATE('01-jan-1950') OR
             dat_first_instal <> TO_DATE('01-jan-1800'))
         AND nam_stage IN ('IOI', 'MOR', 'PMI')
         AND migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23012, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE (dat_first_instal <> TO_DATE('01-jan-1950') OR
             dat_first_instal <> TO_DATE('01-jan-1800'))
         AND nam_stage IN ('IOI', 'MOR', 'PMI')
         AND migration_source = 'BRNET';

  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23013;
  /*consis 23013: DAT_FIRST_INSTAL must not be equals to 01-jan-1950 for EPI */
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE dat_first_instal IN
           (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'))
       AND nam_stage IN ('EPI')
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE dat_first_instal IN
           (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'))
       AND nam_stage IN ('EPI')
       AND migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23013,
       'LN',
       var_l_table_name,
       'DAT_FIRST_INSTAL',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'DAT_FIRST_INSTAL must not be equals to 01-jan-1950/01-Jan-1800 for EPI');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23013,
       'MFI',
       var_l_table_name,
       'DAT_FIRST_INSTAL',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'DAT_FIRST_INSTAL must not be equals to 01-jan-1950/01-Jan-1800 for EPI');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23013, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE dat_first_instal IN
             (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'))
         AND nam_stage IN ('EPI')
         and rownum <= 10 --sample 10 records esaf_changes;
         AND migration_source = 'CBS';
  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23013, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE dat_first_instal IN
             (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'))
         AND nam_stage IN ('EPI')
         and rownum <= 10 --sample 10 records esaf_changes;
         AND migration_source = 'BRNET';

  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23014;
  /*consis 23014: CTR_INSTAL should be zero for IOI,MOR,PMI*/
  /*
      mandatory-need this value.

  Note Value as per STAGE:
  IOI/MOR --> 0
  EPI -->  'N' no of EPI's
  PMI --> 0
  bank to provide for EMI stage. Rest of the stages to be defaulted to 0

      */
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE ctr_instal <> 0
       AND nam_stage IN ('IOI', 'MOR', 'PMI')
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE ctr_instal <> 0
       AND nam_stage IN ('IOI', 'MOR', 'PMI')
       AND migration_source = 'BRNET';

  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23014,
       'LN',
       var_l_table_name,
       'CTR_INSTAL',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'CTR_INSTAL should be zero for IOI,MOR,PMI');
  END;
  BEGIN

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23014,
       'MFI',
       var_l_table_name,
       'CTR_INSTAL',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'CTR_INSTAL should be zero for IOI,MOR,PMI');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23014, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE ctr_instal <> 0
         AND nam_stage IN ('IOI', 'MOR', 'PMI')
         AND migration_source = 'CBS';

  END IF;
  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23014, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE ctr_instal <> 0
         AND nam_stage IN ('IOI', 'MOR', 'PMI')
         AND migration_source = 'BRNET';

  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23015;
  /*consis 23015: CTR_INSTAL should not be zero for EPI*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE ctr_instal = 0
       AND nam_stage IN ('EPI')
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE ctr_instal = 0
       AND nam_stage IN ('EPI')
       AND migration_source = 'BRNET';

  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23015,
       'LN',
       var_l_table_name,
       'CTR_INSTAL',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'CTR_INSTAL should not be zero for EPI');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23015,
       'MFI',
       var_l_table_name,
       'CTR_INSTAL',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'CTR_INSTAL should not be zero for EPI');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23015, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE ctr_instal = 0
         AND NAM_STAGE in ('EPI', 'EMI')
         and rownum <= 10 --sample 10 records esaf_changes;
         AND migration_source = 'CBS';

  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23015, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE ctr_instal = 0
         AND NAM_STAGE in ('EPI', 'EMI')
         and rownum <= 10 --sample 10 records esaf_changes;
         AND migration_source = 'BRNET';

  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23016;
  /*consis 23016: CTR_INT must be grater than zero*/

  /*
  No of interest Installments.
  Note Value as per STAGE:
      . IOI and EPI --> no of month between DAT_STAGE_START and DAT_STAGE_END
      . PMI /MOR -> 0
  Bank to provide
  */
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE ctr_int <= 0
       and NAM_STAGE in ('EPI', 'EMI')
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE ctr_int <= 0
       and NAM_STAGE in ('EPI', 'EMI')
       AND migration_source = 'BRNET';
    -- esaf_changes
    /*FROM co_ln_acct_schedule a, co_ln_Acct_dtls b
    WHERE a.ctr_instal <= 0
      and a.NAM_STAGE in ('EPI', 'EMI')
      and a.cod_AccT_no = b.cod_Acct_no
      and b.cod_Acct_Stat not in (1, 5, 11);
     */
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23016,
       'LN',
       var_l_table_name,
       'CTR_INT',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'CTR_INT must be greater than zero For EPI/EMI stages');

  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23016,
       'MFI',
       var_l_table_name,
       'CTR_INT',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'CTR_INT must be greater than zero For EPI/EMI stages');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23016, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE ctr_instal <= 0
         and NAM_STAGE in ('EPI', 'EMI')
         and rownum <= 10 --sample 10 records esaf_changes;
         AND migration_source = 'CBS';
  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23016, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE ctr_instal <= 0
         and NAM_STAGE in ('EPI', 'EMI')
         and rownum <= 10 --sample 10 records esaf_changes;
         AND migration_source = 'BRNET';
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23017;
  /*consis 23017: DAT_FIRST_REST must be equal to DAT_FIRST_INSTAL*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE dat_first_rest <> dat_first_instal
       AND migration_source = 'CBS';

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE dat_first_rest <> dat_first_instal
       AND migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23017,
       'LN',
       var_l_table_name,
       'DAT_FIRST_REST',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'DAT_FIRST_REST must be equal to DAT_FIRST_INSTAL');
  END;
  BEGIN

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23017,
       'MFI',
       var_l_table_name,
       'DAT_FIRST_REST',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'DAT_FIRST_REST must be equal to DAT_FIRST_INSTAL');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       23017, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE dat_first_rest <> dat_first_instal
         and rownum <= 10 --sample 10 records esaf_changes;
         AND migration_source = 'CBS';
    --and NAM_STAGE in ('EPI') ;
  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       23017, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE dat_first_rest <> dat_first_instal
         and rownum <= 10 --sample 10 records esaf_changes;
         AND migration_source = 'BRNET';
    --and NAM_STAGE in ('EPI') ;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23018;
  /*consis 23018: COD_INSTAL_DATEPART cannot be grater than 31 and less than 1*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE cod_instal_datepart > 31
       AND cod_instal_datepart < 1
       AND migration_source = 'CBS';
    --and NAM_STAGE in ('EPI') ;

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE cod_instal_datepart > 31
       AND cod_instal_datepart < 1
       AND migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23018,
       'LN',
       var_l_table_name,
       'COD_INSTAL_DATEPART',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'COD_INSTAL_DATEPART cannot be grater than 31 and less than 1');
  END;
  BEGIN

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23018,
       'MFI',
       var_l_table_name,
       'COD_INSTAL_DATEPART',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'COD_INSTAL_DATEPART cannot be grater than 31 and less than 1');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       23018, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE cod_instal_datepart > 31
         AND cod_instal_datepart < 1
         AND migration_source = 'CBS';

  END IF;
  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       23018, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE cod_instal_datepart > 31
         AND cod_instal_datepart < 1
         AND migration_source = 'BRNET';

  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23019;
  /*consis 23019: COD_CCY must be equivalent to INR*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE cod_ccy NOT IN (SELECT cod_ccy
                             FROM cbsfchost.ba_ccy_code
                            WHERE nam_ccy_short = 'INR'
                              AND flg_mnt_status = 'A')
       AND migration_source = 'CBS';
    --and NAM_STAGE in ('EPI') ;

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE cod_ccy NOT IN (SELECT cod_ccy
                             FROM cbsfchost.ba_ccy_code
                            WHERE nam_ccy_short = 'INR'
                              AND flg_mnt_status = 'A')
       AND migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23019,
       'LN',
       var_l_table_name,
       'COD_CCY',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'COD_CCY must be equivalent to INR');
  END;
  BEGIN

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23019,
       'MFI',
       var_l_table_name,
       'COD_CCY',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'COD_CCY must be equivalent to INR');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       23019, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE cod_ccy NOT IN (SELECT cod_ccy
                               FROM cbsfchost.ba_ccy_code
                              WHERE nam_ccy_short = 'INR'
                                AND flg_mnt_status = 'A')
         AND migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       23019, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE cod_ccy NOT IN (SELECT cod_ccy
                               FROM cbsfchost.ba_ccy_code
                              WHERE nam_ccy_short = 'INR'
                                AND flg_mnt_status = 'A')
         AND migration_source = 'CBS';

  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23021;
  /*consis 23021: */

  /*Accounts present in schedule but not present in ln_Acct_dtls : consis 23021*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule
     WHERE cod_Acct_no NOT IN
           (SELECT cod_ACct_no
              FROM co_ln_acct_dtls
             WHERE flg_mnt_status = 'A')
       AND migration_source = 'CBS';

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule
     WHERE cod_Acct_no NOT IN
           (SELECT cod_ACct_no
              FROM co_ln_acct_dtls
             WHERE flg_mnt_status = 'A')
       AND migration_source = 'BRNET';

  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23021,
       'LN',
       var_l_table_name,
       'cod_Acct_no',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Accounts present in schedule but not present in loan master (ln_Acct_dtls)');

  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23021,
       'MFI',
       var_l_table_name,
       'cod_Acct_no',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Accounts present in schedule but not present in loan master (ln_Acct_dtls)');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       23021, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE cod_Acct_no NOT IN
             (SELECT cod_ACct_no
                FROM co_ln_acct_dtls
               WHERE flg_mnt_status = 'A')
         AND migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       23021, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE cod_Acct_no NOT IN
             (SELECT cod_ACct_no
                FROM co_ln_acct_dtls
               WHERE flg_mnt_status = 'A')
         AND migration_source = 'BRNET';

  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23022;
  /*Accounts present in ln_Acct_dtls that are absent in schedule : consis 23022*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_Acct_no NOT IN (SELECT cod_acct_no FROM co_ln_acct_schedule)
          --            AND cod_Acct_no NOT IN (SELECT cod_acct_no FROM conv_ln_x_acct_exclude WHERE consis_no = 24015) --ADDEd BY akSHAY ON 2107 aAVAS REQUEST
       AND ctr_disb > 0 --FA : 22-Mar-2024 Run : Check for disbursed cases only
       AND migration_source = 'CBS';

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_Acct_no NOT IN (SELECT cod_acct_no FROM co_ln_acct_schedule)
          --            AND cod_Acct_no NOT IN (SELECT cod_acct_no FROM conv_ln_x_acct_exclude WHERE consis_no = 24015) --ADDEd BY akSHAY ON 2107 aAVAS REQUEST
       AND ctr_disb > 0 --FA : 22-Mar-2024 Run : Check for disbursed cases only
       AND migration_source = 'BRNET';
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23022,
       'LN',
       var_l_table_name,
       'COD_ACCT_NO',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Accounts present in loan master (ln_Acct_dtls) that are absent in schedule.');
  END;
  BEGIN

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23022,
       'MFI',
       var_l_table_name,
       'COD_ACCT_NO',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Accounts present in loan master (ln_Acct_dtls) that are absent in schedule.');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       23022, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_dtls
       WHERE flg_mnt_status = 'A'
         AND cod_Acct_no NOT IN
             (SELECT cod_acct_no FROM co_ln_acct_schedule)
         AND ctr_disb > 0 --FA : 22-Mar-2024 Run : Check for disbursed cases only
         AND migration_source = 'CBS';

  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       23022, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_dtls
       WHERE flg_mnt_status = 'A'
         AND cod_Acct_no NOT IN
             (SELECT cod_acct_no FROM co_ln_acct_schedule)
         AND ctr_disb > 0 --FA : 22-Mar-2024 Run : Check for disbursed cases only
         AND migration_source = 'BRNET';

  END IF;
  commit;

  var_l_consis_no := 23023;
  /*
  No of interest Installments.
  Note Value as per STAGE:
      . IOI and EPI --> no of month between DAT_STAGE_START and DAT_STAGE_END
      . PMI /MOR -> 0
  Bank to provide
  */
  --capri_change-- already 23013 have it

  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE (ctr_instal > 0 OR ctr_instal < 0)
       and NAM_STAGE in ('PMI', 'MOR', 'IOI')
       AND migration_source = 'CBS';

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE (ctr_instal > 0 OR ctr_instal < 0)
       and NAM_STAGE in ('PMI', 'MOR', 'IOI')
       AND migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23023,
       'LN',
       var_l_table_name,
       'ctr_instal',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'CTR_INT must be equal to zero for PMI,MOR, and IOI stages');

  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23023,
       'MFI',
       var_l_table_name,
       'ctr_instal',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'CTR_INT must be equal to zero for PMI,MOR, and IOI stages');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       23023, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE (ctr_instal > 0 OR ctr_instal < 0)
         and NAM_STAGE in ('PMI', 'MOR', 'IOI')
         AND migration_source = 'CBS';
  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       23023, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE (ctr_instal > 0 OR ctr_instal < 0)
         and NAM_STAGE in ('PMI', 'MOR', 'IOI')
         AND migration_source = 'BRNET';
  END IF;
  --------------
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23024;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE DAT_FIRST_CHARGE IS NULL
        OR DAT_FIRST_CHARGE <> '01-Jan-1950'
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE DAT_FIRST_CHARGE IS NULL
        OR DAT_FIRST_CHARGE <> '01-Jan-1950'
       AND migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23024,
       'LN',
       var_l_table_name,
       'DAT_FIRST_CHARGE',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'DAT_FIRST_CHARGE should be 01-Jan-1950 and should not be blank');
  END;
  BEGIN

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23024,
       'MFI',
       var_l_table_name,
       'DAT_FIRST_CHARGE',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'DAT_FIRST_CHARGE should be 01-Jan-1950 and should not be blank');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23024, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE DAT_FIRST_CHARGE IS NULL
          OR DAT_FIRST_CHARGE <> '01-Jan-1950'
         and rownum <= 10 --sample 10 records esaf_changes ;
         AND migration_source = 'CBS';
  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23024, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE DAT_FIRST_CHARGE IS NULL
          OR DAT_FIRST_CHARGE <> '01-Jan-1950'
         and rownum <= 10 --sample 10 records esaf_changes ;
         AND migration_source = 'BRNET';
  END IF;
  --------------------
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23025;
  --FRQ_CHARGE should not be blank. It should be equal to 0.
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE FRQ_CHARGE IS NULL
        OR FRQ_CHARGE <> 0
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE FRQ_CHARGE IS NULL
        OR FRQ_CHARGE <> 0
       AND migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23025,
       'LN',
       var_l_table_name,
       'FRQ_CHARGE',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'FRQ_CHARGE should not be blank. It should be equal to 0.');
  END;
  BEGIN

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23025,
       'MFI',
       var_l_table_name,
       'FRQ_CHARGE',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'FRQ_CHARGE should not be blank. It should be equal to 0.');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23025, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE FRQ_CHARGE IS NULL
          OR FRQ_CHARGE <> 0
         AND migration_source = 'CBS';
  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23025, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE FRQ_CHARGE IS NULL
          OR FRQ_CHARGE <> 0
         AND migration_source = 'BRNET';
  END IF;

  --------------------
  --------------------
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23026;
  --CTR_CHARGE should not be blank. It should be equal to 0.
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE CTR_CHARGE IS NULL
        OR CTR_CHARGE <> 0
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE CTR_CHARGE IS NULL
        OR CTR_CHARGE <> 0
       AND migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23026,
       'LN',
       var_l_table_name,
       'CTR_CHARGE',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'CTR_CHARGE should not be blank. It should be equal to 0.');

  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23026,
       'MFI',
       var_l_table_name,
       'CTR_CHARGE',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'CTR_CHARGE should not be blank. It should be equal to 0.');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23026, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE CTR_CHARGE IS NULL
          OR CTR_CHARGE <> 0
         AND migration_source = 'CBS';
  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23026, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE CTR_CHARGE IS NULL
          OR CTR_CHARGE <> 0
         AND migration_source = 'BRNET';
  END IF;
  --------------------

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23027;
  --FRQ_INT_COMP should not be blank. It should be equal to 0.
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE FRQ_INT_COMP IS NULL
        OR FRQ_INT_COMP <> 0
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE FRQ_INT_COMP IS NULL
        OR FRQ_INT_COMP <> 0
       AND migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23027,
       'LN',
       var_l_table_name,
       'FRQ_INT_COMP',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'FRQ_INT_COMP should not be blank. It should be equal to 0.');
  END;
  BEGIN

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23027,
       'MFI',
       var_l_table_name,
       'FRQ_INT_COMP',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'FRQ_INT_COMP should not be blank. It should be equal to 0.');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23027, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE FRQ_INT_COMP IS NULL
          OR FRQ_INT_COMP <> 0
         AND migration_source = 'CBS';
  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23027, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE FRQ_INT_COMP IS NULL
          OR FRQ_INT_COMP <> 0
         AND migration_source = 'BRNET';
  END IF;
  --------------------

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23028;
  --DAT_FIRST_COMP should not be blank. It should be equal to 01-Jan-1950.
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a
     WHERE DAT_FIRST_COMP IS NULL
        OR DAT_FIRST_COMP <> '01-Jan-1950'
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a
     WHERE DAT_FIRST_COMP IS NULL
        OR DAT_FIRST_COMP <> '01-Jan-1950'
       AND migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23028,
       'LN',
       var_l_table_name,
       'DAT_FIRST_COMP',
       'ap_cons_ln_acct_schedule',
       var_l_count,
       'DAT_FIRST_COMP should not be blank. It should be equal to 01-Jan-1950.');
  END;
  BEGIN

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23028,
       'MFI',
       var_l_table_name,
       'DAT_FIRST_COMP',
       'ap_cons_ln_acct_schedule',
       var_l_count1,
       'DAT_FIRST_COMP should not be blank. It should be equal to 01-Jan-1950.');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23028, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE DAT_FIRST_COMP IS NULL
          OR DAT_FIRST_COMP <> '01-Jan-1950'
         AND migration_source = 'CBS';
  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ PARALLEL(4) */
       23028, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
       WHERE DAT_FIRST_COMP IS NULL
          OR DAT_FIRST_COMP <> '01-Jan-1950'
         AND migration_source = 'BRNET';
  END IF;

  ---------------------------------------

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 23029;
  /*consis 23029: COD_INSTAL_DATEPART should not be blank.*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a, co_ln_acct_dtls C --FA:26-APR-24, Added to exclude closed accounts
     WHERE A.COD_ACCT_NO = C.COD_ACCT_NO
       AND cod_instal_datepart IS NULL
       AND C.COD_ACCT_STAT <> 1
       AND a.migration_source = 'CBS';

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a, co_ln_acct_dtls C --FA:26-APR-24, Added to exclude closed accounts
     WHERE A.COD_ACCT_NO = C.COD_ACCT_NO
       AND cod_instal_datepart IS NULL
       AND C.COD_ACCT_STAT <> 1
       AND a.migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23029,
       'LN',
       var_l_table_name,
       'COD_INSTAL_DATEPART',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'COD_INSTAL_DATEPART should not be blank.');

  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (23029,
       'MFI',
       var_l_table_name,
       'COD_INSTAL_DATEPART',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'COD_INSTAL_DATEPART should not be blank.');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       23029, a.cod_acct_no, a.migration_source
        FROM co_ln_acct_schedule A, co_ln_acct_dtls C --FA:26-APR-24, Added to exclude closed accounts
       WHERE A.COD_ACCT_NO = C.COD_ACCT_NO
         AND cod_instal_datepart IS NULL
         AND C.COD_ACCT_STAT <> 1
         AND a.migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       23029, a.cod_acct_no, a.migration_source
        FROM co_ln_acct_schedule A, co_ln_acct_dtls C --FA:26-APR-24, Added to exclude closed accounts
       WHERE A.COD_ACCT_NO = C.COD_ACCT_NO
         AND cod_instal_datepart IS NULL
         AND C.COD_ACCT_STAT <> 1
         AND a.migration_source = 'BRNET';

  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  var_l_consis_no := 23035;
  /*consis 23035: Accounts where IOI stage dat_first_int = 1-jan-1950*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a, co_ln_acct_dtls b
     WHERE a.cod_Acct_no = b.cod_Acct_no
          --    AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'IOI')
       AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
       AND cod_Acct_Stat <> 1
          --    AND a.flg_mnt_status <> 'X'
       AND dat_first_int = '1-jan-1950'
       AND a.migration_source = 'CBS';

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a, co_ln_acct_dtls b
     WHERE a.cod_Acct_no = b.cod_Acct_no
          --    AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'IOI')
       AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
       AND cod_Acct_Stat <> 1
          --    AND a.flg_mnt_status <> 'X'
       AND dat_first_int = '1-jan-1950'
       AND a.migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'DAT_FIRST_INT',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Accounts where IOI stage dat_first_int = 1-jan-1950');
  END;
  BEGIN

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'DAT_FIRST_INT',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Accounts where IOI stage dat_first_int = 1-jan-1950');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
        FROM co_ln_acct_schedule a, co_ln_acct_dtls b
       WHERE a.cod_Acct_no = b.cod_Acct_no
            --      AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'IOI')
         AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
         AND cod_Acct_Stat <> 1
            --      AND a.flg_mnt_status <> 'X'
         AND dat_first_int = '1-jan-1950'
         AND a.migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
        FROM co_ln_acct_schedule a, co_ln_acct_dtls b
       WHERE a.cod_Acct_no = b.cod_Acct_no
            --      AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'IOI')
         AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
         AND cod_Acct_Stat <> 1
            --      AND a.flg_mnt_status <> 'X'
         AND dat_first_int = '1-jan-1950'
         AND a.migration_source = 'BRNET';
  END IF;
  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  var_l_consis_no := 23036;
  /*consis 23036: Accounts where IOI stage ctr_int = 0*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a, co_ln_acct_dtls b
     WHERE a.cod_Acct_no = b.cod_Acct_no
       AND dat_Stage_start < var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
       AND dat_Stage_end >= var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
          --    AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'IOI')
       AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
       AND cod_Acct_Stat <> 1
          --    AND a.flg_mnt_status <> 'X'
       AND ctr_int = 0
       AND a.migration_source = 'CBS';

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a, co_ln_acct_dtls b
     WHERE a.cod_Acct_no = b.cod_Acct_no
       AND dat_Stage_start < var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
       AND dat_Stage_end >= var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
          --    AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'IOI')
       AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
       AND cod_Acct_Stat <> 1
          --    AND a.flg_mnt_status <> 'X'
       AND ctr_int = 0
       AND a.migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'CTR_INT',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Accounts where IOI stage ctr_int = 0');
  END;
  BEGIN

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'CTR_INT',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Accounts where IOI stage ctr_int = 0');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
        FROM co_ln_acct_schedule a, co_ln_acct_dtls b
       WHERE a.cod_Acct_no = b.cod_Acct_no
         AND dat_Stage_start < var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
         AND dat_Stage_end >= var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
            --      AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'IOI')
         AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
         AND cod_Acct_Stat <> 1
            --      AND a.flg_mnt_status <> 'X'
         AND ctr_int = 0
         AND a.migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
        FROM co_ln_acct_schedule a, co_ln_acct_dtls b
       WHERE a.cod_Acct_no = b.cod_Acct_no
         AND dat_Stage_start < var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
         AND dat_Stage_end >= var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
            --      AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'IOI')
         AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
         AND cod_Acct_Stat <> 1
            --      AND a.flg_mnt_status <> 'X'
         AND ctr_int = 0
         AND a.migration_source = 'BRNET';
  END IF;
  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  var_l_consis_no := 23037;
  /*consis 23037: Accounts where cod_instal_datepart not in (5,10,15)*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a, co_ln_acct_dtls b
     WHERE a.cod_Acct_no = b.cod_Acct_no
          --    AND cod_instal_rule NOT IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'PMI')
       AND nam_stage != 'PMI' ----FA : 26-Mar-2024 Run :  Since on CONV
       AND dat_Stage_end >= var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
       AND cod_Acct_Stat <> 1
          --    AND a.flg_mnt_status <> 'X'
       AND cod_instal_datepart NOT IN (5, 10, 15)
       and 1 <> 1 /* dropping this consis. It was specific to bandhan. Not reqd ni in Esaf.*/
       AND a.migration_source = 'CBS';

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a, co_ln_acct_dtls b
     WHERE a.cod_Acct_no = b.cod_Acct_no
          --    AND cod_instal_rule NOT IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'PMI')
       AND nam_stage != 'PMI' ----FA : 26-Mar-2024 Run :  Since on CONV
       AND dat_Stage_end >= var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
       AND cod_Acct_Stat <> 1
          --    AND a.flg_mnt_status <> 'X'
       AND cod_instal_datepart NOT IN (5, 10, 15)
       and 1 <> 1 /* dropping this consis. It was specific to bandhan. Not reqd ni in Esaf.*/
       AND a.migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'COD_INSTAL_DATEPART',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Accounts where cod_instal_datepart not in (5,10,15)');
  END;
  BEGIN

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'COD_INSTAL_DATEPART',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Accounts where cod_instal_datepart not in (5,10,15)');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
        FROM co_ln_acct_schedule a, co_ln_acct_dtls b
       WHERE a.cod_Acct_no = b.cod_Acct_no
            --      AND cod_instal_rule NOT IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'PMI')
         AND nam_stage != 'PMI' ----FA : 26-Mar-2024 Run :  Since on CONV
         AND dat_Stage_end >= var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
         AND cod_Acct_Stat <> 1
            --      AND a.flg_mnt_status <> 'X'
         AND cod_instal_datepart NOT IN (5, 10, 15)
         AND a.migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
        FROM co_ln_acct_schedule a, co_ln_acct_dtls b
       WHERE a.cod_Acct_no = b.cod_Acct_no
            --      AND cod_instal_rule NOT IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'PMI')
         AND nam_stage != 'PMI' ----FA : 26-Mar-2024 Run :  Since on CONV
         AND dat_Stage_end >= var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
         AND cod_Acct_Stat <> 1
            --      AND a.flg_mnt_status <> 'X'
         AND cod_instal_datepart NOT IN (5, 10, 15)
         AND a.migration_source = 'BRNET';

  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  var_l_consis_no := 23038;
  /*consis 23038: Accounts where EMI stage where dat_first_int = 1-jan-1950 or dat_first_instal =  1-jan-1950*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a, co_ln_acct_dtls b
     WHERE a.cod_Acct_no = b.cod_Acct_no
          --    AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'EPI')
       AND nam_stage IN ('EPI', 'EMI') ----FA : 26-Mar-2024 Run :  Since on CONV
       AND cod_Acct_Stat <> 1
          --    AND a.flg_mnt_status <> 'X'
       AND (dat_first_int = '1-jan-1950' OR dat_first_instal = '1-jan-1950' OR
           dat_first_int <> dat_first_instal)
       AND a.migration_source = 'CBS';

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a, co_ln_acct_dtls b
     WHERE a.cod_Acct_no = b.cod_Acct_no
          --    AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'EPI')
       AND nam_stage IN ('EPI', 'EMI') ----FA : 26-Mar-2024 Run :  Since on CONV
       AND cod_Acct_Stat <> 1
          --    AND a.flg_mnt_status <> 'X'
       AND (dat_first_int = '1-jan-1950' OR dat_first_instal = '1-jan-1950' OR
           dat_first_int <> dat_first_instal)
       AND a.migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'DAT_FIRST_INSTAL',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Accounts where EMI stage where dat_first_int = 1-jan-1950 or dat_first_instal =  1-jan-1950');
  END;
  BEGIN

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'DAT_FIRST_INSTAL',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Accounts where EMI stage where dat_first_int = 1-jan-1950 or dat_first_instal =  1-jan-1950');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
        FROM co_ln_acct_schedule a, co_ln_acct_dtls b
       WHERE a.cod_Acct_no = b.cod_Acct_no
            --      AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'EPI')
         AND nam_stage IN ('EPI', 'EMI') ----FA : 26-Mar-2024 Run :  Since on CONV
         AND cod_Acct_Stat <> 1
            --      AND a.flg_mnt_status <> 'X'
         AND (dat_first_int = '1-jan-1950' OR
             dat_first_instal = '1-jan-1950' OR
             dat_first_int <> dat_first_instal)
         AND a.migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
        FROM co_ln_acct_schedule a, co_ln_acct_dtls b
       WHERE a.cod_Acct_no = b.cod_Acct_no
            --      AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'EPI')
         AND nam_stage IN ('EPI', 'EMI') ----FA : 26-Mar-2024 Run :  Since on CONV
         AND cod_Acct_Stat <> 1
            --      AND a.flg_mnt_status <> 'X'
         AND (dat_first_int = '1-jan-1950' OR
             dat_first_instal = '1-jan-1950' OR
             dat_first_int <> dat_first_instal)
         AND a.migration_source = 'BRNET';

  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  var_l_consis_no := 23039;
  /*consis 23039: Accounts where EMI stage where ctr_int = 0 or ctr_instal= 0*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a, co_ln_acct_dtls b
     WHERE a.cod_Acct_no = b.cod_Acct_no
       AND dat_Stage_start < var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
       AND dat_Stage_end >= var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
          --    AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'EPI')
       AND nam_stage IN ('EPI', 'EMI') ----FA : 26-Mar-2024 Run :  Since on CONV
       AND cod_Acct_Stat <> 1
          --    AND a.flg_mnt_status <> 'X'
       AND (ctr_int = 0 OR a.ctr_instal = 0)
       AND a.migration_source = 'CBS';

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a, co_ln_acct_dtls b
     WHERE a.cod_Acct_no = b.cod_Acct_no
       AND dat_Stage_start < var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
       AND dat_Stage_end >= var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
          --    AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'EPI')
       AND nam_stage IN ('EPI', 'EMI') ----FA : 26-Mar-2024 Run :  Since on CONV
       AND cod_Acct_Stat <> 1
          --    AND a.flg_mnt_status <> 'X'
       AND (ctr_int = 0 OR a.ctr_instal = 0)
       AND a.migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'CTR_INSTAL',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Accounts where EMI stage where ctr_int = 0 or ctr_instal= 0');

  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'CTR_INSTAL',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Accounts where EMI stage where ctr_int = 0 or ctr_instal= 0');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
        FROM co_ln_acct_schedule a, co_ln_acct_dtls b
       WHERE a.cod_Acct_no = b.cod_Acct_no
         AND dat_Stage_start < var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
         AND dat_Stage_end >= var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
            --      AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'EPI')
         AND nam_stage IN ('EPI', 'EMI') ----FA : 26-Mar-2024 Run :  Since on CONV
         AND cod_Acct_Stat <> 1
            --      AND a.flg_mnt_status <> 'X'
         AND (ctr_int = 0 OR a.ctr_instal = 0)
         AND a.migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
        FROM co_ln_acct_schedule a, co_ln_acct_dtls b
       WHERE a.cod_Acct_no = b.cod_Acct_no
         AND dat_Stage_start < var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
         AND dat_Stage_end >= var_l_dat_process --(SELECT dat_process FROM ba_bank_mast)
            --      AND cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method = 'EPI')
         AND nam_stage IN ('EPI', 'EMI') ----FA : 26-Mar-2024 Run :  Since on CONV
         AND cod_Acct_Stat <> 1
            --      AND a.flg_mnt_status <> 'X'
         AND (ctr_int = 0 OR a.ctr_instal = 0)
         AND a.migration_source = 'BRNET';

  END IF;
  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  var_l_consis_no := 23040;
  /*consis 23040: Accounts in IOI stage where dat_first_int < dat_stage_start*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a, co_ln_acct_dtls b
     WHERE a.cod_acct_no = b.cod_acct_no
       AND b.dat_of_maturity > var_mig_date
       AND b.ctr_disb > 0
       AND b.cod_acct_stat <> 1
       AND a.dat_stage_end > var_mig_date
          --    AND a.cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method ='IOI' AND flg_mnt_status = 'A')
       AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
       AND dat_first_int < dat_stage_start
       AND a.migration_source = 'CBS';

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a, co_ln_acct_dtls b
     WHERE a.cod_acct_no = b.cod_acct_no
       AND b.dat_of_maturity > var_mig_date
       AND b.ctr_disb > 0
       AND b.cod_acct_stat <> 1
       AND a.dat_stage_end > var_mig_date
          --    AND a.cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method ='IOI' AND flg_mnt_status = 'A')
       AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
       AND dat_first_int < dat_stage_start
       AND a.migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'DAT_FIRST_INT',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Accounts in IOI stage where dat_first_int < dat_stage_start');
  END;
  BEGIN

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'DAT_FIRST_INT',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Accounts in IOI stage where dat_first_int < dat_stage_start');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
        FROM co_ln_acct_schedule a, co_ln_acct_dtls b
       WHERE a.cod_acct_no = b.cod_acct_no
         AND b.dat_of_maturity > var_mig_date
         AND b.ctr_disb > 0
         AND b.cod_acct_stat <> 1
         AND a.dat_stage_end > var_mig_date
            --      AND a.cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method ='IOI' AND flg_mnt_status = 'A')
         AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
         AND dat_first_int < dat_stage_start
         AND a.migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
        FROM co_ln_acct_schedule a, co_ln_acct_dtls b
       WHERE a.cod_acct_no = b.cod_acct_no
         AND b.dat_of_maturity > var_mig_date
         AND b.ctr_disb > 0
         AND b.cod_acct_stat <> 1
         AND a.dat_stage_end > var_mig_date
            --      AND a.cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method ='IOI' AND flg_mnt_status = 'A')
         AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
         AND dat_first_int < dat_stage_start
         AND a.migration_source = 'BRNET';

  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  --  var_l_consis_no := 23041;
  --     /*consis 23041: Accounts where dat_first_int or cod_instal_datepart not in (5,10,15)(future cases)*/
  --    BEGIN
  --        SELECT /*+ parallel(4) */
  --            COUNT(1)
  --    INTO var_l_count
  --    FROM co_ln_acct_schedule a,co_ln_acct_dtls b
  --    WHERE a.cod_acct_no = b.cod_acct_no
  ----    AND b.flg_mnt_status <> 'X'
  --    AND b.dat_of_maturity > var_mig_date
  --    AND b.ctr_disb > 0
  --    AND b.cod_acct_stat <> 1
  --    AND a.dat_stage_end > var_mig_date
  ----    AND a.cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method ='IOI' AND flg_mnt_status = 'A')
  --        AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
  --    AND (datepart('DD',dat_first_int) NOT IN (5,10,15) OR cod_instal_datepart NOT IN (5,10,15));
  --    END;
  --
  --    BEGIN
  --        INSERT INTO co_ln_consis (
  --            cod_consis_no,
  --            nam_module,
  --            nam_table,
  --            nam_column,
  --            nam_consis_func,
  --            consis_count,
  --            desc_cons
  --        ) VALUES (
  --            var_l_consis_no,
  --            'LN',
  --            var_l_table_name,
  --            'DAT_FIRST_INT',
  --            'ap_cons_LN_ACCT_SCHEDULE',
  --            var_l_count,
  --            'Accounts where dat_first_int or cod_instal_datepart not in (5,10,15)(future cases)'
  --        );
  --
  --    END;
  --
  --    IF ( var_l_count > 0 ) THEN
  --        INSERT INTO co_ln_consis_acct (
  --            cod_consis_no,
  --            cod_acct_no
  --        )
  --            SELECT /*+ parallel(4) */
  --                var_l_consis_no,
  --        a.cod_acct_no
  --      FROM co_ln_acct_schedule a,co_ln_acct_dtls b
  --      WHERE a.cod_acct_no = b.cod_acct_no
  ----      AND b.flg_mnt_status <> 'X'
  --      AND b.dat_of_maturity > var_mig_date
  --      AND b.ctr_disb > 0
  --      AND b.cod_acct_stat <> 1
  --      AND a.dat_stage_end > var_mig_date
  ----      AND a.cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method ='IOI' AND flg_mnt_status = 'A')
  --            AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
  --      AND (datepart('DD',dat_first_int) NOT IN (5,10,15) OR cod_instal_datepart NOT IN (5,10,15));
  --
  --    END IF;
  --
  --  COMMIT;
  -- ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
  --                     var_l_function_name || '# Stream = ' ||
  --                     var_pi_stream); --after each consis

  var_l_consis_no := 23042;
  /*consis 23042: Accounts where instalment day part of dat_first_int != dat_stage_end (future cases)*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule a, co_ln_acct_dtls b
     WHERE a.cod_acct_no = b.cod_acct_no
       AND b.dat_of_maturity > var_mig_date
       AND b.ctr_disb > 0
       AND b.cod_acct_stat <> 1
       AND a.dat_stage_end > var_mig_date
          --    AND a.cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method ='IOI' AND flg_mnt_status = 'A')
       AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
       AND cbsfchost.datepart('DD', dat_stage_end) <>
           cbsfchost.datepart('DD', dat_first_int)
       AND a.migration_source = 'CBS';

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule a, co_ln_acct_dtls b
     WHERE a.cod_acct_no = b.cod_acct_no
       AND b.dat_of_maturity > var_mig_date
       AND b.ctr_disb > 0
       AND b.cod_acct_stat <> 1
       AND a.dat_stage_end > var_mig_date
          --    AND a.cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method ='IOI' AND flg_mnt_status = 'A')
       AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
       AND cbsfchost.datepart('DD', dat_stage_end) <>
           cbsfchost.datepart('DD', dat_first_int)
       AND a.migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'DAT_FIRST_INT',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Accounts where instalment day part of dat_first_int != dat_stage_end (future cases)');
  END;
  BEGIN

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'DAT_FIRST_INT',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Accounts where instalment day part of dat_first_int != dat_stage_end (future cases)');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
        FROM co_ln_acct_schedule a, co_ln_acct_dtls b
       WHERE a.cod_acct_no = b.cod_acct_no
         AND b.dat_of_maturity > var_mig_date
         AND b.ctr_disb > 0
         AND b.cod_acct_stat <> 1
         AND a.dat_stage_end > var_mig_date
            --      AND a.cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method ='IOI' AND flg_mnt_status = 'A')
         AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
         AND cbsfchost.datepart('DD', dat_stage_end) <>
             cbsfchost.datepart('DD', dat_first_int)
         AND a.migration_source = 'CBS';

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
        FROM co_ln_acct_schedule a, co_ln_acct_dtls b
       WHERE a.cod_acct_no = b.cod_acct_no
         AND b.dat_of_maturity > var_mig_date
         AND b.ctr_disb > 0
         AND b.cod_acct_stat <> 1
         AND a.dat_stage_end > var_mig_date
            --      AND a.cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method ='IOI' AND flg_mnt_status = 'A')
         AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
         AND cbsfchost.datepart('DD', dat_stage_end) <>
             cbsfchost.datepart('DD', dat_first_int)
         AND a.migration_source = 'BRNET';

  END IF;
  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  var_l_consis_no := 23043;
  /*consis 23043: Accounts in IOI stage in migration month, but schedule details not available for upcoming month (future cases)*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM (SELECT a.cod_Acct_no,
                   b.dat_last_charged,
                   b.dat_first_disb,
                   a.ctr_int,
                   dat_first_int,
                   cod_instal_datepart,
                   dat_stage_end
              FROM co_ln_acct_schedule a, co_ln_acct_dtls b
             WHERE a.cod_acct_no = b.cod_acct_no
               AND b.flg_mnt_status <> 'X'
               AND b.dat_of_maturity > var_mig_date
               AND b.ctr_disb > 0
               AND b.cod_acct_stat <> 1
               AND a.dat_stage_end > var_mig_date
               AND dat_Stage_start <= var_mig_date
                  -- AND a.cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method ='IOI' AND flg_mnt_status = 'A')
               AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
               AND NOT EXISTS (SELECT 1
                      FROM co_ln_acct_schedule_detls c
                     WHERE a.cod_Acct_no = c.cod_acct_no
                          --AND date_instal >= '1-Mar-2024' AND date_instal <= '31-Mar-2024' )
                       AND to_char(date_instal, 'MMYY') =
                           to_char(var_l_dat_process, 'MMYY'))
               AND a.migration_source = 'CBS');

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM (SELECT a.cod_Acct_no,
                   b.dat_last_charged,
                   b.dat_first_disb,
                   a.ctr_int,
                   dat_first_int,
                   cod_instal_datepart,
                   dat_stage_end
              FROM co_ln_acct_schedule a, co_ln_acct_dtls b
             WHERE a.cod_acct_no = b.cod_acct_no
               AND b.flg_mnt_status <> 'X'
               AND b.dat_of_maturity > var_mig_date
               AND b.ctr_disb > 0
               AND b.cod_acct_stat <> 1
               AND a.dat_stage_end > var_mig_date
               AND dat_Stage_start <= var_mig_date
                  -- AND a.cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method ='IOI' AND flg_mnt_status = 'A')
               AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
               AND NOT EXISTS (SELECT 1
                      FROM co_ln_acct_schedule_detls c
                     WHERE a.cod_Acct_no = c.cod_acct_no
                          --AND date_instal >= '1-Mar-2024' AND date_instal <= '31-Mar-2024' )
                       AND to_char(date_instal, 'MMYY') =
                           to_char(var_l_dat_process, 'MMYY'))
               AND a.migration_source = 'BRNET');

  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'DAT_FIRST_INT',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Accounts in IOI stage in migration month, but schedule details not available for upcoming month (future cases)');

  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'DAT_FIRST_INT',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Accounts in IOI stage in migration month, but schedule details not available for upcoming month (future cases)');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM (SELECT a.cod_Acct_no,
                     b.dat_last_charged,
                     b.dat_first_disb,
                     a.ctr_int,
                     dat_first_int,
                     cod_instal_datepart,
                     dat_stage_end,
                     a.MIGRATION_SOURCE
                FROM co_ln_acct_schedule a, co_ln_acct_dtls b
               WHERE a.cod_acct_no = b.cod_acct_no
                 AND b.flg_mnt_status <> 'X'
                 AND b.dat_of_maturity > var_mig_date
                 AND b.ctr_disb > 0
                 AND b.cod_acct_stat <> 1
                 AND a.dat_stage_end > var_mig_date
                 AND dat_Stage_start <= var_mig_date
                    -- AND a.cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method ='IOI' AND flg_mnt_status = 'A')
                 AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
                 AND NOT EXISTS
               (SELECT 1
                        FROM co_ln_acct_schedule_detls c
                       WHERE a.cod_Acct_no = c.cod_acct_no
                            --AND date_instal >= '1-Mar-2024' AND date_instal <= '31-Mar-2024' )
                         AND to_char(date_instal, 'MMYY') =
                             to_char(var_l_dat_process, 'MMYY'))
                 AND a.migration_source = 'CBS');

  END IF;
  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM (SELECT a.cod_Acct_no,
                     b.dat_last_charged,
                     b.dat_first_disb,
                     a.ctr_int,
                     dat_first_int,
                     cod_instal_datepart,
                     dat_stage_end,
                     a.MIGRATION_SOURCE
                FROM co_ln_acct_schedule a, co_ln_acct_dtls b
               WHERE a.cod_acct_no = b.cod_acct_no
                 AND b.flg_mnt_status <> 'X'
                 AND b.dat_of_maturity > var_mig_date
                 AND b.ctr_disb > 0
                 AND b.cod_acct_stat <> 1
                 AND a.dat_stage_end > var_mig_date
                 AND dat_Stage_start <= var_mig_date
                    -- AND a.cod_instal_rule IN (SELECT cod_inst_rule FROM ln_inst_rules WHERE cod_inst_calc_method ='IOI' AND flg_mnt_status = 'A')
                 AND nam_stage = 'IOI' ----FA : 26-Mar-2024 Run :  Since on CONV
                 AND NOT EXISTS
               (SELECT 1
                        FROM co_ln_acct_schedule_detls c
                       WHERE a.cod_Acct_no = c.cod_acct_no
                            --AND date_instal >= '1-Mar-2024' AND date_instal <= '31-Mar-2024' )
                         AND to_char(date_instal, 'MMYY') =
                             to_char(var_l_dat_process, 'MMYY'))
                 AND a.migration_source = 'BRNET');

  END IF;
  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  var_l_consis_no := 23044;
  /*consis 23044: Part Disbursed Accounts in EMI stage*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM (SELECT cod_acct_no
              FROM co_ln_acct_schedule s
             WHERE dat_stage_start <= var_mig_date
               AND dat_stage_end > var_mig_date
               AND nam_stage = 'EMI'
               AND EXISTS (SELECT 1
                      FROM vwe_x_conv_ln_basic_details v --FA:28-May-24 changed to refer to CONV
                     WHERE cod_acct_stat != 1
                       AND disb_status = 'P'
                       AND s.cod_acct_no = v.cod_acct_no_old)
               AND migration_source = 'CBS');

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM (SELECT cod_acct_no
              FROM co_ln_acct_schedule s
             WHERE dat_stage_start <= var_mig_date
               AND dat_stage_end > var_mig_date
               AND nam_stage = 'EMI'
               AND EXISTS (SELECT 1
                      FROM vwe_x_conv_ln_basic_details v --FA:28-May-24 changed to refer to CONV
                     WHERE cod_acct_stat != 1
                       AND disb_status = 'P'
                       AND s.cod_acct_no = v.cod_acct_no_old)
               AND migration_source = 'BRNET');
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'NAM_STAGE',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Part Disbursed Accounts in EMI stage');

  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'NAM_STAGE',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Part Disbursed Accounts in EMI stage');

  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM (SELECT cod_acct_no, MIGRATION_SOURCE
                FROM co_ln_acct_schedule s
               WHERE dat_stage_start <= var_mig_date
                 AND dat_stage_end > var_mig_date
                 AND nam_stage = 'EMI'
                 AND EXISTS (SELECT 1
                        FROM vwe_x_conv_ln_basic_details v --FA:28-May-24 changed to refer to CONV
                       WHERE cod_acct_stat != 1
                         AND disb_status = 'P'
                         AND s.cod_acct_no = v.cod_acct_no_old)
                 AND migration_source = 'CBS');

  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM (SELECT cod_acct_no, MIGRATION_SOURCE
                FROM co_ln_acct_schedule s
               WHERE dat_stage_start <= var_mig_date
                 AND dat_stage_end > var_mig_date
                 AND nam_stage = 'EMI'
                 AND EXISTS (SELECT 1
                        FROM vwe_x_conv_ln_basic_details v --FA:28-May-24 changed to refer to CONV
                       WHERE cod_acct_stat != 1
                         AND disb_status = 'P'
                         AND s.cod_acct_no = v.cod_acct_no_old)
                 AND migration_source = 'BRNET');

  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  var_l_consis_no := 23045;
  /*consis 23045: DAT_STAGE_START greater than DAT_LAST_CHARGED*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM (SELECT A.cod_acct_no,
                   A.dat_stage_start,
                   A.dat_stage_end,
                   B.dat_last_charged,
                   A.nam_stage
              FROM co_ln_acct_schedule A, co_ln_acct_dtls B
             WHERE A.cod_acct_no = B.cod_acct_no
               AND dat_stage_start <= var_l_dat_process
               AND dat_stage_end > var_l_dat_process
               AND dat_stage_start > dat_last_charged
               AND B.cod_acct_stat NOT IN (1, 5)
               AND B.flg_mnt_status = 'A'
               AND A.flg_mnt_status = 'A'
               AND A.nam_stage NOT IN ('PMI')
               AND a.migration_source = 'CBS');

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM (SELECT A.cod_acct_no,
                   A.dat_stage_start,
                   A.dat_stage_end,
                   B.dat_last_charged,
                   A.nam_stage
              FROM co_ln_acct_schedule A, co_ln_acct_dtls B
             WHERE A.cod_acct_no = B.cod_acct_no
               AND dat_stage_start <= var_l_dat_process
               AND dat_stage_end > var_l_dat_process
               AND dat_stage_start > dat_last_charged
               AND B.cod_acct_stat NOT IN (1, 5)
               AND B.flg_mnt_status = 'A'
               AND A.flg_mnt_status = 'A'
               AND A.nam_stage NOT IN ('PMI')
               AND a.migration_source = 'BRNET');
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'DAT_STAGE_START',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'DAT_STAGE_START greater than DAT_LAST_CHARGED');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'DAT_STAGE_START',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'DAT_STAGE_START greater than DAT_LAST_CHARGED');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM (SELECT A.cod_acct_no,
                     A.dat_stage_start,
                     A.dat_stage_end,
                     B.dat_last_charged,
                     A.nam_stage,
                     a.MIGRATION_SOURCE
                FROM co_ln_acct_schedule A, co_ln_acct_dtls B
               WHERE A.cod_acct_no = B.cod_acct_no
                 AND dat_stage_start <= var_l_dat_process
                 AND dat_stage_end > var_l_dat_process
                 AND dat_stage_start > dat_last_charged
                 AND B.cod_acct_stat NOT IN (1, 5)
                 AND B.flg_mnt_status = 'A'
                 AND A.flg_mnt_status = 'A'
                 AND A.nam_stage NOT IN ('PMI')
                 AND a.migration_source = 'CBS')
       WHERE rownum <= 10 --sample 10 records esaf_changes;
      ;
  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM (SELECT A.cod_acct_no,
                     A.dat_stage_start,
                     A.dat_stage_end,
                     B.dat_last_charged,
                     A.nam_stage,
                     a.MIGRATION_SOURCE
                FROM co_ln_acct_schedule A, co_ln_acct_dtls B
               WHERE A.cod_acct_no = B.cod_acct_no
                 AND dat_stage_start <= var_l_dat_process
                 AND dat_stage_end > var_l_dat_process
                 AND dat_stage_start > dat_last_charged
                 AND B.cod_acct_stat NOT IN (1, 5)
                 AND B.flg_mnt_status = 'A'
                 AND A.flg_mnt_status = 'A'
                 AND A.nam_stage NOT IN ('PMI')
                 AND a.migration_source = 'BRNET')
       WHERE rownum <= 10 --sample 10 records esaf_changes;
      ;
  END IF;
  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  var_l_consis_no := 23046;
  /*consis 23046: AMT_INSTAL and AMT_INSTAL_OUTST not same for latest stage*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM (SELECT zs.cod_acct_no,
                   zs.amt_instal,
                   zsd.amt_instal_outst,
                   zs.ctr_stage_no,
                   zsd.ctr_stage_no
              FROM z_co_ln_acct_schedule_amt_instal             zs,
                   z_co_ln_acct_schedule_detls_amt_instal_outst zsd
             WHERE zs.cod_acct_no = zsd.cod_acct_no
               AND zs.amt_instal != zsd.amt_instal_outst
               AND EXISTS (SELECT 1
                      FROM co_ln_acct_dtls C
                     WHERE C.cod_acct_stat != 1
                       AND C.cod_acct_no = zs.cod_acct_no
                       AND migration_source = 'CBS') --FA:10-Jun-24: Consider only active cases

            );

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM (SELECT zs.cod_acct_no,
                   zs.amt_instal,
                   zsd.amt_instal_outst,
                   zs.ctr_stage_no,
                   zsd.ctr_stage_no
              FROM z_co_ln_acct_schedule_amt_instal             zs,
                   z_co_ln_acct_schedule_detls_amt_instal_outst zsd
             WHERE zs.cod_acct_no = zsd.cod_acct_no
               AND zs.amt_instal != zsd.amt_instal_outst
               AND EXISTS (SELECT 1
                      FROM co_ln_acct_dtls C
                     WHERE C.cod_acct_stat != 1
                       AND C.cod_acct_no = zs.cod_acct_no
                       AND migration_source = 'BRNET') --FA:10-Jun-24: Consider only active cases

            );
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'AMT_INSTAL',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'AMT_INSTAL and AMT_INSTAL_OUTST not same for latest stage');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'AMT_INSTAL',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'AMT_INSTAL and AMT_INSTAL_OUTST not same for latest stage');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM (SELECT zs.cod_acct_no,
                     zs.amt_instal,
                     zsd.amt_instal_outst,
                     zs.ctr_stage_no,
                     zsd.ctr_stage_no,
                     zs.MIGRATION_SOURCE
                FROM z_co_ln_acct_schedule_amt_instal             zs,
                     z_co_ln_acct_schedule_detls_amt_instal_outst zsd
               WHERE zs.cod_acct_no = zsd.cod_acct_no
                 AND zs.amt_instal != zsd.amt_instal_outst
                 AND EXISTS (SELECT 1
                        FROM co_ln_acct_dtls C
                       WHERE C.cod_acct_stat != 1
                         AND C.cod_acct_no = zs.cod_acct_no
                         AND migration_source = 'CBS') --FA:10-Jun-24: Consider only active cases
              )
       WHERE rownum <= 10 --sample 10 records esaf_changes;
      ;
  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM (SELECT zs.cod_acct_no,
                     zs.amt_instal,
                     zsd.amt_instal_outst,
                     zs.ctr_stage_no,
                     zsd.ctr_stage_no,
                     zs.migration_source
                FROM z_co_ln_acct_schedule_amt_instal             zs,
                     z_co_ln_acct_schedule_detls_amt_instal_outst zsd
               WHERE zs.cod_acct_no = zsd.cod_acct_no
                 AND zs.amt_instal != zsd.amt_instal_outst
                 AND EXISTS (SELECT 1
                        FROM co_ln_acct_dtls C
                       WHERE C.cod_acct_stat != 1
                         AND C.cod_acct_no = zs.cod_acct_no
                         AND migration_source = 'BRNET') --FA:10-Jun-24: Consider only active cases
              )
       WHERE rownum <= 10 --sample 10 records esaf_changes;
      ;
  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  var_l_consis_no := 23047;
  /*consis 23047: CTR_STAGE_NO mismatch*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM (SELECT zs.cod_acct_no,
                   zs.amt_instal,
                   zsd.amt_instal_outst,
                   zs.ctr_stage_no,
                   zsd.ctr_stage_no
              FROM z_co_ln_acct_schedule_amt_instal             zs,
                   z_co_ln_acct_schedule_detls_amt_instal_outst zsd
             WHERE zs.cod_acct_no = zsd.cod_acct_no
               AND zs.ctr_stage_no != zsd.ctr_stage_no
               AND EXISTS (SELECT 1
                      FROM co_ln_acct_dtls C
                     WHERE C.cod_acct_stat != 1
                       AND C.cod_acct_no = zs.cod_acct_no
                       AND migration_source = 'CBS') --FA:10-Jun-24: Consider only active cases

            );

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM (SELECT zs.cod_acct_no,
                   zs.amt_instal,
                   zsd.amt_instal_outst,
                   zs.ctr_stage_no,
                   zsd.ctr_stage_no
              FROM z_co_ln_acct_schedule_amt_instal             zs,
                   z_co_ln_acct_schedule_detls_amt_instal_outst zsd
             WHERE zs.cod_acct_no = zsd.cod_acct_no
               AND zs.ctr_stage_no != zsd.ctr_stage_no
               AND EXISTS (SELECT 1
                      FROM co_ln_acct_dtls C
                     WHERE C.cod_acct_stat != 1
                       AND C.cod_acct_no = zs.cod_acct_no
                       AND migration_source = 'BRNET') --FA:10-Jun-24: Consider only active cases

            );
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'CTR_STAGE_NO',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'CTR_STAGE_NO mismatch');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'BRNET',
       var_l_table_name,
       'CTR_STAGE_NO',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'CTR_STAGE_NO mismatch');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM (SELECT zs.cod_acct_no,
                     zs.amt_instal,
                     zsd.amt_instal_outst,
                     zs.ctr_stage_no,
                     zsd.ctr_stage_no,
                     zs.MIGRATION_SOURCE
                FROM z_co_ln_acct_schedule_amt_instal             zs,
                     z_co_ln_acct_schedule_detls_amt_instal_outst zsd
               WHERE zs.cod_acct_no = zsd.cod_acct_no
                 AND zs.ctr_stage_no != zsd.ctr_stage_no
                 AND EXISTS (SELECT 1
                        FROM co_ln_acct_dtls C
                       WHERE C.cod_acct_stat != 1
                         AND C.cod_acct_no = zs.cod_acct_no
                         AND migration_source = 'CBS') --FA:10-Jun-24: Consider only active cases
              )
       WHERE ROWNUM <= 10 --sample 10 records esaf_changes
      ;
  END IF;
  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM (SELECT zs.cod_acct_no,
                     zs.amt_instal,
                     zsd.amt_instal_outst,
                     zs.ctr_stage_no,
                     zsd.ctr_stage_no,
                     zs.MIGRATION_SOURCE
                FROM z_co_ln_acct_schedule_amt_instal             zs,
                     z_co_ln_acct_schedule_detls_amt_instal_outst zsd
               WHERE zs.cod_acct_no = zsd.cod_acct_no
                 AND zs.ctr_stage_no != zsd.ctr_stage_no
                 AND EXISTS (SELECT 1
                        FROM co_ln_acct_dtls C
                       WHERE C.cod_acct_stat != 1
                         AND C.cod_acct_no = zs.cod_acct_no
                         AND migration_source = 'BRNET') --FA:10-Jun-24: Consider only active cases
              )
       WHERE ROWNUM <= 10 --sample 10 records esaf_changes
      ;
  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  var_l_consis_no := 23048;
  /*consis 23048: Mismatch in DAT_STAGE_START or DAT_STAGE_END between schedule detls and schedule summary*/

  EXECUTE IMMEDIATE 'TRUNCATE TABLE z_for_check_sched_consis_23048 DROP ALL STORAGE';

  BEGIN
    INSERT /*+enable_parallel_dml append nologging parallel (4)*/
    INTO z_for_check_sched_consis_23048
      SELECT /*+ PARALLEL(4)*/
       *
        FROM (SELECT sch.cod_acct_no,
                     sch.ctr_stage_no,
                     sch.nam_stage,
                     sch.dat_stage_start,
                     sch.dat_stage_end,
                     sds.*,
                     nvl(LAG(sds.detls_max_date_instal)
                         OVER(PARTITION BY sds.detls_cod_acct_no ORDER BY
                              sds.detls_cod_acct_no,
                              sds.detls_ctr_stage_no),
                         sds.detls_min_dat_start) dat_stage_start_derived,
                     sch.migration_source

                FROM co_ln_acct_schedule sch,
                     (SELECT cod_acct_no detls_cod_acct_no,
                             ctr_stage_no detls_ctr_stage_no,
                             MIN(dat_stage_start) detls_dat_stage_start,
                             MIN(dat_start) detls_min_dat_start,
                             MAX(date_instal) detls_max_date_instal
                        FROM co_ln_acct_schedule_detls
                       GROUP BY cod_acct_no, ctr_stage_no) sds
               WHERE sch.cod_acct_no = sds.detls_cod_acct_no
                 AND sch.ctr_stage_no = sds.detls_ctr_stage_no
                 AND EXISTS
               (SELECT 1
                        FROM co_ln_acct_dtls C
                       WHERE C.cod_acct_stat != 1
                         AND C.cod_acct_no = sch.cod_acct_no) --FA:10-Jun-24: Consider only active cases
              )
       WHERE (dat_stage_start != dat_stage_start_derived OR
             dat_stage_end != detls_max_date_instal OR
             dat_stage_start != detls_min_dat_start) --188068 109754
      ;
    COMMIT;
  END;

  BEGIN
    SELECT /*+ PARALLEL(4)*/
     COUNT(1), COUNT(DISTINCT cod_acct_no)
      INTO var_l_count, var_l_dist_count
      FROM z_for_check_sched_consis_23048
     WHERE migration_source = 'CBS';

    SELECT /*+ PARALLEL(4)*/
     COUNT(1), COUNT(DISTINCT cod_acct_no)
      INTO var_l_count1, var_l_dist_count
      FROM z_for_check_sched_consis_23048;
    --WHERE migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'DAT_STAGE_END',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       var_l_dist_count ||
       'DISTINCT Accounts have : Mismatch in DAT_STAGE_START or DAT_STAGE_END between schedule detls and schedule summary');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'DAT_STAGE_END',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       var_l_dist_count ||
       'DISTINCT Accounts have : Mismatch in DAT_STAGE_START or DAT_STAGE_END between schedule detls and schedule summary');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
      DISTINCT var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM z_for_check_sched_consis_23048
       WHERE rownum <= 50 --sample 50 records esaf_changes;
         AND migration_source = 'CBS';

  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
      DISTINCT var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM z_for_check_sched_consis_23048
       WHERE rownum <= 50 --sample 50 records esaf_changes;
         AND migration_source = 'BRNET';
  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  var_l_consis_no := 23049;
  /*consis 23049: Count mismatch in schedule summary and schedule details for PRE-EMI / EMI*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM (SELECT A.cod_acct_no,
                   A.ctr_stage_no,
                   decode(ctr_int, 0, ctr_instal, ctr_int) ctr_sched_cnt,
                   B.ctr_stage_no detls_ctr_stage_no,
                   B.ctr_dtls_cnt detls_ctr_dtls_cnt
              FROM co_ln_acct_schedule A,
                   (SELECT cod_acct_no, ctr_stage_no, COUNT(1) ctr_dtls_cnt
                      FROM co_ln_acct_schedule_detls
                     GROUP BY cod_acct_no, ctr_stage_no) B
             WHERE A.cod_acct_no = B.cod_acct_no
               AND A.ctr_stage_no = B.ctr_stage_no
               AND A.dat_stage_start < var_l_dat_process
               AND A.dat_stage_end >= var_l_dat_process
               AND decode(ctr_int, 0, ctr_instal, ctr_int) <> B.ctr_dtls_cnt
               AND EXISTS (SELECT 1
                      FROM co_ln_acct_dtls C
                     WHERE C.cod_acct_stat != 1
                       AND C.cod_acct_no = A.cod_acct_no) --FA:10-Jun-24: Consider only active cases
               AND migration_source = 'CBS');

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM (SELECT A.cod_acct_no,
                   A.ctr_stage_no,
                   decode(ctr_int, 0, ctr_instal, ctr_int) ctr_sched_cnt,
                   B.ctr_stage_no detls_ctr_stage_no,
                   B.ctr_dtls_cnt detls_ctr_dtls_cnt
              FROM co_ln_acct_schedule A,
                   (SELECT cod_acct_no, ctr_stage_no, COUNT(1) ctr_dtls_cnt
                      FROM co_ln_acct_schedule_detls
                     GROUP BY cod_acct_no, ctr_stage_no) B
             WHERE A.cod_acct_no = B.cod_acct_no
               AND A.ctr_stage_no = B.ctr_stage_no
               AND A.dat_stage_start < var_l_dat_process
               AND A.dat_stage_end >= var_l_dat_process
               AND decode(ctr_int, 0, ctr_instal, ctr_int) <> B.ctr_dtls_cnt
               AND EXISTS (SELECT 1
                      FROM co_ln_acct_dtls C
                     WHERE C.cod_acct_stat != 1
                       AND C.cod_acct_no = A.cod_acct_no) --FA:10-Jun-24: Consider only active cases
               AND migration_source = 'BRNET');
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'CTR_INT',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Count mismatch in schedule summary and schedule details for PRE-EMI / EMI');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'CTR_INT',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Count mismatch in schedule summary and schedule details for PRE-EMI / EMI');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM (SELECT A.cod_acct_no,
                     A.ctr_stage_no,
                     decode(ctr_int, 0, ctr_instal, ctr_int) ctr_sched_cnt,
                     B.ctr_stage_no detls_ctr_stage_no,
                     B.ctr_dtls_cnt detls_ctr_dtls_cnt,
                     a.MIGRATION_SOURCE
                FROM co_ln_acct_schedule A,
                     (SELECT cod_acct_no, ctr_stage_no, COUNT(1) ctr_dtls_cnt
                        FROM co_ln_acct_schedule_detls
                       GROUP BY cod_acct_no, ctr_stage_no) B
               WHERE A.cod_acct_no = B.cod_acct_no
                 AND A.ctr_stage_no = B.ctr_stage_no
                 AND A.dat_stage_start < var_l_dat_process
                 AND A.dat_stage_end >= var_l_dat_process
                 AND decode(ctr_int, 0, ctr_instal, ctr_int) <>
                     B.ctr_dtls_cnt
                 AND EXISTS (SELECT 1
                        FROM co_ln_acct_dtls C
                       WHERE C.cod_acct_stat != 1
                         AND C.cod_acct_no = A.cod_acct_no) --FA:10-Jun-24: Consider only active cases
                 AND migration_source = 'CBS')
       where rownum <= 10 --sample 10 record esaf_changes
      ;
  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM (SELECT A.cod_acct_no,
                     A.ctr_stage_no,
                     decode(ctr_int, 0, ctr_instal, ctr_int) ctr_sched_cnt,
                     B.ctr_stage_no detls_ctr_stage_no,
                     B.ctr_dtls_cnt detls_ctr_dtls_cnt,
                     a.MIGRATION_SOURCE
                FROM co_ln_acct_schedule A,
                     (SELECT cod_acct_no, ctr_stage_no, COUNT(1) ctr_dtls_cnt
                        FROM co_ln_acct_schedule_detls
                       GROUP BY cod_acct_no, ctr_stage_no) B
               WHERE A.cod_acct_no = B.cod_acct_no
                 AND A.ctr_stage_no = B.ctr_stage_no
                 AND A.dat_stage_start < var_l_dat_process
                 AND A.dat_stage_end >= var_l_dat_process
                 AND decode(ctr_int, 0, ctr_instal, ctr_int) <>
                     B.ctr_dtls_cnt
                 AND EXISTS (SELECT 1
                        FROM co_ln_acct_dtls C
                       WHERE C.cod_acct_stat != 1
                         AND C.cod_acct_no = A.cod_acct_no
                         AND migration_source = 'BRNET') --FA:10-Jun-24: Consider only active cases
              )
       where rownum <= 10 --sample 10 record esaf_changes
      ;

  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  var_l_consis_no := 23050;
  /*consis 23050:  first disbursement date dat_first_disb not within schedule range (func: 1325)*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM (SELECT A.cod_acct_no,
                   A.dat_first_disb,
                   B.min_sched_start,
                   B.max_sched_end
              FROM co_ln_acct_dtls A,
                   (SELECT cod_acct_no,
                           MIN(dat_stage_start) min_sched_start,
                           MAX(dat_stage_end) max_sched_end
                      FROM co_ln_acct_schedule
                     GROUP BY cod_acct_no) B
             WHERE A.cod_acct_no = B.cod_acct_no(+)
               AND A.dat_first_disb NOT BETWEEN
                   nvl(min_sched_start, '01-JAN-1800') AND
                   nvl(max_sched_end, '01-JAN-1800')
               AND A.dat_first_disb != '1-Jan-1800'
                  --        AND A.cod_acct_no NOT IN (SELECT cod_acct_no FROM conv_ln_x_acct_exclude WHERE consis_no = 24015)
               AND migration_source = 'CBS');

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM (SELECT A.cod_acct_no,
                   A.dat_first_disb,
                   B.min_sched_start,
                   B.max_sched_end
              FROM co_ln_acct_dtls A,
                   (SELECT cod_acct_no,
                           MIN(dat_stage_start) min_sched_start,
                           MAX(dat_stage_end) max_sched_end
                      FROM co_ln_acct_schedule
                     GROUP BY cod_acct_no) B
             WHERE A.cod_acct_no = B.cod_acct_no(+)
               AND A.dat_first_disb NOT BETWEEN
                   nvl(min_sched_start, '01-JAN-1800') AND
                   nvl(max_sched_end, '01-JAN-1800')
               AND A.dat_first_disb != '1-Jan-1800'
                  --        AND A.cod_acct_no NOT IN (SELECT cod_acct_no FROM conv_ln_x_acct_exclude WHERE consis_no = 24015)
               AND migration_source = 'BRNET');
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'DAT_STAGE',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'first disbursement date dat_first_disb not within schedule range (func: 1325)');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'DAT_STAGE',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'first disbursement date dat_first_disb not within schedule range (func: 1325)');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM (SELECT A.cod_acct_no,
                     A.dat_first_disb,
                     B.min_sched_start,
                     B.max_sched_end,
                     a.MIGRATION_SOURCE
                FROM co_ln_acct_dtls A,
                     (SELECT cod_acct_no,
                             MIN(dat_stage_start) min_sched_start,
                             MAX(dat_stage_end) max_sched_end
                        FROM co_ln_acct_schedule
                       GROUP BY cod_acct_no) B
               WHERE A.cod_acct_no = B.cod_acct_no(+)
                 AND A.dat_first_disb NOT BETWEEN
                     nvl(min_sched_start, '01-JAN-1800') AND
                     nvl(max_sched_end, '01-JAN-1800')
                 AND A.dat_first_disb != '1-Jan-1800'
                 AND migration_source = 'CBS'

              --            AND A.cod_acct_no NOT IN (SELECT cod_acct_no FROM conv_ln_x_acct_exclude WHERE consis_no = 24015)
              )
       WHERE rownum <= 10 --sample 10 records esaf_changes;
      ;
  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM (SELECT A.cod_acct_no,
                     A.dat_first_disb,
                     B.min_sched_start,
                     B.max_sched_end,
                     a.MIGRATION_SOURCE
                FROM co_ln_acct_dtls A,
                     (SELECT cod_acct_no,
                             MIN(dat_stage_start) min_sched_start,
                             MAX(dat_stage_end) max_sched_end
                        FROM co_ln_acct_schedule
                       GROUP BY cod_acct_no) B
               WHERE A.cod_acct_no = B.cod_acct_no(+)
                 AND A.dat_first_disb NOT BETWEEN
                     nvl(min_sched_start, '01-JAN-1800') AND
                     nvl(max_sched_end, '01-JAN-1800')
                 AND A.dat_first_disb != '1-Jan-1800'
                 AND migration_source = 'BRNET'

              --            AND A.cod_acct_no NOT IN (SELECT cod_acct_no FROM conv_ln_x_acct_exclude WHERE consis_no = 24015)
              )
       WHERE rownum <= 10 --sample 10 records esaf_changes;
      ;
  END IF;
  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  var_l_consis_no := 23051;
  /*consis 23051:  Installment missing for upcoming month*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls a
     WHERE dat_first_disb <= var_mig_date
       AND ctr_disb > 0
       AND cod_Acct_Stat NOT IN (1, 11)
       AND NOT EXISTS
     (SELECT 1
              FROM co_ln_acct_schedule_detls b
             WHERE a.cod_Acct_no = b.cod_Acct_no
                  --    AND date_instal >= '1-jul-2024' AND date_instal <= '31-jul-2024'
                  --    AND to_char(date_instal, 'MMYY') =  to_char(cbsfchost.dateadd(cbsfchost.mm, 1, :var_mig_date), 'MMYY')
               AND date_instal > var_mig_date)
       AND dat_of_maturity > var_mig_date
       AND migration_source = 'CBS';

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls a
     WHERE dat_first_disb <= var_mig_date
       AND ctr_disb > 0
       AND cod_Acct_Stat NOT IN (1, 11)
       AND NOT EXISTS
     (SELECT 1
              FROM co_ln_acct_schedule_detls b
             WHERE a.cod_Acct_no = b.cod_Acct_no
                  --    AND date_instal >= '1-jul-2024' AND date_instal <= '31-jul-2024'
                  --    AND to_char(date_instal, 'MMYY') =  to_char(cbsfchost.dateadd(cbsfchost.mm, 1, :var_mig_date), 'MMYY')
               AND date_instal > var_mig_date)
       AND dat_of_maturity > var_mig_date
       AND migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'DAT_STAGE',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Installment missing for upcoming month');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'DAT_STAGE',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Installment missing for upcoming month');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_dtls a
       WHERE dat_first_disb <= var_mig_date
         AND ctr_disb > 0
         AND cod_Acct_Stat NOT IN (1, 11)
         AND NOT EXISTS
       (SELECT 1
                FROM co_ln_acct_schedule_detls b
               WHERE a.cod_Acct_no = b.cod_Acct_no
                    --    AND date_instal >= '1-jul-2024' AND date_instal <= '31-jul-2024'
                    --    AND to_char(date_instal, 'MMYY') =  to_char(cbsfchost.dateadd(cbsfchost.mm, 1, :var_mig_date), 'MMYY')
                 AND date_instal > var_mig_date)
         AND dat_of_maturity > var_mig_date
         and rownum <= 10
         AND migration_source = 'CBS'; --sample 10 records esaf_changes;
  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_dtls a
       WHERE dat_first_disb <= var_mig_date
         AND ctr_disb > 0
         AND cod_Acct_Stat NOT IN (1, 11)
         AND NOT EXISTS
       (SELECT 1
                FROM co_ln_acct_schedule_detls b
               WHERE a.cod_Acct_no = b.cod_Acct_no
                    --    AND date_instal >= '1-jul-2024' AND date_instal <= '31-jul-2024'
                    --    AND to_char(date_instal, 'MMYY') =  to_char(cbsfchost.dateadd(cbsfchost.mm, 1, :var_mig_date), 'MMYY')
                 AND date_instal > var_mig_date)
         AND dat_of_maturity > var_mig_date
         and rownum <= 10
         AND migration_source = 'BRNET'; --sample 10 records esaf_changes;
  END IF;
  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

---------------------------------------------------

var_l_consis_no := 23054;
  /*consis 23051:  Installment missing for upcoming month*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_schedule
        WHERE 
--        nam_stage = 'EPI'
        nam_stage IN ( 'EPI', 'EMI', 'IPI' )
        AND amt_instal = 0
        AND cod_acct_no IN (SELECT cod_acct_no FROM co_ln_acct_balances WHERE (amt_princ_balance - amt_rpa_balance) > 0) --PP case, as PP not recived in arrears

       AND migration_source = 'CBS';

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_schedule
        WHERE 
--        nam_stage = 'EPI'
        nam_stage IN ( 'EPI', 'EMI', 'IPI' )
        AND amt_instal = 0
        AND cod_acct_no IN (SELECT cod_acct_no FROM co_ln_acct_balances WHERE (amt_princ_balance - amt_rpa_balance) > 0) --PP case, as PP not recived in arrears

       AND migration_source = 'BRNET';
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'AMT_INSTAL',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Accounts with EMI stage and amt_instal = 0');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'AMT_INSTAL',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Accounts with EMI stage and amt_instal = 0');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
        WHERE 
--        nam_stage = 'EPI'
        nam_stage IN ( 'EPI', 'EMI', 'IPI' )
        AND amt_instal = 0
        AND cod_acct_no IN (SELECT cod_acct_no FROM co_ln_acct_balances WHERE (amt_princ_balance - amt_rpa_balance) > 0) --PP case, as PP not recived in arrears

         and rownum <= 10
         AND migration_source = 'CBS'; --sample 10 records esaf_changes;
  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
        FROM co_ln_acct_schedule
        WHERE 
--        nam_stage = 'EPI'
        nam_stage IN ( 'EPI', 'EMI', 'IPI' )
        AND amt_instal = 0
        AND cod_acct_no IN (SELECT cod_acct_no FROM co_ln_acct_balances WHERE (amt_princ_balance - amt_rpa_balance) > 0) --PP case, as PP not recived in arrears

         and rownum <= 10
         AND migration_source = 'BRNET'; --sample 10 records esaf_changes;
  END IF;
  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis


----------------------------------------------------------

var_l_consis_no := 23055;
  /*consis 23051:  Installment missing for upcoming month*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
       FROM (
      SELECT cod_acct_no, COUNT(1)
      , LISTAGG(DISTINCT nam_stage, ', ') WITHIN GROUP (ORDER BY ctr_stage_no) nam_stage
      FROM co_ln_acct_schedule where  migration_source = 'CBS'
      GROUP BY cod_acct_no
    )
    WHERE nam_stage NOT LIKE '%EPI%'
    AND nam_stage NOT LIKE '%IPI%'

       ;

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
       FROM (
      SELECT cod_acct_no, COUNT(1)
      , LISTAGG(DISTINCT nam_stage, ', ') WITHIN GROUP (ORDER BY ctr_stage_no) nam_stage
      FROM co_ln_acct_schedule where  migration_source = 'BRNET'
      GROUP BY cod_acct_no
    )
    WHERE nam_stage NOT LIKE '%EPI%'
    AND nam_stage NOT LIKE '%IPI%'
;
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'NAM_STAGE',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Accounts without EPI stage');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'NAM_STAGE',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Accounts without EPI stage');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, 'CBS'
         FROM (
      SELECT cod_acct_no, COUNT(1)
      , LISTAGG(DISTINCT nam_stage, ', ') WITHIN GROUP (ORDER BY ctr_stage_no) nam_stage
      FROM co_ln_acct_schedule where  migration_source = 'CBS'
      GROUP BY cod_acct_no
    )
    WHERE nam_stage NOT LIKE '%EPI%'
    AND nam_stage NOT LIKE '%IPI%'

         and rownum <= 10
         ; --sample 10 records esaf_changes;
  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, 'BRNET'
         FROM (
      SELECT cod_acct_no, COUNT(1)
      , LISTAGG(DISTINCT nam_stage, ', ') WITHIN GROUP (ORDER BY ctr_stage_no) nam_stage
      FROM co_ln_acct_schedule where  migration_source = 'BRNET'
      GROUP BY cod_acct_no
    )
    WHERE nam_stage NOT LIKE '%EPI%'
    AND nam_stage NOT LIKE '%IPI%'

         and rownum <= 10; --sample 10 records esaf_changes;
  END IF;
  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
                       
                       
------------------------------------------------------------------

var_l_consis_no := 23056;
  /*consis 23051:  Installment missing for upcoming month*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
       FROM (
      SELECT cod_acct_no, COUNT(1)
      , LISTAGG(DISTINCT nam_stage, ', ') WITHIN GROUP (ORDER BY ctr_stage_no) nam_stage
      FROM co_ln_acct_schedule where  migration_source = 'CBS'
      GROUP BY cod_acct_no
    )
    WHERE nam_stage NOT LIKE '%PMI%'

       ;

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
       FROM (
      SELECT cod_acct_no, COUNT(1)
      , LISTAGG(DISTINCT nam_stage, ', ') WITHIN GROUP (ORDER BY ctr_stage_no) nam_stage
      FROM co_ln_acct_schedule where  migration_source = 'BRNET'
      GROUP BY cod_acct_no
    )
    WHERE nam_stage NOT LIKE '%PMI%'
;
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'NAM_STAGE',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Accounts without PMI stage');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'NAM_STAGE',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Accounts without PMI stage');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, 'CBS'
         FROM (
      SELECT cod_acct_no, COUNT(1)
      , LISTAGG(DISTINCT nam_stage, ', ') WITHIN GROUP (ORDER BY ctr_stage_no) nam_stage
      FROM co_ln_acct_schedule where  migration_source = 'CBS'
      GROUP BY cod_acct_no
    )
    WHERE nam_stage NOT LIKE '%PMI%'

         and rownum <= 10
         ; --sample 10 records esaf_changes;
  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, 'BRNET'
         FROM (
      SELECT cod_acct_no, COUNT(1)
      , LISTAGG(DISTINCT nam_stage, ', ') WITHIN GROUP (ORDER BY ctr_stage_no) nam_stage
      FROM co_ln_acct_schedule where  migration_source = 'BRNET'
      GROUP BY cod_acct_no
    )
    WHERE nam_stage NOT LIKE '%PMI%'

         and rownum <= 10; --sample 10 records esaf_changes;
  END IF;
  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
                       
--------------------------------------------


var_l_consis_no := 23060;
  /*consis 23051:  Installment missing for upcoming month*/
  BEGIN
    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count
      FROM (SELECT A.cod_acct_no,
                       LISTAGG(DISTINCT nam_stage, ', ') WITHIN GROUP(ORDER BY ctr_stage_no) stage_list
                       , D.cod_prod
                  FROM co_ln_acct_schedule A, co_ln_acct_dtls D
                 WHERE EXISTS (SELECT 1
                          FROM 
--                            CO_LN_ACCT_DTLS     B,
                               co_ln_acct_balances C
                         WHERE C.cod_acct_no = D.cod_acct_no
                         AND A.cod_acct_no = C.cod_acct_no
                           AND D.amt_face_value > C.amt_disbursed
                           AND D.ctr_disb > 0 --FA: 06-May-2024: Commented as Full disbursement can happen before next due date
                           )
                   AND A.cod_acct_no = D.cod_acct_no
                   AND D.cod_acct_stat <> 1
                   AND D.migration_source ='CBS'
                 GROUP BY A.cod_acct_no, D.cod_prod)
         WHERE stage_list NOT LIKE '%IOI%'


       ;

    SELECT /*+ parallel(4) */
     COUNT(1)
      INTO var_l_count1
      FROM (SELECT A.cod_acct_no,
                       LISTAGG(DISTINCT nam_stage, ', ') WITHIN GROUP(ORDER BY ctr_stage_no) stage_list
                       , D.cod_prod
                  FROM co_ln_acct_schedule A, co_ln_acct_dtls D
                 WHERE EXISTS (SELECT 1
                          FROM 
--                            CO_LN_ACCT_DTLS     B,
                               co_ln_acct_balances C
                         WHERE C.cod_acct_no = D.cod_acct_no
                         AND A.cod_acct_no = C.cod_acct_no
                           AND D.amt_face_value > C.amt_disbursed
                           AND D.ctr_disb > 0 --FA: 06-May-2024: Commented as Full disbursement can happen before next due date
                           )
                   AND A.cod_acct_no = D.cod_acct_no
                   AND D.cod_acct_stat <> 1
                   AND D.migration_source ='BRNET'
                 GROUP BY A.cod_acct_no, D.cod_prod)
         WHERE stage_list NOT LIKE '%IOI%'

;
  END;

  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'NAM_STAGE',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count,
       'Accounts without PMI stage');
  END;
  BEGIN
    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (var_l_consis_no,
       'MFI',
       var_l_table_name,
       'NAM_STAGE',
       'ap_cons_LN_ACCT_SCHEDULE',
       var_l_count1,
       'Accounts without PMI stage');
  END;

  IF (var_l_count > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, 'CBS'
        FROM (SELECT A.cod_acct_no,
                       LISTAGG(DISTINCT nam_stage, ', ') WITHIN GROUP(ORDER BY ctr_stage_no) stage_list
                       , D.cod_prod
                  FROM co_ln_acct_schedule A, co_ln_acct_dtls D
                 WHERE EXISTS (SELECT 1
                          FROM 
--                            CO_LN_ACCT_DTLS     B,
                               co_ln_acct_balances C
                         WHERE C.cod_acct_no = D.cod_acct_no
                         AND A.cod_acct_no = C.cod_acct_no
                           AND D.amt_face_value > C.amt_disbursed
                           AND D.ctr_disb > 0 --FA: 06-May-2024: Commented as Full disbursement can happen before next due date
                           )
                   AND A.cod_acct_no = D.cod_acct_no
                   AND D.cod_acct_stat <> 1
                   AND D.migration_source ='CBS'
                 GROUP BY A.cod_acct_no, D.cod_prod)
         WHERE stage_list NOT LIKE '%IOI%'


         and rownum <= 10
         ; --sample 10 records esaf_changes;
  END IF;

  IF (var_l_count1 > 0) THEN
    INSERT INTO co_ln_consis_acct
      (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
      SELECT /*+ parallel(4) */
       var_l_consis_no, cod_acct_no, 'BRNET'
        FROM (SELECT A.cod_acct_no,
                       LISTAGG(DISTINCT nam_stage, ', ') WITHIN GROUP(ORDER BY ctr_stage_no) stage_list
                       , D.cod_prod
                  FROM co_ln_acct_schedule A, co_ln_acct_dtls D
                 WHERE EXISTS (SELECT 1
                          FROM 
--                            CO_LN_ACCT_DTLS     B,
                               co_ln_acct_balances C
                         WHERE C.cod_acct_no = D.cod_acct_no
                         AND A.cod_acct_no = C.cod_acct_no
                           AND D.amt_face_value > C.amt_disbursed
                           AND D.ctr_disb > 0 --FA: 06-May-2024: Commented as Full disbursement can happen before next due date
                           )
                   AND A.cod_acct_no = D.cod_acct_no
                   AND D.cod_acct_stat <> 1
                   AND D.migration_source ='BRNET'
                 GROUP BY A.cod_acct_no, D.cod_prod)
         WHERE stage_list NOT LIKE '%IOI%'


         and rownum <= 10; --sample 10 records esaf_changes;
  END IF;
  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
                            
  RETURN 0;
END AP_CONS_LN_ACCT_SCHEDULE;
/
