CREATE OR REPLACE FUNCTION "AP_CONS_LN_ACCT_DTLS" (var_pi_stream IN NUMBER)
  RETURN NUMBER AS
  var_l_count             NUMBER;
  var_l_count1            NUMBER;
  var_bank_mast_dt_to_use VARCHAR2(1) := 'Y'; /*;nvl(ap_get_data_mig_param('BANK_MAST_DT_TO_USE'),
                                             'Y'); */
  var_dat_process         DATE := cbsfchost.pk_ba_global.dat_process;
  var_l_consis_no         NUMBER := 0;
  var_mig_date            date := cbsfchost.pk_ba_global.dat_last_process; /*nvl(ap_get_data_mig_param('MIG_DATE'),
                                      cbsfchost.pk_ba_global.dat_last_process); */
  var_l_function_name     VARCHAR2(100) := 'AP_CONS_LN_ACCT_DTLS';
  var_l_table_name        VARCHAR2(100) := 'CO_LN_ACCT_DTLS';
BEGIN
  ap_bb_mig_log_string('Started #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream);
  var_bank_mast_dt_to_use := 'Y'; /*nvl(ap_get_data_mig_param('BANK_MAST_DT_TO_USE'),
                                 'Y');*/
  BEGIN
    SELECT dat_process
      INTO var_dat_process
      FROM cbsfchost.ba_bank_mast
     WHERE flg_mnt_status = 'A';

  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select From co_civ_dates Failed.');
      cbsfchost.ora_raiserror(sqlcode,
                              'Select From cbsfchost.ba_bank_mast Failed.',
                              94);
  END;

  /*
  IF (var_bank_mast_dt_to_use = 'N') THEN
    --var_dat_last_process := nvl(ap_get_data_mig_param('DAT_LAST_PROCESS'), '30-Apr-2024');
    /*var_dat_process := nvl(ap_get_data_mig_param('DAT_PROCESS'),
    '01-May-2024');*/
  --  var_dat_process := NVL(TO_DATE(ap_get_data_mig_param('DAT_PROCESS'),
  --                                 'DD-Mon-YYYY'),
  --                         TO_DATE(var_mig_date, 'DD-Mon-YYYY'));
  --END IF;

  ap_bb_mig_log_string('00000 #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --Beginning of function
  DELETE FROM co_ln_consis
   WHERE cod_consis_no >= 12001
     AND cod_consis_no <= 12999;

  DELETE FROM co_ln_consis_acct
   WHERE cod_consis_no >= 12001
     AND cod_consis_no <= 12999;

  COMMIT;
  var_l_consis_no := 12001;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  /* AMT_FACE_VALUE cannot be zero or less than zero : consis 12001
  */
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND amt_face_value <= 0
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND amt_face_value <= 0
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              49);
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
      (12001,
       'LN',
       var_l_table_name,
       'AMT_FACE_VALUE',
       var_l_function_name,
       var_l_count,
       'AMT_FACE_VALUE cannot be zero or less than zero');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12001,
       'MFI',
       var_l_table_name,
       'AMT_FACE_VALUE',
       var_l_function_name,
       var_l_count1,
       'AMT_FACE_VALUE cannot be zero or less than zero');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              72);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12001, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND amt_face_value <= 0
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                89);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12001, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND amt_face_value <= 0
           AND migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                89);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12002;
  /*AMT_UNCLR cannot be other than zero : consis 12002
  */
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND amt_unclr <> 0
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND amt_unclr <> 0
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              105);
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
      (12002,
       'LN',
       var_l_table_name,
       'AMT_UNCLR',
       var_l_function_name,
       var_l_count,
       'AMT_UNCLR cannot be other than zero');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12002,
       'MFI',
       var_l_table_name,
       'AMT_UNCLR',
       var_l_function_name,
       var_l_count1,
       'AMT_UNCLR cannot be other than zero');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              127);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12002, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND amt_unclr <> 0
           AND a.migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                145);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12002, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND amt_unclr <> 0
           AND a.migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                145);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12003;
  /*COD_ACCT_STAT must be 8 for all account : consis 12003 */
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_acct_stat NOT IN (8, 10, 11, 1) -- 5 was there at capri
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_acct_stat NOT IN (8, 10, 11, 1) -- 5 was there at capri
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              161);
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
      (12003,
       'LN',
       var_l_table_name,
       'COD_ACCT_STAT',
       var_l_function_name,
       var_l_count,
       'COD_ACCT_STAT must be 8,10 or 11 for all account');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12003,
       'MFI',
       var_l_table_name,
       'COD_ACCT_STAT',
       var_l_function_name,
       var_l_count1,
       'COD_ACCT_STAT must be 8,10 or 11 for all account');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              183);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12003, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND cod_acct_stat NOT IN (8, 10, 11, 1)
           AND a.migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                201);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12003, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND cod_acct_stat NOT IN (8, 10, 11, 1)
           AND a.migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                201);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12004;
  /* COD_CC_BRN  MISMATCH GOLD COPY AND DATA : consis 12004*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_cc_brn NOT IN
           (SELECT cod_cc_brn
              FROM cbsfchost.ba_cc_brn_mast
             WHERE flg_mnt_status = 'A')
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_cc_brn NOT IN
           (SELECT cod_cc_brn
              FROM cbsfchost.ba_cc_brn_mast
             WHERE flg_mnt_status = 'A')
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              218);
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
      (12004,
       'LN',
       var_l_table_name,
       'COD_CC_BRN',
       var_l_function_name,
       var_l_count,
       'COD_CC_BRN  MISMATCH GOLD COPY AND DATA');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12004,
       'MFI',
       var_l_table_name,
       'COD_CC_BRN',
       var_l_function_name,
       var_l_count1,
       'COD_CC_BRN  MISMATCH GOLD COPY AND DATA');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              240);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12004, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND cod_cc_brn NOT IN
               (SELECT cod_cc_brn
                  FROM cbsfchost.ba_cc_brn_mast
                 WHERE flg_mnt_status = 'A')
           AND a.migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                260);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12004, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND cod_cc_brn NOT IN
               (SELECT cod_cc_brn
                  FROM cbsfchost.ba_cc_brn_mast
                 WHERE flg_mnt_status = 'A')
           AND a.migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                260);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12005;
  /*COD_CCY  must be equivalent INR equivalent CCY : consis 12005 */
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_ccy NOT IN (SELECT cod_ccy
                             FROM cbsfchost.ba_ccy_code
                            WHERE nam_ccy_short = 'INR'
                              AND flg_mnt_status = 'A')
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_ccy NOT IN (SELECT cod_ccy
                             FROM cbsfchost.ba_ccy_code
                            WHERE nam_ccy_short = 'INR'
                              AND flg_mnt_status = 'A')
       and migration_source = 'BRNET';
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              278);
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
      (12005,
       'LN',
       var_l_table_name,
       'COD_CCY',
       var_l_function_name,
       var_l_count,
       'COD_CCY  must be equivalent INR equivalent CCY');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12005,
       'MFI',
       var_l_table_name,
       'COD_CCY',
       var_l_function_name,
       var_l_count1,
       'COD_CCY  must be equivalent INR equivalent CCY');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              300);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12005, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND cod_ccy NOT IN (SELECT cod_ccy
                                 FROM cbsfchost.ba_ccy_code
                                WHERE nam_ccy_short = 'INR'
                                  AND flg_mnt_status = 'A')
           AND a.migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                321);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12005, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND cod_ccy NOT IN (SELECT cod_ccy
                                 FROM cbsfchost.ba_ccy_code
                                WHERE nam_ccy_short = 'INR'
                                  AND flg_mnt_status = 'A')
           AND a.migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                321);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12006;
  /*COD_CUST_ID must be present ci_cust_mast : consis 12006*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_cust_id NOT IN
           (SELECT cod_cust_id
              FROM co_ci_custmast
             WHERE flg_mnt_status = 'A')
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_cust_id NOT IN
           (SELECT cod_cust_id
              FROM co_ci_custmast
             WHERE flg_mnt_status = 'A')
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              338);
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
      (12006,
       'LN',
       var_l_table_name,
       'COD_CUST_ID',
       var_l_function_name,
       var_l_count,
       'COD_CUST_ID must be present ci_cust_mast');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12006,
       'MFI',
       var_l_table_name,
       'COD_CUST_ID',
       var_l_function_name,
       var_l_count1,
       'COD_CUST_ID must be present ci_cust_mast');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              361);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12006, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND cod_cust_id NOT IN
               (SELECT cod_cust_id
                  FROM co_ci_custmast
                 WHERE flg_mnt_status = 'A')
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                382);
    END;
  END IF;

  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12006, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND cod_cust_id NOT IN
               (SELECT cod_cust_id
                  FROM co_ci_custmast
                 WHERE flg_mnt_status = 'A')
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                382);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12007;
  /*COD_LANG  must be english : consis 12007 */
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_lang <> 'ENG'
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_lang <> 'ENG'
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              397);
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
      (12007,
       'LN',
       var_l_table_name,
       'COD_LANG',
       var_l_function_name,
       var_l_count,
       'COD_LANG  must be english');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12007,
       'MFI',
       var_l_table_name,
       'COD_LANG',
       var_l_function_name,
       var_l_count1,
       'COD_LANG  must be english');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              420);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12007, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND cod_lang <> 'ENG'
           AND a.migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                438);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12007, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND cod_lang <> 'ENG'
           AND a.migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                438);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12008;
  /*consis 12008 : COD_PROD cannot be null*/

    BEGIN
        SELECT /*+ PARALLEL(4) */
            COUNT(1)
        INTO var_l_count
        FROM
            co_ln_acct_dtls
        WHERE
                flg_mnt_status = 'A'
            AND ( cod_prod IS NULL
                  OR cod_prod NOT IN (
                SELECT
                    cod_prod
                FROM
                    cbsfchost.ln_prod_mast
                WHERE
                    flg_mnt_status = 'A'
            ) );

    EXCEPTION
        WHEN OTHERS THEN
            cbsfchost.ora_raiserror(sqlcode, 'In #' || var_l_function_name || '# Select From co_ln_acct_dtls Failed.' || sqlerrm, 454);
    END;

    BEGIN
        INSERT INTO co_ln_consis (
            cod_consis_no,
            nam_module,
            nam_table,
            nam_column,
            nam_consis_func,
            consis_count,
            desc_cons
        ) VALUES (
            12008,
            'LN',
            var_l_table_name,
            'COD_PROD',
            var_l_function_name,
            var_l_count,
            'COD_PROD cannot be null OR product code not in gold copy product master.'
        );

    EXCEPTION
        WHEN OTHERS THEN
            cbsfchost.ora_raiserror(sqlcode, 'In #' || var_l_function_name || '# INSERT INTO co_ln_consis Failed.' || sqlerrm, 477);
    END;

    IF ( var_l_count > 0 ) THEN
        BEGIN
            INSERT INTO co_ln_consis_acct (
                cod_consis_no,
                cod_acct_no
            )
                SELECT /*+ PARALLEL(4) */
                    12008,
                    cod_acct_no
                FROM
                    co_ln_acct_dtls
                WHERE
                        flg_mnt_status = 'A'
                    AND ( cod_prod IS NULL
                          OR cod_prod NOT IN (
                        SELECT
                            cod_prod
                        FROM
                            cbsfchost.ln_prod_mast
                        WHERE
                            flg_mnt_status = 'A'
                    ) );

        EXCEPTION
            WHEN OTHERS THEN
                cbsfchost.ora_raiserror(sqlcode, 'In #' || var_l_function_name || '# INSERT INTO co_ln_consis_acct Failed.' || sqlerrm, 495);
        END;
    END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' || var_l_function_name || '# Stream = ' || var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12009;
  /*consis 9 : COD_REMITTER_ACCT should be null and must be synched with ln_acct_payinstrn
  This should not be as casa module is not present, conv_ln_acct_payinstrn_avs does not exists */

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls a, co_ln_acct_payinstrn b
     WHERE a.flg_mnt_status = 'A'
       AND a.cod_acct_no = b.
     cod_acct_no(+)
       and (a.COD_REMITTER_ACCT is not null AND
           a.COD_REMITTER_ACCT <> b.COD_REMITTER_ACCT)
       and a.migration_source = 'CBS';

    --        SELECT /*+ PARALLEL(4) */
    --            COUNT(1)
    --        INTO var_l_count
    --        FROM
    --            co_ln_acct_dtls a
    --        WHERE
    --                flg_mnt_status = 'A'
    --            AND a.cod_remitter_acct IS NOT NULL;
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls a, co_ln_acct_payinstrn b
     WHERE a.flg_mnt_status = 'A'
       AND a.cod_acct_no = b.
     cod_acct_no(+)
       and (a.COD_REMITTER_ACCT is not null AND
           a.COD_REMITTER_ACCT <> b.COD_REMITTER_ACCT)
       and a.migration_source = 'BRNET';
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              522);
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
      (12009,
       'LN',
       var_l_table_name,
       'COD_REMITTER_ACCT',
       var_l_function_name,
       var_l_count,
       'COD_REMITTER_ACCT should be null.');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12009,
       'MFI',
       var_l_table_name,
       'COD_REMITTER_ACCT',
       var_l_function_name,
       var_l_count1,
       'COD_REMITTER_ACCT should be null.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              546);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12009, a.cod_acct_no,a.MIGRATION_SOURCE
          FROM co_ln_acct_dtls a, co_ln_acct_payinstrn b
         WHERE a.flg_mnt_status = 'A'
           AND a.cod_acct_no = b.
         cod_acct_no(+)
           and (a.COD_REMITTER_ACCT is not null AND
               a.COD_REMITTER_ACCT <> b.COD_REMITTER_ACCT)
           AND a.migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                564);
    END;
  END IF;

  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12009, a.cod_acct_no, a.migration_source
          FROM co_ln_acct_dtls a, co_ln_acct_payinstrn b
         WHERE a.flg_mnt_status = 'A'
           AND a.cod_acct_no = b.
         cod_acct_no(+)
           and (a.COD_REMITTER_ACCT is not null AND
               a.COD_REMITTER_ACCT <> b.COD_REMITTER_ACCT)
           AND a.migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                564);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12010;
  /*consis 12010 : CTR_DISB must be gaeater 0 if DAT_FIRST_DISB is not null*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND ctr_disb <= 0
       AND dat_first_disb IS NOT NULL
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND ctr_disb <= 0
       AND dat_first_disb IS NOT NULL
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              581);
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
      (12010,
       'LN',
       var_l_table_name,
       'CTR_DISB',
       var_l_function_name,
       var_l_count,
       'CTR_DISB must be greater 0 if DAT_FIRST_DISB is not null');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12010,
       'MFI',
       var_l_table_name,
       'CTR_DISB',
       var_l_function_name,
       var_l_count1,
       'CTR_DISB must be greater 0 if DAT_FIRST_DISB is not null');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              604);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12010, cod_acct_no,MIGRATION_SOURCE
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND ctr_disb <= 0
           AND dat_first_disb IS NOT NULL
           AND a.migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                623);
    END;
  END IF;

  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12010, cod_acct_no,MIGRATION_SOURCE
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND ctr_disb <= 0
           AND dat_first_disb IS NOT NULL
           AND a.migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                623);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12011;
  /*consis 12011 : CTR_TERM_MONTHS must grater than zero and equals to (DAT_OF_MATURITY-DAT_FIRST_DISB)/30*/
  --    BEGIN
  --        SELECT /*+ PARALLEL(4) */
  --            COUNT(1)
  --        INTO var_l_count
  --        FROM
  --            co_ln_acct_dtls
  --        WHERE
  --                flg_mnt_status = 'A'
  --            AND ctr_term_months <= 0
  --            OR ctr_term_months <> round((dat_of_maturity - dat_first_disb) / 30);
  --
  --    EXCEPTION
  --        WHEN OTHERS THEN
  --            cbsfchost.ora_raiserror(sqlcode, 'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' || sqlerrm, 640);
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
  --            12011,
  --            'LN',
  --            var_l_table_name,
  --            'CTR_TERM_MONTHS',
  --            var_l_function_name,
  --            var_l_count,
  --            'CTR_TERM_MONTHS must grater than zero and equals to (DAT_OF_MATURITY-DAT_FIRST_DISB)/30'
  --        );
  --
  --    EXCEPTION
  --        WHEN OTHERS THEN
  --            cbsfchost.ora_raiserror(sqlcode, 'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' || sqlerrm, 663);
  --    END;
  --
  --    IF ( var_l_count > 0 ) THEN
  --        BEGIN
  --            INSERT INTO co_ln_consis_acct (
  --                cod_consis_no,
  --                cod_acct_no
  --            )
  --                SELECT /*+ PARALLEL(4) */
  --                    12011,
  --                    cod_acct_no
  --                FROM
  --                    co_ln_acct_dtls
  --                WHERE
  --                        flg_mnt_status = 'A'
  --                    AND ctr_term_months <= 0
  --                    OR ctr_term_months <> round((dat_of_maturity - dat_first_disb) / 30);
  --
  --        EXCEPTION
  --            WHEN OTHERS THEN
  --                cbsfchost.ora_raiserror(sqlcode, 'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' || sqlerrm, 683);
  --        END;
  --    END IF;
  /*consis 13 : DAT_ACCT_OPEN should not be null and not equals to 01-jan-1950 or 01-jan-1800*/
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12012;
  /*consis 12 : DAT_ACCT_OPEN should not be null and not equals to 01-jan-1950 or 01-jan-1800*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_acct_open IS NULL
        OR dat_acct_open IN
           (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'))
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_acct_open IS NULL
        OR dat_acct_open IN
           (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'))
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              700);
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
      (12012,
       'LN',
       var_l_table_name,
       'DAT_ACCT_OPEN',
       var_l_function_name,
       var_l_count,
       'DAT_ACCT_OPEN should not be null and not equals to 01-jan-1950 or 01-jan-1800');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12012,
       'MFI',
       var_l_table_name,
       'DAT_ACCT_OPEN',
       var_l_function_name,
       var_l_count1,
       'DAT_ACCT_OPEN should not be null and not equals to 01-jan-1950 or 01-jan-1800');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              723);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12012, cod_acct_no,MIGRATION_SOURCE
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND dat_acct_open IS NULL
            OR dat_acct_open IN
               (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'))
           AND a.migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                742);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12012, cod_acct_no,MIGRATION_SOURCE
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND dat_acct_open IS NULL
            OR dat_acct_open IN
               (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'))
           AND a.migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                742);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12013;
  /*consis 12013 : DAT_FIRST_DISB must be greater than DAT_ACCT_OPEN and should not be null if ctr_disb not equals to zero*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_first_disb < dat_acct_open
        OR (dat_first_disb IS NULL AND ctr_disb <> 0)
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_first_disb < dat_acct_open
        OR (dat_first_disb IS NULL AND ctr_disb <> 0)
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              760);
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
      (12013,
       'LN',
       var_l_table_name,
       'DAT_FIRST_DISB',
       var_l_function_name,
       var_l_count,
       'DAT_FIRST_DISB must be greater than DAT_ACCT_OPEN and should not be null if ctr_disb not equals to zero');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12013,
       'MFI',
       var_l_table_name,
       'DAT_FIRST_DISB',
       var_l_function_name,
       var_l_count1,
       'DAT_FIRST_DISB must be greater than DAT_ACCT_OPEN and should not be null if ctr_disb not equals to zero');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              783);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12013, cod_acct_no,MIGRATION_SOURCE
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND dat_first_disb < dat_acct_open
            OR (dat_first_disb IS NULL AND ctr_disb <> 0)
           AND a.migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                802);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12013, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND dat_first_disb < dat_acct_open
            OR (dat_first_disb IS NULL AND ctr_disb <> 0)
           AND a.migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                802);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12014;
  /*consis 12014 : DAT_LAST_CHARGED should not be null*/

  BEGIN
    SELECT /*+ PARALLEL(4)*/
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_charged IS NULL
       and ctr_disb > 0
       and cod_acct_stat not in (1, 11)
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4)*/
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_charged IS NULL
       and ctr_disb > 0
       and cod_acct_stat not in (1, 11)
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              819);
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
      (12014,
       'LN',
       var_l_table_name,
       'DAT_LAST_CHARGED',
       var_l_function_name,
       var_l_count,
       'DAT_LAST_CHARGED cannot be null');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12014,
       'MFI',
       var_l_table_name,
       'DAT_LAST_CHARGED',
       var_l_function_name,
       var_l_count1,
       'DAT_LAST_CHARGED cannot be null');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              842);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4)*/
         12014, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND dat_last_charged IS NULL
           and ctr_disb > 0
           and cod_acct_stat not in (1, 11)
           AND a.migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                860);
    END;
  END IF;
  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4)*/
         12014, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND dat_last_charged IS NULL
           and ctr_disb > 0
           and cod_acct_stat not in (1, 11)
           AND a.migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                860);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12015;
  /*consis 12015 : DAT_LAST_DISB must be grater than or equals to DAT_FIRST_DISB and should not be null if ctr_disb not equals to zero*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_disb < dat_first_disb
        OR (dat_last_disb IS NULL AND ctr_disb <> 0)
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_disb < dat_first_disb
        OR (dat_last_disb IS NULL AND ctr_disb <> 0)
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              878);
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
      (12015,
       'LN',
       var_l_table_name,
       'DAT_LAST_DISB',
       var_l_function_name,
       var_l_count,
       'DAT_LAST_DISB must be grater than or equals to DAT_FIRST_DISB and should not be null if ctr_disb not equals to zero');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12015,
       'MFI',
       var_l_table_name,
       'DAT_LAST_DISB',
       var_l_function_name,
       var_l_count1,
       'DAT_LAST_DISB must be grater than or equals to DAT_FIRST_DISB and should not be null if ctr_disb not equals to zero');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              901);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12015, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_disb < dat_first_disb
            OR (dat_last_disb IS NULL AND ctr_disb <> 0)
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                920);
    END;
  END IF;

  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12015, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_disb < dat_first_disb
            OR (dat_last_disb IS NULL AND ctr_disb <> 0)
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                920);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12016;
  /*consis 12016 : DAT_LAST_IOA must be same as ln_arrears_table.dat_arrears_due for arrears type I else it should be null*/ -- careful

  --  BEGIN
  --        SELECT /*+ PARALLEL(4) */
  --            COUNT(1)
  --        INTO var_l_count
  --        FROM
  --            (SELECT  a.cod_acct_no a_cod_acct_no,
  --                b.cod_acct_no b_cod_acct_no,
  --                b.dat_arrears_due b_dat_arrears_due,
  --                a .DAT_LAST_IOA  a_DAT_LAST_IOA
  --        FROM
  --            co_LN_ACCT_DTLS a ,(select * from ln_arrears_table where cod_arrear_type ='I') b
  --        WHERE flg_mnt_status ='A' AND  a.cod_acct_no = b.cod_acct_no (+)
  --        )
  --            where (b_dat_arrears_due<>a_DAT_LAST_IOA and b_cod_acct_no is not null and b_dat_arrears_due is not null  )
  --            or (a_DAT_LAST_IOA is not null and b_cod_acct_no is null );
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
  --            12016,
  --            'LN',
  --            var_l_table_name,
  --            'DAT_LAST_IOA',
  --            var_l_function_name,
  --            var_l_count,
  --            'DAT_LAST_IOA must be same as ln_arrears_table.dat_arrears_due for arrears type I else it should be null'
  --        );
  --
  --    END;
  --    IF ( var_l_count > 0 ) THEN begin
  --        INSERT INTO co_ln_consis_acct (
  --            cod_consis_no,
  --            cod_acct_no
  --        )
  --            SELECT /*+ PARALLEL(4) */
  --                12016,
  --                a_cod_acct_no
  --            from
  --           (SELECT  a.cod_acct_no a_cod_acct_no,
  --                b.cod_acct_no b_cod_acct_no,
  --                b.dat_arrears_due b_dat_arrears_due,
  --                a .DAT_LAST_IOA  a_DAT_LAST_IOA
  --        FROM
  --            co_LN_ACCT_DTLS a ,(select * from ln_arrears_table where cod_arrear_type ='I') b
  --        WHERE  flg_mnt_status ='A' AND a.cod_acct_no = b.cod_acct_no (+)
  --        )
  --            where (b_dat_arrears_due<>a_DAT_LAST_IOA and b_cod_acct_no is not null and b_dat_arrears_due is not null  )
  --            or (a_DAT_LAST_IOA is not null and b_cod_acct_no is null );
  --
  --    END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  /*consis 12018 : DAT_LAST_DUE*/ -- doubt

  var_l_consis_no := 12019;
  /*consis 12019 : DAT_OF_MATURITY should not be null and not equals to 01-jan-1950 or 01-jan-1800 */
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_of_maturity IS NULL
        OR dat_of_maturity IN
           (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'))
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_of_maturity IS NULL
        OR dat_of_maturity IN
           (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'))
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1065);
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
      (12019,
       'LN',
       var_l_table_name,
       'DAT_OF_MATURITY',
       var_l_function_name,
       var_l_count,
       'DAT_OF_MATURITY should not be null and not equals to 01-jan-1950 or 01-jan-1800');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12019,
       'MFI',
       var_l_table_name,
       'DAT_OF_MATURITY',
       var_l_function_name,
       var_l_count1,
       'DAT_OF_MATURITY should not be null and not equals to 01-jan-1950 or 01-jan-1800');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1088);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12019, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND dat_of_maturity IS NULL
            OR dat_of_maturity IN
               (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'))
           AND a.migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1107);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12019, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND dat_of_maturity IS NULL
            OR dat_of_maturity IN
               (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'))
           AND a.migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1107);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12020;
  /*consis 12020 : FLG_ACCR_STATUS should not be null or other than N,S*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND flg_accr_status IS NULL
        OR flg_accr_status NOT IN ('N', 'S')
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND flg_accr_status IS NULL
        OR flg_accr_status NOT IN ('N', 'S')
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1123);
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
      (12020,
       'LN',
       var_l_table_name,
       'FLG_ACCR_STATUS',
       var_l_function_name,
       var_l_count,
       'FLG_ACCR_STATUS should not be null or other than N,S');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12020,
       'MFI',
       var_l_table_name,
       'FLG_ACCR_STATUS',
       var_l_function_name,
       var_l_count1,
       'FLG_ACCR_STATUS should not be null or other than N,S');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1146);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12020, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND flg_accr_status IS NULL
            OR flg_accr_status NOT IN ('N', 'S')
           AND a.migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1655);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12020, cod_acct_no, migration_source
          FROM co_ln_acct_dtls a
         WHERE flg_mnt_status = 'A'
           AND flg_accr_status IS NULL
            OR flg_accr_status NOT IN ('N', 'S')
           AND a.migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1655);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12021;

  /*consis 22 : FLG_PAYOFF_NOTICE should not be other than Y,N*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND flg_payoff_notice IS NULL
        OR flg_payoff_notice NOT IN ('Y', 'N')
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND flg_payoff_notice IS NULL
        OR flg_payoff_notice NOT IN ('Y', 'N')
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1181);
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
      (12021,
       'LN',
       var_l_table_name,
       'FLG_PAYOFF_NOTICE',
       var_l_function_name,
       var_l_count,
       'FLG_PAYOFF_NOTICE should not be other than Y,N');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12021,
       'MFI',
       var_l_table_name,
       'FLG_PAYOFF_NOTICE',
       var_l_function_name,
       var_l_count1,
       'FLG_PAYOFF_NOTICE should not be other than Y,N');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1204);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12021, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND flg_payoff_notice IS NULL
            OR flg_payoff_notice NOT IN ('Y', 'N')
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1223);
    END;
  END IF;
  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12021, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND flg_payoff_notice IS NULL
            OR flg_payoff_notice NOT IN ('Y', 'N')
           AND migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1223);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12022;
  /*consis 12022 : FLG_MEMO should not be other than A,B,C,N*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND flg_memo IS NULL
        OR flg_memo NOT IN ('A', 'B', 'C', 'N')
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND flg_memo IS NULL
        OR flg_memo NOT IN ('A', 'B', 'C', 'N')
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1240);
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
      (12022,
       'LN',
       var_l_table_name,
       'FLG_PAYOFF_NOTICE',
       var_l_function_name,
       var_l_count,
       'FLG_MEMO should not be other than A,B,C,N');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12022,
       'MFI',
       var_l_table_name,
       'FLG_PAYOFF_NOTICE',
       var_l_function_name,
       var_l_count1,
       'FLG_MEMO should not be other than A,B,C,N');
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1263);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12022, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND flg_payoff_notice IS NULL
            OR flg_payoff_notice NOT IN ('A', 'B', 'C', 'N')
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1282);
    END;
  END IF;
  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12022, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND flg_payoff_notice IS NULL
            OR flg_payoff_notice NOT IN ('A', 'B', 'C', 'N')
           AND migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1282);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12023;
  /*consis 12023 : FLG_RESERVE_EXIST should not be other than Y,N*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND flg_reserve_exist IS NULL
        OR flg_reserve_exist NOT IN ('Y', 'N')
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND flg_reserve_exist IS NULL
        OR flg_reserve_exist NOT IN ('Y', 'N')
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1299);
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
      (12023,
       'LN',
       var_l_table_name,
       'FLG_RESERVE_EXIST',
       var_l_function_name,
       var_l_count,
       'FLG_RESERVE_EXIST should not be other than Y,N');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12023,
       'MFI',
       var_l_table_name,
       'FLG_RESERVE_EXIST',
       var_l_function_name,
       var_l_count1,
       'FLG_RESERVE_EXIST should not be other than Y,N');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1322);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12023, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND flg_reserve_exist IS NULL
            OR flg_reserve_exist NOT IN ('Y', 'N')
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1341);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12023, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND flg_reserve_exist IS NULL
            OR flg_reserve_exist NOT IN ('Y', 'N')
           AND migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1341);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12024;
  /*consis 12024 : NAM_CUST_SHRT should not be null*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND nam_cust_shrt IS NULL
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND nam_cust_shrt IS NULL
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1356);
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
      (12024,
       'LN',
       var_l_table_name,
       'NAM_CUST_SHRT',
       var_l_function_name,
       var_l_count,
       'NAM_CUST_SHRT cannot be null');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12024,
       'MFI',
       var_l_table_name,
       'NAM_CUST_SHRT',
       var_l_function_name,
       var_l_count1,
       'NAM_CUST_SHRT cannot be null');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1379);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12024, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND nam_cust_shrt IS NULL
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1397);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12024, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND nam_cust_shrt IS NULL
           AND migration_source = 'MFI';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1397);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12025;
  /*consis 12025 : CTR_INSTAL greater than 0 if DAT_LAST_DUE< migration date  else 0*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls a, co_mig_ln_acct_rate_chart_mapping b
     WHERE a.flg_mnt_status = 'A'
       AND (a.ctr_instal <= 0
           --                  AND dat_last_due < :var_dat_process
           --Exclude bullet loans
           and a.cod_acct_no = b.cod_acct_no and
           (b.cod_sched_type, b.cod_prod) not IN
           (select cod_sched_type, cod_prod
               from cbsfchost.ln_sched_types
              where frq_instal = 0
                and cod_instal_rule not in (103, 107, 106)))
       and a.migration_source = 'CBS';
    --or  (CTR_INSTAL <>0 and DAT_LAST_DUE > var_dat_process)   ; --saurabhsai

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls a, co_mig_ln_acct_rate_chart_mapping b
     WHERE a.flg_mnt_status = 'A'
       AND (a.ctr_instal <= 0
           --                  AND dat_last_due < :var_dat_process
           --Exclude bullet loans
           and a.cod_acct_no = b.cod_acct_no and
           (b.cod_sched_type, b.cod_prod) not IN
           (select cod_sched_type, cod_prod
               from cbsfchost.ln_sched_types
              where frq_instal = 0
                and cod_instal_rule not in (103, 107, 106)))
       and a.migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1414);
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
      (12025,
       'LN',
       var_l_table_name,
       'CTR_INSTAL',
       var_l_function_name,
       var_l_count,
       'Installment counter is less than or equal to 0 when last due date is less than migration date/current process date.');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12025,
       'MFI',
       var_l_table_name,
       'CTR_INSTAL',
       var_l_function_name,
       var_l_count1,
       'Installment counter is less than or equal to 0 when last due date is less than migration date/current process date.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1437);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12025, a.cod_acct_no, a.migration_source
          FROM co_ln_acct_dtls a, co_mig_ln_acct_rate_chart_mapping b
         WHERE a.flg_mnt_status = 'A'
           AND (a.ctr_instal <= 0
               --                  AND dat_last_due < :var_dat_process
               --Exclude bullet loans
               and a.cod_acct_no = b.cod_acct_no and
               (b.cod_sched_type, b.cod_prod) not IN
               (select cod_sched_type, cod_prod
                   from cbsfchost.ln_sched_types
                  where frq_instal = 0
                    and cod_instal_rule not in (103, 107, 106)))
           and rownum <= 10 --sample 10 records esaf_changes;
           and a.migration_source = 'CBS';
      /*OR ( ctr_instal <> 0
      AND dat_last_due > var_dat_process ); --var_dat_process*/
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1456);
    END;
  END IF;

  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12025, a.cod_acct_no, a.migration_source
          FROM co_ln_acct_dtls a, co_mig_ln_acct_rate_chart_mapping b
         WHERE a.flg_mnt_status = 'A'
           AND (a.ctr_instal <= 0
               --                  AND dat_last_due < :var_dat_process
               --Exclude bullet loans
               and a.cod_acct_no = b.cod_acct_no and
               (b.cod_sched_type, b.cod_prod) not IN
               (select cod_sched_type, cod_prod
                   from cbsfchost.ln_sched_types
                  where frq_instal = 0
                    and cod_instal_rule not in (103, 107, 106)))
           and rownum <= 10 --sample 10 records esaf_changes;
           and a.migration_source = 'BRNET';
      /*OR ( ctr_instal <> 0
      AND dat_last_due > var_dat_process ); --var_dat_process*/
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1456);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12026;
  /*consis 12026 : DAT_PRINC_REPAY_STRT should not be null*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND (dat_princ_repay_strt IS NULL AND
           dat_last_payment < var_dat_process)
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND (dat_princ_repay_strt IS NULL AND
           dat_last_payment < var_dat_process)
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1472);
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
      (12026,
       'LN',
       var_l_table_name,
       'DAT_PRINC_REPAY_STRT',
       var_l_function_name,
       var_l_count,
       'DAT_PRINC_REPAY_STRT cannot be null WHEN last payment date is less than current process date/migration date.');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12026,
       'MFI',
       var_l_table_name,
       'DAT_PRINC_REPAY_STRT',
       var_l_function_name,
       var_l_count1,
       'DAT_PRINC_REPAY_STRT cannot be null WHEN last payment date is less than current process date/migration date.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1495);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12026, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_princ_repay_strt IS NULL
           AND migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1513);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12026, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_princ_repay_strt IS NULL
           AND migration_source = 'MFI';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1513);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12027;
  /*consis 12027 : CTR_CURR_TERM_MONTHS must be less than equals to ctr_term_months*/
  /*--this consis was commented in capri*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND ctr_curr_term_months > ctr_term_months
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND ctr_curr_term_months > ctr_term_months
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1530);
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
      (12027,
       --'LN',
       'INFO', --'LN', --FA:29-Apr-2024, moved to INFO
       var_l_table_name,
       'CTR_CURR_TERM_MONTHS',
       var_l_function_name,
       var_l_count,
       'CTR_CURR_TERM_MONTHS must be less than equals to ctr_term_months');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12027,
       --'LN',
       'MFI-INFO', --'LN', --FA:29-Apr-2024, moved to INFO
       var_l_table_name,
       'CTR_CURR_TERM_MONTHS',
       var_l_function_name,
       var_l_count1,
       'CTR_CURR_TERM_MONTHS must be less than equals to ctr_term_months');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1553);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12027, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND ctr_curr_term_months > ctr_term_months
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1571);
    END;
  END IF;

  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12027, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND ctr_curr_term_months > ctr_term_months
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1571);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12028;
  /*consis 12028 : COD_ACCT_DATE_BASIS should not be other than 1,2*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_acct_date_basis NOT IN (1, 2)
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_acct_date_basis NOT IN (1, 2)
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1588);
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
      (12028,
       'LN',
       var_l_table_name,
       'COD_ACCT_DATE_BASIS',
       var_l_function_name,
       var_l_count,
       'COD_ACCT_DATE_BASIS should not be other than 1,2');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12028,
       'MFI',
       var_l_table_name,
       'COD_ACCT_DATE_BASIS',
       var_l_function_name,
       var_l_count1,
       'COD_ACCT_DATE_BASIS should not be other than 1,2');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1611);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12028, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND cod_acct_date_basis NOT IN (1, 2)
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1628);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12028, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND cod_acct_date_basis NOT IN (1, 2)
           AND migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1628);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12029;
  /*consis 12029 : FLG_SUBSIDY_TYP should not be other than R,S*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND flg_subsidy_typ NOT IN ('R', 'S')
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND flg_subsidy_typ NOT IN ('R', 'S')
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1645);
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
      (12029,
       'LN',
       var_l_table_name,
       'COD_ACCT_DATE_BASIS',
       var_l_function_name,
       var_l_count,
       'COD_ACCT_DATE_BASIS should not be other than 1,2');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12029,
       'MFI',
       var_l_table_name,
       'COD_ACCT_DATE_BASIS',
       var_l_function_name,
       var_l_count1,
       'COD_ACCT_DATE_BASIS should not be other than 1,2');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1668);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12029, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND flg_subsidy_typ NOT IN ('R', 'S')
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1686);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12029, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND flg_subsidy_typ NOT IN ('R', 'S')
           AND migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1686);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12030;
  /*consis 12030 : DAT_LAST_APOA should not be null*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_apoa IS NULL
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_apoa IS NULL
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1701);
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
      (12030,
       'LN',
       var_l_table_name,
       'DAT_LAST_APOA',
       var_l_function_name,
       var_l_count,
       'DAT_LAST_APOA cannot be null');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12030,
       'MFI',
       var_l_table_name,
       'DAT_LAST_APOA',
       var_l_function_name,
       var_l_count1,
       'DAT_LAST_APOA cannot be null');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1724);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12030, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_apoa IS NULL
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1742);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12030, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_apoa IS NULL
           AND migration_source = 'MFI';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1742);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12031;
  /*consis 12031 : DAT_LAST_PENALTY_ACCRUAL should not be null*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_penalty_accrual IS NULL
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_penalty_accrual IS NULL
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1758);
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
      (12031,
       'LN',
       var_l_table_name,
       'DAT_LAST_PENALTY_ACCRUAL',
       var_l_function_name,
       var_l_count,
       'DAT_LAST_PENALTY_ACCRUAL cannot be null');

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12031,
       'MFI',
       var_l_table_name,
       'DAT_LAST_PENALTY_ACCRUAL',
       var_l_function_name,
       var_l_count1,
       'DAT_LAST_PENALTY_ACCRUAL cannot be null');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1781);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12031, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_penalty_accrual IS NULL
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12031, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_penalty_accrual IS NULL
           AND migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12032;
  /*consis 12032 :  validation Should be reverse of this:: ideal case : DAT_SANCTION AND dat_sanction <= dat_first_disb
  and dat_sanction >= dat_Acct_open;*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND (dat_sanction > dat_first_disb) /* two separate condition esaf_changes*/
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND (dat_sanction > dat_first_disb) /* two separate condition esaf_changes*/
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1816);
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
      (12032,
       'LN',
       var_l_table_name,
       'DAT_SANCTION',
       var_l_function_name,
       var_l_count,
       'DAT_SANCTION must be less than equal to first disbursement date.'
       --            'DAT_SANCTION must be less than equal to first disbursement date and should not be less than account open date.'
       );

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12032,
       'MFI',
       var_l_table_name,
       'DAT_SANCTION',
       var_l_function_name,
       var_l_count1,
       'DAT_SANCTION must be less than equal to first disbursement date.'
       --            'DAT_SANCTION must be less than equal to first disbursement date and should not be less than account open date.'
       );

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1839);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12032, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND (dat_sanction > dat_first_disb OR
               dat_sanction < dat_acct_open)
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1857);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12032, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND (dat_sanction > dat_first_disb OR
               dat_sanction < dat_acct_open)
           AND migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1857);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12033;
  /*consis 12033 : DAT_LOAN_PAPERS must be equals to first disb date*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND (dat_loan_papers > dat_first_disb OR
           dat_loan_papers < dat_acct_open)
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND (dat_loan_papers > dat_first_disb OR
           dat_loan_papers < dat_acct_open)
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1874);
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
      (12033,
       'LN',
       var_l_table_name,
       'DAT_LOAN_PAPERS',
       var_l_function_name,
       var_l_count,
       'DAT_LOAN_PAPERS must be less than equal to first disbursement date'
       --            'DAT_LOAN_PAPERS must be less than equal to first disbursement date and should not be less than account open date.'
       );

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12033,
       'MFI',
       var_l_table_name,
       'DAT_LOAN_PAPERS',
       var_l_function_name,
       var_l_count1,
       'DAT_LOAN_PAPERS must be less than equal to first disbursement date'
       --            'DAT_LOAN_PAPERS must be less than equal to first disbursement date and should not be less than account open date.'
       );

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1897);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12033, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND (dat_loan_papers > dat_first_disb OR
               dat_loan_papers < dat_acct_open)
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1915);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12033, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND (dat_loan_papers > dat_first_disb OR
               dat_loan_papers < dat_acct_open)
           AND migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1915);
    END;
  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis

  --  var_l_consis_no := 12035;
  --    BEGIN
  --        SELECT /*+ PARALLEL(4) */
  --            COUNT(1)
  --        INTO var_l_count
  --        FROM
  --            co_ln_acct_dtls
  --        WHERE
  --                flg_mnt_status = 'A'
  --            AND amt_face_value_org <> 0;
  --
  --    EXCEPTION
  --        WHEN OTHERS THEN
  --            ora_raiserror(sqlcode, 'In #' || var_l_function_name || '# Select From co_ln_acct_dtls Failed.' || sqlerrm, 1758);
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
  --            'AMT_FACE_VALUE_ORG',
  --            var_l_function_name,
  --            var_l_count,
  --            'LOANS WHERE AMT_FACE_VALUE_ORG should be zero.'
  --        );
  --
  --    EXCEPTION
  --        WHEN OTHERS THEN
  --            ora_raiserror(sqlcode, 'In #' || var_l_function_name || '# INSERT INTO co_ln_consis Failed.' || sqlerrm, 1781);
  --    END;
  --
  --    IF ( var_l_count > 0 ) THEN
  --        BEGIN
  --            INSERT INTO co_ln_consis_acct (
  --                cod_consis_no,
  --                cod_acct_no
  --            )
  --                SELECT /*+ PARALLEL(4) */
  --                    var_l_consis_no,
  --                    cod_acct_no
  --                FROM
  --                    co_ln_acct_dtls
  --                WHERE
  --                        flg_mnt_status = 'A'
  --                    AND amt_face_value_org <> 0;
  --
  --        EXCEPTION
  --            WHEN OTHERS THEN
  --                ora_raiserror(sqlcode, 'In #' || var_l_function_name || '# INSERT INTO co_ln_consis_acct Failed.' || sqlerrm, 1799);
  --        END;
  --    END IF;
  --
  --  ap_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' || var_l_function_name || '# Stream = ' || var_pi_stream); --after each consis
  --  COMMIT;

  var_l_consis_no := 12036;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_charged > var_dat_process
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_charged > var_dat_process
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_ln_acct_dtls Failed.' ||
                              sqlerrm,
                              1758);
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
       'DAT_LAST_CHARGED',
       var_l_function_name,
       var_l_count,
       'LOANS WHERE dat_last_charged >  dat_process.');

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
       'DAT_LAST_CHARGED',
       var_l_function_name,
       var_l_count1,
       'LOANS WHERE dat_last_charged >  dat_process.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1781);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_charged > var_dat_process
           and rownum <= 10 --sample 10 records esaf_changes ;
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;

  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_charged > var_dat_process
           and rownum <= 10 --sample 10 records esaf_changes ;
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12037;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_acct_open > var_dat_process
       AND migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_acct_open > var_dat_process
       AND migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_ln_acct_dtls Failed.' ||
                              sqlerrm,
                              1758);
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
       'DAT_ACCT_OPEN',
       var_l_function_name,
       var_l_count,
       'LOANS WHERE dat_acct_open >  dat_process.');

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
       'DAT_ACCT_OPEN',
       var_l_function_name,
       var_l_count1,
       'LOANS WHERE dat_acct_open >  dat_process.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1781);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_acct_open > var_dat_process
           and rownum <= 10 --sample 10 records esaf_changes;
           AND migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;

  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_acct_open > var_dat_process
           and rownum <= 10 --sample 10 records esaf_changes;
           AND migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12038;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_ioa < dat_acct_open
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_ioa < dat_acct_open
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_ln_acct_dtls Failed.' ||
                              sqlerrm,
                              1758);
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
       'DAT_LAST_IOA',
       var_l_function_name,
       var_l_count,
       'DAT_LAST_IOA IS LESS THAN DAT_ACCT_OPEN');

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
       'DAT_LAST_IOA',
       var_l_function_name,
       var_l_count1,
       'DAT_LAST_IOA IS LESS THAN DAT_ACCT_OPEN');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1781);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_ioa < dat_acct_open
           and rownum <= 10 --sample 10 records esaf_changes ;
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;

  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_ioa < dat_acct_open
           and rownum <= 10 --sample 10 records esaf_changes ;
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

 /* var_l_consis_no := 12039;
  BEGIN
    SELECT \*+ PARALLEL(4) *\
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_sched_type NOT IN
           (SELECT cod_sched_type FROM cbsfchost.ln_sched_types)
       and migration_source = 'CBS';

    SELECT \*+ PARALLEL(4) *\
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND cod_sched_type NOT IN
           (SELECT cod_sched_type FROM cbsfchost.ln_sched_types)
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_ln_acct_dtls Failed.' ||
                              sqlerrm,
                              1758);
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
       'COD_SCHED_TYPE',
       var_l_function_name,
       var_l_count,
       'To be moved to post check. COD_SCHED_TYPE IS NOT IN cbsfchost.ln_sched_types');

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
       'COD_SCHED_TYPE',
       var_l_function_name,
       var_l_count1,
       'To be moved to post check. COD_SCHED_TYPE IS NOT IN cbsfchost.ln_sched_types');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1781);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT \*+ PARALLEL(4) *\
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND cod_sched_type NOT IN
               (SELECT cod_sched_type FROM cbsfchost.ln_sched_types)
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;

  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT \*+ PARALLEL(4) *\
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND cod_sched_type NOT IN
               (SELECT cod_sched_type FROM cbsfchost.ln_sched_types)
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;*/

  var_l_consis_no := 12040;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_acct_close IS NOT NULL
       AND cod_acct_stat = 8
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_acct_close IS NOT NULL
       AND cod_acct_stat = 8
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_ln_acct_dtls Failed.' ||
                              sqlerrm,
                              1758);
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
       'DAT_ACCT_CLOSE',
       var_l_function_name,
       var_l_count,
       'DAT_ACCT_CLOSE is present for active accounts');

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
       'DAT_ACCT_CLOSE',
       var_l_function_name,
       var_l_count1,
       'DAT_ACCT_CLOSE is present for active accounts');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1781);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_acct_close IS NOT NULL
           AND cod_acct_stat = 8
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;

  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_acct_close IS NOT NULL
           AND cod_acct_stat = 8
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12041;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_accrual IS NULL
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_accrual IS NULL
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_ln_acct_dtls Failed.' ||
                              sqlerrm,
                              1758);
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
       'DAT_LAST_ACCRUAL',
       var_l_function_name,
       var_l_count,
       'dat_last_accrual IS NULL');

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
       'DAT_LAST_ACCRUAL',
       var_l_function_name,
       var_l_count1,
       'dat_last_accrual IS NULL');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1781);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_accrual IS NULL
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_accrual IS NULL
           AND migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12043;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND NVL(flg_repl_cube, 'X') NOT IN ('Y', 'N')
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND NVL(flg_repl_cube, 'X') NOT IN ('Y', 'N')
       and migration_source = 'BRNET';
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_ln_acct_dtls Failed.' ||
                              sqlerrm,
                              1758);
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
       'FLG_REPL_CUBE',
       var_l_function_name,
       var_l_count,
       'FLG_REPL_CUBE IS NOT IN Y,N');

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
       'FLG_REPL_CUBE',
       var_l_function_name,
       var_l_count1,
       'FLG_REPL_CUBE IS NOT IN Y,N');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1781);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND NVL(flg_repl_cube, 'X') NOT IN ('Y', 'N')
           AND migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND NVL(flg_repl_cube, 'X') NOT IN ('Y', 'N')
           AND migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12044;

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND ctr_instal < 0
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND ctr_instal < 0
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_ln_acct_dtls Failed.' ||
                              sqlerrm,
                              1414);
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
       var_l_function_name,
       var_l_count,
       'Installment counter is less than 0');

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
       var_l_function_name,
       var_l_count1,
       'Installment counter is less than 0');
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1437);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND ctr_instal < 0
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1456);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND ctr_instal < 0
           AND migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1456);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12045;

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_charged < dat_first_disb
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_charged < dat_first_disb
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_ln_acct_dtls Failed.' ||
                              sqlerrm,
                              1414);
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
       'DAT_LAST_CHARGED',
       var_l_function_name,
       var_l_count,
       'ACCOUNTS where dat_last_charged is less than the dat_first_disb');

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
       'DAT_LAST_CHARGED',
       var_l_function_name,
       var_l_count1,
       'ACCOUNTS where dat_last_charged is less than the dat_first_disb');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1437);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_charged < dat_first_disb
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1456);
    END;
  END IF;

  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_charged < dat_first_disb
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1456);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12046;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_accrual < dat_last_restructure
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_accrual < dat_last_restructure
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_ln_acct_dtls Failed.' ||
                              sqlerrm,
                              1758);
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
       'DAT_LAST_ACCRUAL',
       var_l_function_name,
       var_l_count,
       'ACCOUNTS where dat_last_accrual is less than the dat_last_restructure');

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
       'DAT_LAST_ACCRUAL',
       var_l_function_name,
       var_l_count1,
       'ACCOUNTS where dat_last_accrual is less than the dat_last_restructure');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1781);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_accrual < dat_last_restructure
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_accrual < dat_last_restructure
           AND migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12047;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND (regexp_replace(ctr_term_months, '[^0-9]', '') = '0')
        OR (regexp_replace(ctr_term_months, '[^0-9]', '') IS NULL)
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND (regexp_replace(ctr_term_months, '[^0-9]', '') = '0')
        OR (regexp_replace(ctr_term_months, '[^0-9]', '') IS NULL)
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_ln_acct_dtls Failed.' ||
                              sqlerrm,
                              1758);
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
       'CTR_TERM_MONTHS',
       var_l_function_name,
       var_l_count,
       'ACCOUNTS where CTR_TERM_MONTHS is 0 or null');

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
       'CTR_TERM_MONTHS',
       var_l_function_name,
       var_l_count1,
       'ACCOUNTS where CTR_TERM_MONTHS is 0 or null');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1781);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND (regexp_replace(ctr_term_months, '[^0-9]', '') = '0')
            OR (regexp_replace(ctr_term_months, '[^0-9]', '') IS NULL)
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND (regexp_replace(ctr_term_months, '[^0-9]', '') = '0')
            OR (regexp_replace(ctr_term_months, '[^0-9]', '') IS NULL)
           AND migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12048;

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_first_disb > dat_last_penalty_accrual
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_first_disb > dat_last_penalty_accrual
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_ln_acct_dtls Failed.' ||
                              sqlerrm,
                              581);
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
       'DAT_LAST_PENALTY_ACCRUAL',
       var_l_function_name,
       var_l_count,
       'ACCOUNTS where dat_last_penalty_accrual is less than dat_first_disb');

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
       'DAT_LAST_PENALTY_ACCRUAL',
       var_l_function_name,
       var_l_count1,
       'ACCOUNTS where dat_last_penalty_accrual is less than dat_first_disb');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              604);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_first_disb > dat_last_penalty_accrual
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                623);
    END;
  END IF;

  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_first_disb > dat_last_penalty_accrual
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                623);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  --  var_l_consis_no := 12049;
  --
  --    BEGIN
  --        SELECT /*+ PARALLEL(4) */
  --            COUNT(1)
  --        INTO var_l_count
  --        FROM
  --            co_ln_acct_dtls
  --        WHERE
  --                flg_mnt_status = 'A'
  --            AND net_rate = 0;
  --
  --    EXCEPTION
  --        WHEN OTHERS THEN
  --            ora_raiserror(sqlcode, 'In #' || var_l_function_name || '# Select From co_ln_acct_dtls Failed.' || sqlerrm, 581);
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
  --            'NET_RATE',
  --            var_l_function_name,
  --            var_l_count,
  --            'ACCOUNTS where net_rate is 0'
  --        );
  --
  --    EXCEPTION
  --        WHEN OTHERS THEN
  --            ora_raiserror(sqlcode, 'In #' || var_l_function_name || '# INSERT INTO co_ln_consis Failed.' || sqlerrm, 604);
  --    END;
  --
  --    IF ( var_l_count > 0 ) THEN
  --        BEGIN
  --            INSERT INTO co_ln_consis_acct (
  --                cod_consis_no,
  --                cod_acct_no
  --            )
  --                SELECT /*+ PARALLEL(4) */
  --                    var_l_consis_no,
  --                    cod_acct_no
  --                FROM
  --                    co_ln_acct_dtls
  --                WHERE
  --                        flg_mnt_status = 'A'
  --                    AND net_rate = 0;
  --
  --        EXCEPTION
  --            WHEN OTHERS THEN
  --                ora_raiserror(sqlcode, 'In #' || var_l_function_name || '# INSERT INTO co_ln_consis_acct Failed.' || sqlerrm, 623);
  --        END;
  --    END IF;
  --
  --  ap_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' || var_l_function_name || '# Stream = ' || var_pi_stream); --after each consis
  --  COMMIT;

  var_l_consis_no := 12050;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_first_disb > dat_last_restructure
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_first_disb > dat_last_restructure
       and migration_source = 'BRNET';
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_ln_acct_dtls Failed.' ||
                              sqlerrm,
                              1758);
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
       'DAT_LAST_RESTRUCTURE',
       var_l_function_name,
       var_l_count,
       'ACCOUNTS where dat_last_restructure is less than the dat_first_disb');

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
       'DAT_LAST_RESTRUCTURE',
       var_l_function_name,
       var_l_count1,
       'ACCOUNTS where dat_last_restructure is less than the dat_first_disb');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1781);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_first_disb > dat_last_restructure
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;

  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_first_disb > dat_last_restructure
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 12051;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_accrual < dat_first_disb
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND dat_last_accrual < dat_first_disb
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_ln_acct_dtls Failed.' ||
                              sqlerrm,
                              1758);
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
       'DAT_LAST_ACCRUAL',
       var_l_function_name,
       var_l_count,
       'ACCOUNTS where dat_last_accrual is less than the dat_first_disb');

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
       'DAT_LAST_ACCRUAL',
       var_l_function_name,
       var_l_count1,
       'ACCOUNTS where dat_last_accrual is less than the dat_first_disb');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1781);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_accrual < dat_first_disb
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;

  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND dat_last_accrual < dat_first_disb
           and rownum <= 10 --sample 10 records esaf_changes;
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1799);
    END;
  END IF;
 ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;
  var_l_consis_no := 12052;
  /*consis 12032 :  validation Should be reverse of this:: ideal case : DAT_SANCTION AND dat_sanction <= dat_first_disb
  and dat_sanction >= dat_Acct_open;*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND (dat_sanction < dat_Acct_open) /* two separate condition esaf_changes*/
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_acct_dtls
     WHERE flg_mnt_status = 'A'
       AND (dat_sanction < dat_Acct_open) /* two separate condition esaf_changes*/
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1816);
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
      (12052,
       'LN',
       var_l_table_name,
       'DAT_SANCTION',
       var_l_function_name,
       var_l_count,
       'DAT_SANCTION must be greater than equal to dat_Acct_open.'
       --            'DAT_SANCTION must be less than equal to first disbursement date and should not be less than account open date.'
       );

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12052,
       'MFI',
       var_l_table_name,
       'DAT_SANCTION',
       var_l_function_name,
       var_l_count1,
       'DAT_SANCTION must be greater than equal to dat_Acct_open.'
       --            'DAT_SANCTION must be less than equal to first disbursement date and should not be less than account open date.'
       );

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1839);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12052, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND (dat_sanction > dat_first_disb OR
               dat_sanction < dat_acct_open)
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1857);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12052, cod_acct_no, migration_source
          FROM co_ln_acct_dtls
         WHERE flg_mnt_status = 'A'
           AND (dat_sanction > dat_first_disb OR
               dat_sanction < dat_acct_open)
           AND migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1857);
    END;
  END IF;
 

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;


var_l_consis_no := 12053;
  /*consis 12032 :  validation Should be reverse of this:: ideal case : DAT_SANCTION AND dat_sanction <= dat_first_disb
  and dat_sanction >= dat_Acct_open;*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM CO_LN_ACCT_DTLS A
           WHERE NVL(A.FLG_ACCR_STATUS, 'S') <> 'N' /* two separate condition esaf_changes*/
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM CO_LN_ACCT_DTLS A
           WHERE NVL(A.FLG_ACCR_STATUS, 'S') <> 'N' /* two separate condition esaf_changes*/
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS Select From co_LN_ACCT_DTLS Failed.' ||
                              sqlerrm,
                              1816);
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
      (12053,
       'LN',
       var_l_table_name,
       'FLG_ACCR_STATUS',
       var_l_function_name,
       var_l_count,
       'accounts where accrual status is SUSPENDED'
       --            'DAT_SANCTION must be less than equal to first disbursement date and should not be less than account open date.'
       );

    INSERT INTO co_ln_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (12053,
       'MFI',
       var_l_table_name,
       'FLG_ACCR_STATUS',
       var_l_function_name,
       var_l_count1,
       'accounts where accrual status is SUSPENDED'
       --            'DAT_SANCTION must be less than equal to first disbursement date and should not be less than account open date.'
       );

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1839);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12053, cod_acct_no, migration_source
         FROM CO_LN_ACCT_DTLS A
           WHERE NVL(A.FLG_ACCR_STATUS, 'S') <> 'N'
           AND migration_source = 'CBS';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1857);
    END;
  END IF;
  IF (var_l_count1 > 0) THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         12053, cod_acct_no, migration_source
          FROM CO_LN_ACCT_DTLS A
           WHERE NVL(A.FLG_ACCR_STATUS, 'S') <> 'N'
           AND migration_source = 'BRNET';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_LN_ACCT_DTLS INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1857);
    END;
  END IF;
 

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #ap_cons_ln_acct_dtls# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;
  
  ap_bb_mig_log_string('99999 #' || var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --Ending of function
  RETURN 0;
END AP_CONS_LN_ACCT_DTLS;
/
