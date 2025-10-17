CREATE OR REPLACE FUNCTION "AP_CONS_LN_ARREARS_TABLE" (var_pi_stream IN NUMBER)
  RETURN NUMBER AS
  var_l_count                     NUMBER;
  var_l_count1                    NUMBER;
  var_l_dist_count                NUMBER := 0;
  var_l_log_in_dtl_ln_arrears_tbl VARCHAR2(1) := 'N';
  var_l_consis_no                 NUMBER := 0;
  var_l_function_name             VARCHAR2(100) := 'ap_cons_ln_arrears_table';
  var_l_table_name                VARCHAR2(100) := 'CO_LN_ARREARS_TABLE';
  var_l_cutoff_date               DATE := cbsfchost.pk_ba_global.dat_last_process; /*nvl(ap_get_data_mig_param('CUTOFF_DATE'),
                                              cbsfchost.pk_ba_global.dat_last_process);*/
BEGIN
  ap_bb_mig_log_string('Started #' || var_l_function_name || '# Stream = ' ||
                       var_pi_stream);
  DELETE FROM co_ln_consis
   WHERE upper(nam_table) = upper('LN_ARREARS_TABLE');

  DELETE FROM co_ln_consis_acct
   WHERE cod_consis_no >= 25001
     AND cod_consis_no <= 25100;

  COMMIT;
  ap_bb_mig_log_string('00000 #' || var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --Beginning of function
  var_l_log_in_dtl_ln_arrears_tbl := 'N'; /*nvl(ap_get_data_mig_param('LOG_DTL_LN_ARRAERS_TABLE'),
                                         'N');*/
  /* COD_ACCT_NO must be present in ln_acct_dtls : consis 25001
  */
  var_l_consis_no := 25001;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE cod_acct_no NOT IN
           (SELECT cod_acct_no
              FROM co_ln_acct_dtls
             WHERE flg_mnt_status = 'A')
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE cod_acct_no NOT IN
           (SELECT cod_acct_no
              FROM co_ln_acct_dtls
             WHERE flg_mnt_status = 'A')
       and migration_source = 'BRNET';
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              34);
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
       'COD_ACCT_NO',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'COD_ACCT_NO must be present in co_ln_acct_dtls');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              58);
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
       'COD_ACCT_NO',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'COD_ACCT_NO must be present in co_ln_acct_dtls');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              58);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE cod_acct_no NOT IN
               (SELECT cod_acct_no
                  FROM co_ln_acct_dtls
                 WHERE flg_mnt_status = 'A')
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                82);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE cod_acct_no NOT IN
               (SELECT cod_acct_no
                  FROM co_ln_acct_dtls
                 WHERE flg_mnt_status = 'A')
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                82);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25002;
  /*REF_BILLNO_SRL should not be null : consis 25002*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE (ref_billno_srl IS NULL OR ref_billno_srl = 0)
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE (ref_billno_srl IS NULL OR ref_billno_srl = 0)
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              98);
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
       'REF_BILLNO_SRL',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'REF_BILLNO_SRL should not be null or 0.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              122);
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
       'REF_BILLNO_SRL',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'REF_BILLNO_SRL should not be null or 0.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              122);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE (ref_billno_srl IS NULL OR ref_billno_srl = 0)
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                142);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE (ref_billno_srl IS NULL OR ref_billno_srl = 0)
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                142);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25003;
  /*AMT_ARREARS_ASSESSED should not be null or less than equals to zero: consis 25003*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE amt_arrears_assessed IS NULL
        OR amt_arrears_assessed <= 0
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE amt_arrears_assessed IS NULL
        OR amt_arrears_assessed <= 0
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              158);
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
       'AMT_ARREARS_ASSESSED',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'AMT_ARREARS_ASSESSED should not be null or less than equals to zero');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              182);
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
       'AMT_ARREARS_ASSESSED',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'AMT_ARREARS_ASSESSED should not be null or less than equals to zero');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              182);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE amt_arrears_assessed IS NULL
            OR amt_arrears_assessed <= 0
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                202);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE amt_arrears_assessed IS NULL
            OR amt_arrears_assessed <= 0
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                202);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25004;
  /*AMT_ARREARS_DUE should null or greater than AMT_ARREARS_ASSESSED: consis 25004 */
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE amt_arrears_due IS NULL
        OR amt_arrears_due > amt_arrears_assessed
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE amt_arrears_due IS NULL
        OR amt_arrears_due > amt_arrears_assessed
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
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
      (var_l_consis_no,
       'LN',
       var_l_table_name,
       'AMT_ARREARS_DUE',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'AMT_ARREARS_DUE should not be null or greater than AMT_ARREARS_ASSESSED');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              242);
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
       'AMT_ARREARS_DUE',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'AMT_ARREARS_DUE should not be null or greater than AMT_ARREARS_ASSESSED');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              242);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE amt_arrears_due IS NULL
            OR amt_arrears_due > amt_arrears_assessed
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                262);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE amt_arrears_due IS NULL
            OR amt_arrears_due > amt_arrears_assessed
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                262);
    END;
  END IF;

  /*AMT_ARREARS_WAIVED can not be null: consis 25005*/
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25005;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE amt_arrears_waived IS NULL
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE amt_arrears_waived IS NULL
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              277);
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
       'AMT_ARREARS_WAIVED',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'AMT_ARREARS_WAIVED should not be null');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              305);
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
       'AMT_ARREARS_WAIVED',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'AMT_ARREARS_WAIVED should not be null');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              305);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE amt_arrears_waived IS NULL
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                319);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE amt_arrears_waived IS NULL
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                319);
    END;
  END IF;

  /*AMT_ARREARS_CAP can not be null: consis 25006*/
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25006;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE amt_arrears_cap IS NULL
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE amt_arrears_cap IS NULL
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              334);
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
       'AMT_ARREARS_WAIVED',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'AMT_ARREARS_CAP should not be null');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              357);
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
       'AMT_ARREARS_WAIVED',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'AMT_ARREARS_CAP should not be null');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              357);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE amt_arrears_cap IS NULL
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                376);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE amt_arrears_cap IS NULL
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                376);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25007;
  /*COD_CHARGE_CCY should be as INR: consis 25007*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE cod_charge_ccy NOT IN
           (SELECT cod_ccy
              FROM cbsfchost.ba_ccy_code
             WHERE nam_ccy_short = 'INR'
               AND flg_mnt_status = 'A')
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE cod_charge_ccy NOT IN
           (SELECT cod_ccy
              FROM cbsfchost.ba_ccy_code
             WHERE nam_ccy_short = 'INR'
               AND flg_mnt_status = 'A')
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              399);
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
       'COD_CHARGE_CCY',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'COD_CHARGE_CCY should be as INR');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              423);
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
       'COD_CHARGE_CCY',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'COD_CHARGE_CCY should be as INR');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              423);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE cod_charge_ccy NOT IN
               (SELECT cod_ccy
                  FROM cbsfchost.ba_ccy_code
                 WHERE nam_ccy_short = 'INR'
                   AND flg_mnt_status = 'A')
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                450);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE cod_charge_ccy NOT IN
               (SELECT cod_ccy
                  FROM cbsfchost.ba_ccy_code
                 WHERE nam_ccy_short = 'INR'
                   AND flg_mnt_status = 'A')
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                450);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25008;
  /*COD_ARREAR_TYPE should not be null or other than C, A,L, P,M, I, N: consis 25008*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE (cod_arrear_type IS NULL OR
           cod_arrear_type NOT IN ('C',
                                    'A',
                                    'L',
                                    'P',
                                    'M',
                                    'I',
                                    'N',
                                    'T',
                                    'U',
                                    'F',
                                    'D',
                                    'S',
                                    'E',
                                    'O',
                                    'R',
                                    '1')) --CAPRI_CHANGE
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE (cod_arrear_type IS NULL OR
           cod_arrear_type NOT IN ('C',
                                    'A',
                                    'L',
                                    'P',
                                    'M',
                                    'I',
                                    'N',
                                    'T',
                                    'U',
                                    'F',
                                    'D',
                                    'S',
                                    'E',
                                    'O',
                                    'R',
                                    '1')) --CAPRI_CHANGE
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              470);
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
       'COD_ARREAR_TYPE',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'COD_ARREAR_TYPE should not be null or other than C,A,L,P,M,I,N,T,U,F,D,S,E,O,R,1.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              494);
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
       'COD_ARREAR_TYPE',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'COD_ARREAR_TYPE should not be null or other than C,A,L,P,M,I,N,T,U,F,D,S,E,O,R,1.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              494);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE (cod_arrear_type IS NULL OR
               cod_arrear_type NOT IN ('C',
                                        'A',
                                        'L',
                                        'P',
                                        'M',
                                        'I',
                                        'N',
                                        'T',
                                        'U',
                                        'F',
                                        'D',
                                        'S',
                                        'E',
                                        'O',
                                        'R',
                                        '1'))
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                516);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE (cod_arrear_type IS NULL OR
               cod_arrear_type NOT IN ('C',
                                        'A',
                                        'L',
                                        'P',
                                        'M',
                                        'I',
                                        'N',
                                        'T',
                                        'U',
                                        'F',
                                        'D',
                                        'S',
                                        'E',
                                        'O',
                                        'R',
                                        '1'))
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                516);
    END;
  END IF;

  /*COD_RULE_ID: consis 25009*/ --doubt

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25010;
  /*CTR_INSTAL_NO should not be null: consis 25010*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE ctr_instal_no IS NULL
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE ctr_instal_no IS NULL
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              532);
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
       'CTR_INSTAL_NO',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'CTR_INSTAL_NO should not be null');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              555);
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
       'CTR_INSTAL_NO',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'CTR_INSTAL_NO should not be null');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              555);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE ctr_instal_no IS NULL
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                574);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE ctr_instal_no IS NULL
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                574);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25011;
  /*CTR_SEQ_NO should not be null: consis 25011*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE ctr_seq_no IS NULL
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE ctr_seq_no IS NULL
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              589);
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
       'CTR_SEQ_NO',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'CTR_SEQ_NO should not be null');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              612);
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
       'CTR_SEQ_NO',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'CTR_SEQ_NO should not be null');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              612);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE ctr_seq_no IS NULL
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                631);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE ctr_seq_no IS NULL
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                631);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25012;
  /*DAT_ARREARS_ASSESSED should be same as DAT_ARREARS_DUE: consis 25012*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE dat_arrears_assessed <> dat_arrears_due
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE dat_arrears_assessed <> dat_arrears_due
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              646);
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
       'DAT_ACCT_NPA',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'DAT_ARREARS_ASSESSED should be same as DAT_ARREARS_DUE');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              669);
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
       'DAT_ACCT_NPA',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'DAT_ARREARS_ASSESSED should be same as DAT_ARREARS_DUE');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              669);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE dat_arrears_assessed <> dat_arrears_due
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                688);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE dat_arrears_assessed <> dat_arrears_due
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                688);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25013;
  /*DAT_ARREARS_DUE must be greater than loan account open date: consis 25013 */
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table a, co_ln_acct_dtls b
     WHERE a.cod_acct_no = b.cod_acct_no
       AND a.dat_arrears_due < b.dat_acct_open
       AND b.flg_mnt_status = 'A'
       and a.migration_source = 'CBS';
    --DAT_ARREARS_ASSESSED <> DAT_ARREARS_DUE;

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table a, co_ln_acct_dtls b
     WHERE a.cod_acct_no = b.cod_acct_no
       AND a.dat_arrears_due < b.dat_acct_open
       AND b.flg_mnt_status = 'A'
       and a.migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              706);
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
       'DAT_ARREARS_DUE',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'DAT_ARREARS_DUE must be greater than loan account open date');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              729);
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
       'DAT_ARREARS_DUE',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'DAT_ARREARS_DUE must be greater than loan account open date');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              729);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
          FROM co_ln_arrears_table a, co_ln_acct_dtls b
         WHERE a.cod_acct_no = b.cod_acct_no
           AND a.dat_arrears_due < b.dat_acct_open
           AND b.flg_mnt_status = 'A'
           and a.migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                750);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
          FROM co_ln_arrears_table a, co_ln_acct_dtls b
         WHERE a.cod_acct_no = b.cod_acct_no
           AND a.dat_arrears_due < b.dat_acct_open
           AND b.flg_mnt_status = 'A'
           and a.migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                750);
    END;
  END IF;

  /*DAT_LAST_PAYMENT is not equals to 1-jan-1950 and not equals to last payment date for loan: consis 25014*/
  --    BEGIN
  --        SELECT /*+ PARALLEL(4) */
  --            COUNT(1)
  --        INTO var_l_count
  --        FROM
  --            co_ln_arrears_table a,
  --            co_ln_acct_dtls     b
  --        WHERE
  --                a.cod_acct_no = b.cod_acct_no
  --            AND a.dat_last_payment <> TO_DATE('01-jan-1950')
  --            AND a.dat_last_payment <> b.dat_last_payment
  --            AND b.flg_mnt_status ='A';
  --
  --    EXCEPTION
  --        WHEN OTHERS THEN
  --            cbsfchost.ora_raiserror(sqlcode, 'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' || sqlerrm, 771);
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
  --            25014,
  --            'LN',
  --            'LN_ARREARS_TABLE',
  --            'DAT_LAST_PAYMENT',
  --            'ap_cons_ln_arrears_table',
  --            var_l_count,
  --            'DAT_LAST_PAYMENT is not equals to 1-jan-1950 and not equals to last payment date for loan'
  --        );
  --
  --    EXCEPTION
  --        WHEN OTHERS THEN
  --            cbsfchost.ora_raiserror(sqlcode, 'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' || sqlerrm, 794);
  --    END;
  --
  --    IF ( var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl ='Y') THEN
  --        BEGIN
  --            INSERT INTO co_ln_consis_acct (
  --                cod_consis_no,
  --                cod_acct_no
  --            )
  --                SELECT /*+ PARALLEL(4) */
  --                    25014,
  --                    a.cod_acct_no
  --                FROM
  --                    co_ln_arrears_table a,
  --                    co_ln_acct_dtls     b
  --                WHERE
  --                        a.cod_acct_no = b.cod_acct_no
  --                    AND a.dat_last_payment <> TO_DATE('01-jan-1950')
  --                    AND a.dat_last_payment <> b.dat_last_payment
  --                    AND b.flg_mnt_status ='A';
  --
  --        EXCEPTION
  --            WHEN OTHERS THEN
  --                cbsfchost.ora_raiserror(sqlcode, 'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' || sqlerrm, 816);
  --        END;
  --    END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25015;
  /*REF_BILLNO_DATE should be same as DAT_ARREARS_DUE: consis 25015 */

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE ref_billno_date <> dat_arrears_due
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE ref_billno_date <> dat_arrears_due
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              832);
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
       'REF_BILLNO_DATE',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'REF_BILLNO_DATE should be same as DAT_ARREARS_DUE');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              855);
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
       'REF_BILLNO_DATE',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'REF_BILLNO_DATE should be same as DAT_ARREARS_DUE');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              855);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE ref_billno_date <> dat_arrears_due
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                874);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE ref_billno_date <> dat_arrears_due
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                874);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25016;
  /*DAT_IOA_GRACE_EXPIRY should be same as DAT_ARREARS_DUE : consis 25016*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table a
     WHERE dat_ioa_grace_expiry <>
           (dat_arrears_due +
           (SELECT nvl(ctr_max_grace, 0)
               FROM cbsfchost.ln_prod_mast p
              WHERE p.flg_mnt_status = 'A'
                AND cod_prod = (SELECT cod_prod
                                  FROM co_ln_acct_dtls d
                                 WHERE d.cod_acct_no = a.cod_acct_no
                                   AND d.flg_mnt_status = 'A')))
       and a.migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table a
     WHERE dat_ioa_grace_expiry <>
           (dat_arrears_due +
           (SELECT nvl(ctr_max_grace, 0)
               FROM cbsfchost.ln_prod_mast p
              WHERE p.flg_mnt_status = 'A'
                AND cod_prod = (SELECT cod_prod
                                  FROM co_ln_acct_dtls d
                                 WHERE d.cod_acct_no = a.cod_acct_no
                                   AND d.flg_mnt_status = 'A')))
       and a.migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              909);
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
       'INFO', --FA : 05-May-2023 Run : Marked as INFO
       var_l_table_name,
       'DAT_IOA_GRACE_EXPIRY',
       'ap_cons_LN_ARREARS_TABLE',
       var_l_count,
       'DAT_IOA_GRACE_EXPIRY should be same as (DAT_ARREARS_DUE + Maximum Grace Days on product master of loan account).');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              933);
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
       'MFI_INFO', --FA : 05-May-2023 Run : Marked as INFO
       var_l_table_name,
       'DAT_IOA_GRACE_EXPIRY',
       'ap_cons_LN_ARREARS_TABLE',
       var_l_count1,
       'DAT_IOA_GRACE_EXPIRY should be same as (DAT_ARREARS_DUE + Maximum Grace Days on product master of loan account).');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              933);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table a
         WHERE dat_ioa_grace_expiry <>
               (dat_arrears_due +
               (SELECT nvl(ctr_max_grace, 0)
                   FROM cbsfchost.ln_prod_mast p
                  WHERE p.flg_mnt_status = 'A'
                    AND cod_prod =
                        (SELECT cod_prod
                           FROM co_ln_acct_dtls d
                          WHERE d.cod_acct_no = a.cod_acct_no
                            AND d.flg_mnt_status = 'A')))
           and a.migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                968);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table a
         WHERE dat_ioa_grace_expiry <>
               (dat_arrears_due +
               (SELECT nvl(ctr_max_grace, 0)
                   FROM cbsfchost.ln_prod_mast p
                  WHERE p.flg_mnt_status = 'A'
                    AND cod_prod =
                        (SELECT cod_prod
                           FROM co_ln_acct_dtls d
                          WHERE d.cod_acct_no = a.cod_acct_no
                            AND d.flg_mnt_status = 'A')))
           and a.migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                968);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;
  /*COD_CCY SHOULD BE SAME AS INR : consis 25017*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE cod_ccy NOT IN (SELECT cod_ccy
                             FROM cbsfchost.ba_ccy_code
                            WHERE nam_ccy_short = 'INR'
                              AND flg_mnt_status = 'A')
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE cod_ccy NOT IN (SELECT cod_ccy
                             FROM cbsfchost.ba_ccy_code
                            WHERE nam_ccy_short = 'INR'
                              AND flg_mnt_status = 'A')
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              990);
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
       'COD_CCY',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'COD_CHARGE_CCY should be as INR');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1014);
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
       'COD_CCY',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'COD_CHARGE_CCY should be as INR');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1014);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE cod_ccy NOT IN (SELECT cod_ccy
                                 FROM cbsfchost.ba_ccy_code
                                WHERE nam_ccy_short = 'INR'
                                  AND flg_mnt_status = 'A')
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1039);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE cod_ccy NOT IN (SELECT cod_ccy
                                 FROM cbsfchost.ba_ccy_code
                                WHERE nam_ccy_short = 'INR'
                                  AND flg_mnt_status = 'A')
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1039);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25018;
  /*DAT_ACCT_NPA should not be null: consis 25018*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table a
     WHERE dat_acct_npa IS NULL
       AND a.cod_acct_no = (SELECT cod_acct_no
                              FROM co_ln_acct_dtls d
                             WHERE d.cod_acct_no = a.cod_acct_no
                               AND d.flg_mnt_status = 'A'
                               AND d.flg_accr_status = 'S')
       and a.migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table a
     WHERE dat_acct_npa IS NULL
       AND a.cod_acct_no = (SELECT cod_acct_no
                              FROM co_ln_acct_dtls d
                             WHERE d.cod_acct_no = a.cod_acct_no
                               AND d.flg_mnt_status = 'A'
                               AND d.flg_accr_status = 'S')
       and a.migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              1055);
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
       'DAT_ACCT_NPA',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'DAT_ACCT_NPA should not be null for Suspended loan accounts.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1077);
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
       'DAT_ACCT_NPA',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'DAT_ACCT_NPA should not be null for Suspended loan accounts.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1077);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table a
         WHERE dat_acct_npa IS NULL
           AND a.cod_acct_no =
               (SELECT cod_acct_no
                  FROM co_ln_acct_dtls d
                 WHERE d.cod_acct_no = a.cod_acct_no
                   AND d.flg_mnt_status = 'A'
                   AND d.flg_accr_status = 'S')
           and a.migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1097);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table a
         WHERE dat_acct_npa IS NULL
           AND a.cod_acct_no =
               (SELECT cod_acct_no
                  FROM co_ln_acct_dtls d
                 WHERE d.cod_acct_no = a.cod_acct_no
                   AND d.flg_mnt_status = 'A'
                   AND d.flg_accr_status = 'S')
           and a.migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1097);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25019;
  /*COD_ARREAR_CHARGE should be from cbsfchost.ba_sc_code table consis 25019*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1), COUNT(DISTINCT a.cod_acct_no)
      INTO var_l_count, var_l_dist_count
      FROM co_ln_arrears_table a, co_ln_acct_dtls b
     WHERE cod_arrear_type IN ('F', 'D')
       and a.cod_acct_no = b.cod_acct_no
       and b.cod_acct_stat not in (1, 11)
       AND cod_arrear_charge NOT IN
           (SELECT cod_sc
              FROM cbsfchost.ba_sc_code
             WHERE flg_mnt_status = 'A')
       AND dat_arrears_due >= var_l_cutoff_date --FA : 15-Apr-2024 Run : Added to consider recent cases
       and a.migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1), COUNT(DISTINCT a.cod_acct_no)
      INTO var_l_count1, var_l_dist_count
      FROM co_ln_arrears_table a, co_ln_acct_dtls b
     WHERE cod_arrear_type IN ('F', 'D')
       and a.cod_acct_no = b.cod_acct_no
       and b.cod_acct_stat not in (1, 11)
       AND cod_arrear_charge NOT IN
           (SELECT cod_sc
              FROM cbsfchost.ba_sc_code
             WHERE flg_mnt_status = 'A')
       AND dat_arrears_due >= var_l_cutoff_date --FA : 15-Apr-2024 Run : Added to consider recent cases
       and a.migration_source = 'BRNET';

    /*esaf_changes
           (SELECT cod_sc
              FROM cbsfchost.cbsfchost.ba_sc_code
             WHERE flg_mnt_status = 'A')
       and a.cod_arrear_charge <> 0; --capri_change
    */

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              1120);
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
       'COD_ARREAR_CHARGE',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'COD_ARREAR_CHARGE should be from ba_Sc_code in case of Fee type arrears in case of F type arrears.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1144);
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
       'COD_ARREAR_CHARGE',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'COD_ARREAR_CHARGE should be from ba_Sc_code in case of Fee type arrears in case of F type arrears.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1144);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
        DISTINCT var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table a
         WHERE cod_arrear_type IN ('F', 'D')
           AND cod_arrear_charge NOT IN
               (SELECT cod_sc
                  FROM cbsfchost.ba_sc_code
                 WHERE flg_mnt_status = 'A')
           AND dat_arrears_due >= var_l_cutoff_date --FA : 15-Apr-2024 Run : Added to consider recent cases
           AND exists (select 1
                  from co_ln_acct_dtls b
                 where a.cod_acct_no = b.cod_acct_no
                   and b.cod_acct_stat not in (1, 11))
           and a.migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1171);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
        DISTINCT var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table a
         WHERE cod_arrear_type IN ('F', 'D')
           AND cod_arrear_charge NOT IN
               (SELECT cod_sc
                  FROM cbsfchost.ba_sc_code
                 WHERE flg_mnt_status = 'A')
           AND dat_arrears_due >= var_l_cutoff_date --FA : 15-Apr-2024 Run : Added to consider recent cases
           AND exists (select 1
                  from co_ln_acct_dtls b
                 where a.cod_acct_no = b.cod_acct_no
                   and b.cod_acct_stat not in (1, 11))
           and a.migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1171);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25020;

  /*AMT_ARREARS_DUE should not be null or less than zero: consis 25020*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE amt_arrears_due IS NULL
        OR amt_arrears_due < 0
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE amt_arrears_due IS NULL
        OR amt_arrears_due < 0
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              1229);
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
       'amt_arrears_due',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'AMT_ARREARS_DUE should not be null or less than zero');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1253);
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
       'amt_arrears_due',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'AMT_ARREARS_DUE should not be null or less than zero');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1253);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE amt_arrears_due IS NULL
            OR amt_arrears_due < 0
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1273);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE amt_arrears_due IS NULL
            OR amt_arrears_due < 0
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1273);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25021;
  /*CTR_INSTAL_NO should be non zero for I/C type arrears. For other type of arrears, it should be 0: consis 25021*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table a,
           (select /*+ PARALLEL(4) */
             cod_acct_no, min(dat_stage_start) dat_stage_start
              from co_ln_acct_schedule
             where nam_stage = 'EMI'
             group by cod_acct_no) sched
     WHERE ctr_instal_no <= 0
       AND cod_arrear_type IN ('C')
       AND a.cod_acct_no = sched.cod_acct_no
       AND dat_arrears_due > sched.dat_stage_start
       and a.migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table a,
           (select /*+ PARALLEL(4) */
             cod_acct_no, min(dat_stage_start) dat_stage_start
              from co_ln_acct_schedule
             where nam_stage = 'EMI'
             group by cod_acct_no) sched
     WHERE ctr_instal_no <= 0
       AND cod_arrear_type IN ('C')
       AND a.cod_acct_no = sched.cod_acct_no
       AND dat_arrears_due > sched.dat_stage_start
       and a.migration_source = 'BRNET';
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              1290);
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
       'CTR_INSTAL_NO',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'CTR_INSTAL_NO should be non zero for I/C type arrears. For other type of arrears, it should be 0.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1314);
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
       'CTR_INSTAL_NO',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'CTR_INSTAL_NO should be non zero for I/C type arrears. For other type of arrears, it should be 0.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1314);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE (ctr_instal_no <= 0 OR ctr_instal_no IS NULL)
           AND cod_arrear_type IN ('I', 'C')
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1335);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE (ctr_instal_no <= 0 OR ctr_instal_no IS NULL)
           AND cod_arrear_type IN ('I', 'C')
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1335);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25022;
  /*CTR_INSTAL_NO is non zero/NULL for rest of the arrears type (excluding I/C type arrears).: consis 25022*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE (ctr_instal_no > 0 OR ctr_instal_no IS NULL)
       AND cod_arrear_type NOT IN ('I', 'C')
       and migration_source = 'CBS';
    --AND cod_arrear_type NOT IN ('I', 'C', 'N'); --CAPRI_changes

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE (ctr_instal_no > 0 OR ctr_instal_no IS NULL)
       AND cod_arrear_type NOT IN ('I', 'C')
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              1352);
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
       'CTR_INSTAL_NO',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'CTR_INSTAL_NO is non zero/NULL for rest of the arrears type (excluding I/C type arrears).');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1376);
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
       'CTR_INSTAL_NO',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'CTR_INSTAL_NO is non zero/NULL for rest of the arrears type (excluding I/C type arrears).');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1376);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE (ctr_instal_no > 0 OR ctr_instal_no IS NULL)
           AND cod_arrear_type NOT IN ('I', 'C')
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1397);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE (ctr_instal_no > 0 OR ctr_instal_no IS NULL)
           AND cod_arrear_type NOT IN ('I', 'C')
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1397);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25023;
  /*FLG_ACCRUAL_BASIS should have values A (Accrual basis)/C(Cash basis).: consis 25023*/
  /*
          Indicates whether its CASH Basis or ACCRUAL Basis
  A-accrual basis
  C-cash basis

          */
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE flg_accrual_basis NOT IN ('A', 'C')
        OR flg_accrual_basis IS NULL
       and migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE flg_accrual_basis NOT IN ('A', 'C')
        OR flg_accrual_basis IS NULL
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              1419);
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
       'FLG_ACCRUAL_BASIS',
       'ap_cons_ln_arrears_table',
       var_l_count,
       '*FLG_ACCRUAL_BASIS should have values A (Accrual basis)/C(Cash basis) And should not be NULL.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1443);
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
       'FLG_ACCRUAL_BASIS',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       '*FLG_ACCRUAL_BASIS should have values A (Accrual basis)/C(Cash basis) And should not be NULL.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1443);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE flg_accrual_basis NOT IN ('A', 'C')
            OR flg_accrual_basis IS NULL
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1463);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE flg_accrual_basis NOT IN ('A', 'C')
            OR flg_accrual_basis IS NULL
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1463);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25024;
  var_l_log_in_dtl_ln_arrears_tbl:='Y';
  /*DAT_ACCT_NPA should not be null for Normal accrual status and should be equal to DAT_ARREARS_ASSESSED: consis 25024*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table a
     WHERE (dat_acct_npa IS NULL OR dat_arrears_assessed <> dat_acct_npa)
       AND a.cod_acct_no = (SELECT cod_acct_no
                              FROM co_ln_acct_dtls d
                             WHERE d.cod_acct_no = a.cod_acct_no
                               AND d.flg_mnt_status = 'A'
                               AND d.flg_accr_status = 'N')
       and a.migration_source = 'CBS';

    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table a
     WHERE (dat_acct_npa IS NULL OR dat_arrears_assessed <> dat_acct_npa)
       AND a.cod_acct_no = (SELECT cod_acct_no
                              FROM co_ln_acct_dtls d
                             WHERE d.cod_acct_no = a.cod_acct_no
                               AND d.flg_mnt_status = 'A'
                               AND d.flg_accr_status = 'N')
       and a.migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              1489);
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
       'DAT_ACCT_NPA',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'DAT_ACCT_NPA should not be null for Normal accrual status and should be equal to DAT_ARREARS_ASSESSED.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1513);
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
       'DAT_ACCT_NPA',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'DAT_ACCT_NPA should not be null for Normal accrual status and should be equal to DAT_ARREARS_ASSESSED.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1513);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table a
         WHERE (dat_acct_npa IS NULL OR
               dat_arrears_assessed <> dat_acct_npa)
           AND a.cod_acct_no =
               (SELECT cod_acct_no
                  FROM co_ln_acct_dtls d
                 WHERE d.cod_acct_no = a.cod_acct_no
                   AND d.flg_mnt_status = 'A'
                   AND d.flg_accr_status = 'N')
           and a.migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1543);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table a
         WHERE (dat_acct_npa IS NULL OR
               dat_arrears_assessed <> dat_acct_npa)
           AND a.cod_acct_no =
               (SELECT cod_acct_no
                  FROM co_ln_acct_dtls d
                 WHERE d.cod_acct_no = a.cod_acct_no
                   AND d.flg_mnt_status = 'A'
                   AND d.flg_accr_status = 'N')
           and a.migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1543);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 25025;
  /* If the account is in suspended status then it's all arrear types should in suspended arrear types and not in normal arrear type */
  BEGIN
    SELECT /*+Parallel(4)*/
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table
     WHERE cod_acct_no IN (SELECT cod_acct_no
                             FROM co_ln_acct_dtls
                            WHERE flg_mnt_status = 'A'
                              AND flg_accr_status = 'S'
                           --and cod_Acct_stat not in (1, 5, 11) -- capri 12-oct-2023
                           )
       AND upper(cod_arrear_type) IN ('A', 'P', 'I', 'T', 'F', 'S', 'O')
       and migration_source = 'CBS';

    SELECT /*+Parallel(4)*/
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table
     WHERE cod_acct_no IN (SELECT cod_acct_no
                             FROM co_ln_acct_dtls
                            WHERE flg_mnt_status = 'A'
                              AND flg_accr_status = 'S'
                           --and cod_Acct_stat not in (1, 5, 11) -- capri 12-oct-2023
                           )
       AND upper(cod_arrear_type) IN ('A', 'P', 'I', 'T', 'F', 'S', 'O')
       and migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              1636);
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
       'COD_ARREAR_TYPE',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'If the account is in suspended status then it''s all arrear types should in suspended arrear types and not in normal arrear type.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1661);
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
       'COD_ARREAR_TYPE',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'If the account is in suspended status then it''s all arrear types should in suspended arrear types and not in normal arrear type.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1661);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE cod_acct_no IN (SELECT cod_acct_no
                                 FROM co_ln_acct_dtls
                                WHERE flg_mnt_status = 'A'
                                  AND flg_accr_status = 'S')
           AND upper(cod_arrear_type) IN
               ('A', 'P', 'I', 'T', 'F', 'S', 'O')
           and migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1692);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
         var_l_consis_no, cod_acct_no, MIGRATION_SOURCE
          FROM co_ln_arrears_table
         WHERE cod_acct_no IN (SELECT cod_acct_no
                                 FROM co_ln_acct_dtls
                                WHERE flg_mnt_status = 'A'
                                  AND flg_accr_status = 'S')
           AND upper(cod_arrear_type) IN
               ('A', 'P', 'I', 'T', 'F', 'S', 'O')
           and migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1692);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;
  var_l_log_in_dtl_ln_arrears_tbl:='N'; --Taking long time hence removed
  var_l_consis_no := 25026;
  /* If the account is in normal status then it's all arrear types should in suspended arrear types and not in suspended arrear type */
  BEGIN
    SELECT /*+Parallel(4)*/
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table a, co_ln_acct_dtls b
     WHERE a.cod_acct_no = b.cod_accT_no
       AND b.cod_acct_stat <> 1
       AND flg_accr_status = 'N'
          /*AND a.cod_acct_no IN (SELECT cod_acct_no
            FROM co_ln_acct_dtls
           WHERE flg_mnt_status = 'A'
             AND flg_accr_status = 'N'
             and cod_acct_stat not in (1, 5, 11) --capri_change 29sep23
          )*/
       AND upper(cod_arrear_type) IN ('L', 'M', 'N', 'U', 'D', 'E')
       and a.migration_source = 'CBS';

    SELECT /*+Parallel(4)*/
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table a, co_ln_acct_dtls b
     WHERE a.cod_acct_no = b.cod_accT_no
       AND b.cod_acct_stat <> 1
       AND flg_accr_status = 'N'
          /*AND a.cod_acct_no IN (SELECT cod_acct_no
            FROM co_ln_acct_dtls
           WHERE flg_mnt_status = 'A'
             AND flg_accr_status = 'N'
             and cod_acct_stat not in (1, 5, 11) --capri_change 29sep23
          )*/
       AND upper(cod_arrear_type) IN ('L', 'M', 'N', 'U', 'D', 'E')
       and a.migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              1718);
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
       'COD_ARREAR_TYPE',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'If the account is in Normal status then it''s all arrear types should in normal arrear types and not in suspended arrear type.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1743);
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
       'COD_ARREAR_TYPE',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'If the account is in Normal status then it''s all arrear types should in normal arrear types and not in suspended arrear type.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1743);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
        DISTINCT var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
          FROM co_ln_arrears_table a, co_ln_acct_dtls b
         WHERE a.cod_acct_no = b.cod_accT_no
           AND b.cod_acct_stat <> 1
           AND flg_accr_status = 'N'
           AND upper(cod_arrear_type) IN ('L', 'M', 'N', 'U', 'D', 'E')
           and a.migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1775);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
        DISTINCT var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
          FROM co_ln_arrears_table a, co_ln_acct_dtls b
         WHERE a.cod_acct_no = b.cod_accT_no
           AND b.cod_acct_stat <> 1
           AND flg_accr_status = 'N'
           AND upper(cod_arrear_type) IN ('L', 'M', 'N', 'U', 'D', 'E')
           and a.migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1775);
    END;
  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  /*capri_change*/
  /* cod_rule_id present in ln_Arrears_Table but not present in ln_prod_int_attr.cod_int_rule */
  var_l_consis_no := 25027;
  BEGIN
    SELECT /*+parallel(4)*/
     COUNT(1)
      INTO var_l_count
      FROM co_ln_arrears_table a
     WHERE dat_arrears_due IS NULL
       and a.migration_source = 'CBS';

    SELECT /*+parallel(4)*/
     COUNT(1)
      INTO var_l_count1
      FROM co_ln_arrears_table a
     WHERE dat_arrears_due IS NULL
       and a.migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              1718);
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
       'DAT_ARREARS_DUE',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'DAT_ARREARS_DUE TO BE NULL');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1743);
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
       'DAT_ARREARS_DUE',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'DAT_ARREARS_DUE TO BE NULL');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1743);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
        DISTINCT var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
          FROM co_ln_arrears_table a
         WHERE dat_arrears_due IS NULL
           and a.migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1776);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
        DISTINCT var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
          FROM co_ln_arrears_table a
         WHERE dat_arrears_due IS NULL
           and a.migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1776);
    END;
  END IF;

  commit;
   ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
                       
                       
--------------------------------------------------------
 var_l_consis_no := 25028;
  BEGIN
    SELECT /*+parallel(4)*/
     COUNT(1)
      INTO var_l_count
      FROM CO_LN_ARREARS_TABLE A
     WHERE a.dat_last_payment < dat_arrears_due
        AND a.dat_last_payment <> TO_DATE('01011950', 'DDMMYYYY')
       and a.migration_source = 'CBS';

    SELECT /*+parallel(4)*/
     COUNT(1)
      INTO var_l_count1
      FROM CO_LN_ARREARS_TABLE A
     WHERE a.dat_last_payment < dat_arrears_due
        AND a.dat_last_payment <> TO_DATE('01011950', 'DDMMYYYY')
       and a.migration_source = 'BRNET';

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table Select From co_ln_arrears_table Failed.' ||
                              sqlerrm,
                              1718);
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
       'DAT_LAST_PAYMENT',
       'ap_cons_ln_arrears_table',
       var_l_count,
       'DAT_LAST_PAYMENT cannot be less than DAT_ARREARS_DUE');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1743);
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
       'DAT_LAST_PAYMENT',
       'ap_cons_ln_arrears_table',
       var_l_count1,
       'DAT_LAST_PAYMENT cannot be less than DAT_ARREARS_DUE');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1743);
  END;

  IF (var_l_count > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
        DISTINCT var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
         FROM CO_LN_ARREARS_TABLE A
     WHERE a.dat_last_payment < dat_arrears_due
        AND a.dat_last_payment <> TO_DATE('01011950', 'DDMMYYYY')
           and a.migration_source = 'CBS';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1776);
    END;
  END IF;

  IF (var_l_count1 > 0 AND var_l_log_in_dtl_ln_arrears_tbl = 'Y') THEN
    BEGIN
      INSERT INTO co_ln_consis_acct
        (cod_consis_no, cod_acct_no, MIGRATION_SOURCE)
        SELECT /*+ PARALLEL(4) */
        DISTINCT var_l_consis_no, a.cod_acct_no, a.MIGRATION_SOURCE
          FROM CO_LN_ARREARS_TABLE A
     WHERE a.dat_last_payment < dat_arrears_due
        AND a.dat_last_payment <> TO_DATE('01011950', 'DDMMYYYY')
           and a.migration_source = 'BRNET';

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In ap_cons_ln_arrears_table INSERT INTO co_ln_consis_acct Failed.' ||
                                sqlerrm,
                                1776);
    END;
  END IF;

  commit;
   ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis
                       
  ap_bb_mig_log_string('99999 #' || var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --Ending of function

  RETURN 0;
END ap_cons_ln_arrears_table;
/
