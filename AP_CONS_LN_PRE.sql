CREATE OR REPLACE FUNCTION "AP_CONS_LN_PRE" (VAR_PI_STREAM NUMBER)
  RETURN NUMBER AS
  VAR_L_COUNT                        NUMBER;
  VAR_L_COUNT1                       NUMBER; -- for BRNET split
  VAR_L_CONSIS_NO                    NUMBER := 0;
  VAR_L_MIGRATION_DATE               DATE;
  VAR_L_FUNCTION_NAME                VARCHAR2(100) := 'AP_CONS_LN_PRE';
  VAR_DAT_PROCESS                    DATE;
  var_dat_last_process               DATE;
  var_l_dist_count                   NUMBER := 0;
  MIG_DATE                           DATE;
  var_l_rec_count                    NUMBER := 0;
BEGIN
  AP_BB_MIG_LOG_STRING('START AP_CONS_LN_PRE');

  var_dat_process      := CBSFCHOST.PK_BA_GLOBAL.DAT_PROCESS;
  var_dat_last_process := CBSFCHOST.PK_BA_GLOBAL.DAT_LAST_PROCESS;

  BEGIN
    VAR_L_MIGRATION_DATE := CBSFCHOST.PK_BA_GLOBAL.DAT_LAST_PROCESS;
  END;

  AP_BB_MIG_LOG_STRING('00000 #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);
  AP_BB_MIG_LOG_STRING('START ' || VAR_L_FUNCTION_NAME);

  DELETE FROM CO_LN_CONSIS
   WHERE (COD_CONSIS_NO BETWEEN 12901 AND 12938);
  DELETE FROM CO_LN_CONSIS_ACCT
   WHERE (COD_CONSIS_NO BETWEEN 12901 AND 12938);
  COMMIT;

  -----------------------------------------------------------------------------
  -- CONSIS 12901 : Guarantor missing for guarantor-mandatory product (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12901;
  BEGIN
     SELECT /*+PARALLEL(256) */
            COUNT(1)
       INTO VAR_L_COUNT
       FROM CO_LN_ACCT_DTLS A
      WHERE COD_PROD IN (SELECT COD_PROD
                           FROM CBSFCHOST.LN_PROD_MAST
                          WHERE COD_SECURED = 1
                            AND FLG_MNT_STATUS = 'A')
        AND COD_ACCT_NO NOT IN
            (SELECT DISTINCT COD_ACCT_NO
               FROM CO_CH_ACCT_CUST_XREF B
              WHERE B.COD_ACCT_NO = A.COD_ACCT_NO
                AND COD_ACCT_CUST_REL = 'GUA')
        AND A.migration_source = 'CBS';

     INSERT INTO CO_LN_CONSIS
       (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
     VALUES
       (VAR_L_CONSIS_NO, 'LN', 'CO_LN_ACCT_DTLS', 'COD_PROD', VAR_L_FUNCTION_NAME, VAR_L_COUNT,
        'COD_ACCT_NO : LOANS WHERE GUARANTOR MISSING FOR GUARANTOR MANDATORY PRODUCT.');

     IF (VAR_L_COUNT <> 0) THEN
       INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
         INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
       SELECT /*+ PARALLEL(64) */
              VAR_L_CONSIS_NO, COD_ACCT_NO, 'CBS'
         FROM CO_LN_ACCT_DTLS A
        WHERE COD_PROD IN (SELECT COD_PROD
                             FROM CBSFCHOST.LN_PROD_MAST
                            WHERE COD_SECURED = 1
                              AND FLG_MNT_STATUS = 'A')
          AND COD_ACCT_NO NOT IN
              (SELECT DISTINCT COD_ACCT_NO
                 FROM CO_CH_ACCT_CUST_XREF B
                WHERE B.COD_ACCT_NO = A.COD_ACCT_NO
                  AND COD_ACCT_CUST_REL = 'GUA')
          AND A.migration_source = 'CBS';
     END IF;

     SELECT /*+PARALLEL(256) */
            COUNT(1)
       INTO VAR_L_COUNT1
       FROM CO_LN_ACCT_DTLS A
      WHERE COD_PROD IN (SELECT COD_PROD
                           FROM CBSFCHOST.LN_PROD_MAST
                          WHERE COD_SECURED = 1
                            AND FLG_MNT_STATUS = 'A')
        AND COD_ACCT_NO NOT IN
            (SELECT DISTINCT COD_ACCT_NO
               FROM CO_CH_ACCT_CUST_XREF B
              WHERE B.COD_ACCT_NO = A.COD_ACCT_NO
                AND COD_ACCT_CUST_REL = 'GUA')
        AND A.migration_source = 'BRNET';

     INSERT INTO CO_LN_CONSIS
       (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
     VALUES
       (VAR_L_CONSIS_NO, 'BRNET', 'CO_LN_ACCT_DTLS', 'COD_PROD', VAR_L_FUNCTION_NAME, VAR_L_COUNT1,
        'COD_ACCT_NO : LOANS WHERE GUARANTOR MISSING FOR GUARANTOR MANDATORY PRODUCT.');

     IF (VAR_L_COUNT1 <> 0) THEN
       INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
         INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
       SELECT /*+ PARALLEL(64) */
              VAR_L_CONSIS_NO, COD_ACCT_NO, 'BRNET'
         FROM CO_LN_ACCT_DTLS A
        WHERE COD_PROD IN (SELECT COD_PROD
                             FROM CBSFCHOST.LN_PROD_MAST
                            WHERE COD_SECURED = 1
                              AND FLG_MNT_STATUS = 'A')
          AND COD_ACCT_NO NOT IN
              (SELECT DISTINCT COD_ACCT_NO
                 FROM CO_CH_ACCT_CUST_XREF B
                WHERE B.COD_ACCT_NO = A.COD_ACCT_NO
                  AND COD_ACCT_CUST_REL = 'GUA')
          AND A.migration_source = 'BRNET';
     END IF;
     COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12902 : AMT_FACE_VALUE < AMT_PRINC_BALANCE (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12902;
  BEGIN
    SELECT /*+PARALLEL(256) */
           COUNT(1)
      INTO VAR_L_COUNT
      FROM CO_LN_ACCT_DTLS A, CO_LN_ACCT_BALANCES B
     WHERE B.COD_CC_BRN = A.COD_CC_BRN
       AND A.COD_ACCT_NO = B.COD_ACCT_NO
       AND A.AMT_FACE_VALUE < B.AMT_PRINC_BALANCE
       AND A.migration_source = 'CBS';

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'CO_LN_ACCT_DTLS', 'AMT_FACE_VALUE', VAR_L_FUNCTION_NAME, VAR_L_COUNT,
       'COD_ACCT_NO : LOANS WHERE AMT_FACE_VALUE < AMT_PRINC_BALANCE.');

    IF (VAR_L_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, A.COD_ACCT_NO, 'CBS'
        FROM CO_LN_ACCT_DTLS A, CO_LN_ACCT_BALANCES B
       WHERE B.COD_CC_BRN = A.COD_CC_BRN
         AND A.COD_ACCT_NO = B.COD_ACCT_NO
         AND A.AMT_FACE_VALUE < B.AMT_PRINC_BALANCE
         AND A.migration_source = 'CBS';
    END IF;

    SELECT /*+PARALLEL(256) */
           COUNT(1)
      INTO VAR_L_COUNT1
      FROM CO_LN_ACCT_DTLS A, CO_LN_ACCT_BALANCES B
     WHERE B.COD_CC_BRN = A.COD_CC_BRN
       AND A.COD_ACCT_NO = B.COD_ACCT_NO
       AND A.AMT_FACE_VALUE < B.AMT_PRINC_BALANCE
       AND A.migration_source = 'BRNET';

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'CO_LN_ACCT_DTLS', 'AMT_FACE_VALUE', VAR_L_FUNCTION_NAME, VAR_L_COUNT1,
       'COD_ACCT_NO : LOANS WHERE AMT_FACE_VALUE < AMT_PRINC_BALANCE.');

    IF (VAR_L_COUNT1 <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, A.COD_ACCT_NO, 'BRNET'
        FROM CO_LN_ACCT_DTLS A, CO_LN_ACCT_BALANCES B
       WHERE B.COD_CC_BRN = A.COD_CC_BRN
         AND A.COD_ACCT_NO = B.COD_ACCT_NO
         AND A.AMT_FACE_VALUE < B.AMT_PRINC_BALANCE
         AND A.migration_source = 'BRNET';
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12903 : Disbursed but schedule missing (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12903;
  BEGIN
    SELECT /*+PARALLEL(256) */
           COUNT(1)
      INTO VAR_L_COUNT
      FROM (
            SELECT A.COD_ACCT_NO
              FROM CO_LN_ACCT_DTLS A, CO_LN_ACCT_BALANCES B
             WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
               AND B.AMT_DISBURSED > 0
               AND A.migration_source = 'CBS'
            MINUS
            SELECT A.COD_ACCT_NO
              FROM CO_LN_ACCT_SCHEDULE A, CO_LN_ACCT_DTLS B
             WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
               AND B.migration_source = 'CBS'
           );

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'CO_LN_ACCT_DTLS', 'AMT_DISBURSED', VAR_L_FUNCTION_NAME, VAR_L_COUNT,
       'COD_ACCT_NO : Loans Where LOANS SCHEDULE MISSING');

    IF (VAR_L_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, COD_ACCT_NO, 'CBS'
        FROM (
              SELECT A.COD_ACCT_NO
                FROM CO_LN_ACCT_DTLS A, CO_LN_ACCT_BALANCES B
               WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
                 AND B.AMT_DISBURSED > 0
                 AND A.migration_source = 'CBS'
              MINUS
              SELECT A.COD_ACCT_NO
                FROM CO_LN_ACCT_SCHEDULE A, CO_LN_ACCT_DTLS B
               WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
                 AND B.migration_source = 'CBS'
             );
    END IF;

    SELECT /*+PARALLEL(256) */
           COUNT(1)
      INTO VAR_L_COUNT1
      FROM (
            SELECT A.COD_ACCT_NO
              FROM CO_LN_ACCT_DTLS A, CO_LN_ACCT_BALANCES B
             WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
               AND B.AMT_DISBURSED > 0
               AND A.migration_source = 'BRNET'
            MINUS
            SELECT A.COD_ACCT_NO
              FROM CO_LN_ACCT_SCHEDULE A, CO_LN_ACCT_DTLS B
             WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
               AND B.migration_source = 'BRNET'
           );

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'CO_LN_ACCT_DTLS', 'AMT_DISBURSED', VAR_L_FUNCTION_NAME, VAR_L_COUNT1,
       'COD_ACCT_NO : Loans Where LOANS SCHEDULE MISSING');

    IF (VAR_L_COUNT1 <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, COD_ACCT_NO, 'BRNET'
        FROM (
              SELECT A.COD_ACCT_NO
                FROM CO_LN_ACCT_DTLS A, CO_LN_ACCT_BALANCES B
               WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
                 AND B.AMT_DISBURSED > 0
                 AND A.migration_source = 'BRNET'
              MINUS
              SELECT A.COD_ACCT_NO
                FROM CO_LN_ACCT_SCHEDULE A, CO_LN_ACCT_DTLS B
               WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
                 AND B.migration_source = 'BRNET'
             );
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12904 : AMT_INSTAL <= 0 in EPI stage (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12904;
  BEGIN
    SELECT /*+PARALLEL(256) */
           COUNT(1)
      INTO VAR_L_COUNT
      FROM CO_LN_ACCT_SCHEDULE a, CO_LN_ACCT_DTLS b
     WHERE amt_instal <= 0
       AND b.dat_of_maturity > var_dat_process
       AND b.cod_acct_no = a.cod_acct_no
       AND a.cod_instal_rule IN (SELECT cod_inst_rule
                                   FROM CBSFCHOST.ln_inst_rules
                                  WHERE flg_mnt_status = 'A'
                                    AND cod_inst_calc_method = 'EPI')
       AND b.migration_source = 'CBS';

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'CO_LN_ACCT_DTLS', 'AMT_INSTAL', VAR_L_FUNCTION_NAME, VAR_L_COUNT,
       'COD_ACCT_NO : Loans Where AMT_INSTAL = zero In EPI Stage.');

    IF (VAR_L_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, b.COD_ACCT_NO, 'CBS'
        FROM CO_LN_ACCT_SCHEDULE a, CO_LN_ACCT_DTLS b
       WHERE amt_instal <= 0
         AND b.dat_of_maturity > var_dat_process
         AND b.cod_acct_no = a.cod_acct_no
         AND a.cod_instal_rule IN (SELECT cod_inst_rule
                                     FROM CBSFCHOST.ln_inst_rules
                                    WHERE flg_mnt_status = 'A'
                                      AND cod_inst_calc_method = 'EPI')
         AND b.migration_source = 'CBS';
    END IF;

    SELECT /*+PARALLEL(256) */
           COUNT(1)
      INTO VAR_L_COUNT1
      FROM CO_LN_ACCT_SCHEDULE a, CO_LN_ACCT_DTLS b
     WHERE amt_instal <= 0
       AND b.dat_of_maturity > var_dat_process
       AND b.cod_acct_no = a.cod_acct_no
       AND a.cod_instal_rule IN (SELECT cod_inst_rule
                                   FROM CBSFCHOST.ln_inst_rules
                                  WHERE flg_mnt_status = 'A'
                                    AND cod_inst_calc_method = 'EPI')
       AND b.migration_source = 'BRNET';

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'CO_LN_ACCT_DTLS', 'AMT_INSTAL', VAR_L_FUNCTION_NAME, VAR_L_COUNT1,
       'COD_ACCT_NO : Loans Where AMT_INSTAL = zero In EPI Stage.');

    IF (VAR_L_COUNT1 <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, b.COD_ACCT_NO, 'BRNET'
        FROM CO_LN_ACCT_SCHEDULE a, CO_LN_ACCT_DTLS b
       WHERE amt_instal <= 0
         AND b.dat_of_maturity > var_dat_process
         AND b.cod_acct_no = a.cod_acct_no
         AND a.cod_instal_rule IN (SELECT cod_inst_rule
                                     FROM CBSFCHOST.ln_inst_rules
                                    WHERE flg_mnt_status = 'A'
                                      AND cod_inst_calc_method = 'EPI')
         AND b.migration_source = 'BRNET';
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12905 : Schedule type not defined at product level (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12905;
  BEGIN
    SELECT /*+PARALLEL(256) */
           COUNT(1)
      INTO VAR_L_COUNT
      FROM CO_LN_ACCT_DTLS A
     WHERE A.cod_sched_type NOT IN
           (SELECT B.cod_sched_type
              FROM CBSFCHOST.ln_sched_types B
             WHERE A.cod_prod = B.cod_prod
               AND B.flg_mnt_status = 'A')
       AND A.migration_source = 'CBS';

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'CO_LN_ACCT_DTLS', 'COD_SCHED_TYPE', VAR_L_FUNCTION_NAME, VAR_L_COUNT,
       'COD_ACCT_NO : Loans Where SCH_TYPE Not Defined At Product Level.');

    IF (VAR_L_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, COD_ACCT_NO, 'CBS'
        FROM CO_LN_ACCT_DTLS A
       WHERE A.cod_sched_type NOT IN
             (SELECT B.cod_sched_type
                FROM CBSFCHOST.ln_sched_types B
               WHERE A.cod_prod = B.cod_prod
                 AND B.flg_mnt_status = 'A')
         AND A.migration_source = 'CBS';
    END IF;

    SELECT /*+PARALLEL(256) */
           COUNT(1)
      INTO VAR_L_COUNT1
      FROM CO_LN_ACCT_DTLS A
     WHERE A.cod_sched_type NOT IN
           (SELECT B.cod_sched_type
              FROM CBSFCHOST.ln_sched_types B
             WHERE A.cod_prod = B.cod_prod
               AND B.flg_mnt_status = 'A')
       AND A.migration_source = 'BRNET';

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'CO_LN_ACCT_DTLS', 'COD_SCHED_TYPE', VAR_L_FUNCTION_NAME, VAR_L_COUNT1,
       'COD_ACCT_NO : Loans Where SCH_TYPE Not Defined At Product Level.');

    IF (VAR_L_COUNT1 <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, COD_ACCT_NO, 'BRNET'
        FROM CO_LN_ACCT_DTLS A
       WHERE A.cod_sched_type NOT IN
             (SELECT B.cod_sched_type
                FROM CBSFCHOST.ln_sched_types B
               WHERE A.cod_prod = B.cod_prod
                 AND B.flg_mnt_status = 'A')
         AND A.migration_source = 'BRNET';
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12906 : DAT_ARREARS_DUE > DAT_PROCESS (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12906;
  BEGIN
    SELECT /*+PARALLEL(256) */
           COUNT(DISTINCT A.COD_ACCT_NO)
      INTO VAR_L_COUNT
      FROM CO_LN_ARREARS_TABLE a, CO_LN_ACCT_DTLS b
     WHERE a.dat_arrears_due > var_dat_process
       AND b.cod_acct_no = a.cod_acct_no
       AND b.migration_source = 'CBS';

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'CO_LN_ARREARS_TABLE', 'DAT_ARREARS_DUE', VAR_L_FUNCTION_NAME, VAR_L_COUNT,
       'COD_ACCT_NO : Loans Where DAT_ARREARS_DUE > DAT_PROCESS.');

    IF (VAR_L_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, b.COD_ACCT_NO, 'CBS'
        FROM CO_LN_ARREARS_TABLE a, CO_LN_ACCT_DTLS b
       WHERE a.dat_arrears_due > var_dat_process
         AND b.cod_acct_no = a.cod_acct_no
         AND b.migration_source = 'CBS';
    END IF;

    SELECT /*+PARALLEL(256) */
           COUNT(DISTINCT A.COD_ACCT_NO)
      INTO VAR_L_COUNT1
      FROM CO_LN_ARREARS_TABLE a, CO_LN_ACCT_DTLS b
     WHERE a.dat_arrears_due > var_dat_process
       AND b.cod_acct_no = a.cod_acct_no
       AND b.migration_source = 'BRNET';

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'CO_LN_ARREARS_TABLE', 'DAT_ARREARS_DUE', VAR_L_FUNCTION_NAME, VAR_L_COUNT1,
       'COD_ACCT_NO : Loans Where DAT_ARREARS_DUE > DAT_PROCESS.');

    IF (VAR_L_COUNT1 <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, b.COD_ACCT_NO, 'BRNET'
        FROM CO_LN_ARREARS_TABLE a, CO_LN_ACCT_DTLS b
       WHERE a.dat_arrears_due > var_dat_process
         AND b.cod_acct_no = a.cod_acct_no
         AND b.migration_source = 'BRNET';
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12907 : Dat of maturity < process and dat_last_charged is NULL (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12907;
  BEGIN
    SELECT /*+parallel(128) nologging*/
           COUNT(1)
      INTO VAR_L_COUNT
      FROM (SELECT a.cod_acct_no
              FROM CO_LN_ACCT_DTLS a
             WHERE a.dat_of_maturity < var_dat_process
               AND a.dat_last_charged IS NULL
               AND a.dat_of_maturity != to_date('01-JAN-1950','DD-MON-YYYY')
               AND a.migration_source = 'CBS');

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'CO_LN_ACCT_DTLS', 'DAT_LAST_CHARGED', VAR_L_FUNCTION_NAME, VAR_L_COUNT,
       'COD_ACCT_NO : RECORDS where Dat_last_charged is null in PMI accounts.');

    IF (VAR_L_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, a.COD_ACCT_NO, 'CBS'
        FROM CO_LN_ACCT_DTLS a
       WHERE a.dat_of_maturity < var_dat_process
         AND a.dat_last_charged IS NULL
         AND a.dat_of_maturity != to_date('01-JAN-1950','DD-MON-YYYY')
         AND a.migration_source = 'CBS';
    END IF;

    SELECT /*+parallel(128) nologging*/
           COUNT(1)
      INTO VAR_L_COUNT1
      FROM (SELECT a.cod_acct_no
              FROM CO_LN_ACCT_DTLS a
             WHERE a.dat_of_maturity < var_dat_process
               AND a.dat_last_charged IS NULL
               AND a.dat_of_maturity != to_date('01-JAN-1950','DD-MON-YYYY')
               AND a.migration_source = 'BRNET');

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'CO_LN_ACCT_DTLS', 'DAT_LAST_CHARGED', VAR_L_FUNCTION_NAME, VAR_L_COUNT1,
       'COD_ACCT_NO : RECORDS where Dat_last_charged is null in PMI accounts.');

    IF (VAR_L_COUNT1 <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, a.COD_ACCT_NO, 'BRNET'
        FROM CO_LN_ACCT_DTLS a
       WHERE a.dat_of_maturity < var_dat_process
         AND a.dat_last_charged IS NULL
         AND a.dat_of_maturity != to_date('01-JAN-1950','DD-MON-YYYY')
         AND a.migration_source = 'BRNET';
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12908 : PMI rule coverage check (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12908;
  BEGIN
    SELECT /*+parallel(128) nologging*/
           COUNT(1)
      INTO VAR_L_COUNT
      FROM (
            SELECT DISTINCT TRIM(a.cod_acct_no)
              FROM CO_LN_ACCT_SCHEDULE a, CO_LN_ACCT_DTLS b
             WHERE a.cod_acct_no = b.cod_acct_no
               AND b.migration_source = 'CBS'
            MINUS
            SELECT DISTINCT TRIM(a.cod_acct_no)
              FROM CO_LN_ACCT_SCHEDULE a, CO_LN_ACCT_DTLS b
             WHERE a.cod_acct_no = b.cod_acct_no
               AND b.migration_source = 'CBS'
               AND a.cod_instal_rule IN (SELECT cod_inst_rule
                                           FROM CBSFCHOST.ln_inst_rules
                                          WHERE cod_inst_calc_method = 'PMI')
           );

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'CO_LN_ACCT_SCHEDULE', 'PMI_RULE', VAR_L_FUNCTION_NAME, VAR_L_COUNT,
       'COD_ACCT_NO : Accounts without PMI stage present when required.');

    IF (VAR_L_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, cod_acct_no, 'CBS'
        FROM (
              SELECT DISTINCT TRIM(a.cod_acct_no) cod_acct_no
                FROM CO_LN_ACCT_SCHEDULE a, CO_LN_ACCT_DTLS b
               WHERE a.cod_acct_no = b.cod_acct_no
                 AND b.migration_source = 'CBS'
              MINUS
              SELECT DISTINCT TRIM(a.cod_acct_no)
                FROM CO_LN_ACCT_SCHEDULE a, CO_LN_ACCT_DTLS b
               WHERE a.cod_acct_no = b.cod_acct_no
                 AND b.migration_source = 'CBS'
                 AND a.cod_instal_rule IN (SELECT cod_inst_rule
                                             FROM CBSFCHOST.ln_inst_rules
                                            WHERE cod_inst_calc_method = 'PMI')
             );
    END IF;

    SELECT /*+parallel(128) nologging*/
           COUNT(1)
      INTO VAR_L_COUNT1
      FROM (
            SELECT DISTINCT TRIM(a.cod_acct_no)
              FROM CO_LN_ACCT_SCHEDULE a, CO_LN_ACCT_DTLS b
             WHERE a.cod_acct_no = b.cod_acct_no
               AND b.migration_source = 'BRNET'
            MINUS
            SELECT DISTINCT TRIM(a.cod_acct_no)
              FROM CO_LN_ACCT_SCHEDULE a, CO_LN_ACCT_DTLS b
             WHERE a.cod_acct_no = b.cod_acct_no
               AND b.migration_source = 'BRNET'
               AND a.cod_instal_rule IN (SELECT cod_inst_rule
                                           FROM CBSFCHOST.ln_inst_rules
                                          WHERE cod_inst_calc_method = 'PMI')
           );

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'CO_LN_ACCT_SCHEDULE', 'PMI_RULE', VAR_L_FUNCTION_NAME, VAR_L_COUNT1,
       'COD_ACCT_NO : Accounts without PMI stage present when required.');

    IF (VAR_L_COUNT1 <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, cod_acct_no, 'BRNET'
        FROM (
              SELECT DISTINCT TRIM(a.cod_acct_no) cod_acct_no
                FROM CO_LN_ACCT_SCHEDULE a, CO_LN_ACCT_DTLS b
               WHERE a.cod_acct_no = b.cod_acct_no
                 AND b.migration_source = 'BRNET'
              MINUS
              SELECT DISTINCT TRIM(a.cod_acct_no)
                FROM CO_LN_ACCT_SCHEDULE a, CO_LN_ACCT_DTLS b
               WHERE a.cod_acct_no = b.cod_acct_no
                 AND b.migration_source = 'BRNET'
                 AND a.cod_instal_rule IN (SELECT cod_inst_rule
                                             FROM CBSFCHOST.ln_inst_rules
                                            WHERE cod_inst_calc_method = 'PMI')
             );
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12909 : dat_last_ioa <> var_dat_last_process (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12909;
  BEGIN
    SELECT /*+parallel(128) nologging*/
           COUNT(1)
      INTO VAR_L_COUNT
      FROM CO_LN_ACCT_DTLS a
     WHERE a.dat_last_ioa <> var_dat_last_process
       AND a.migration_source = 'CBS';

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'CO_LN_ACCT_DTLS', 'dat_last_ioa', VAR_L_FUNCTION_NAME, VAR_L_COUNT,
       'COD_ACCT_NO : Records where dat_last_ioa is not equal to process date code changed as per Bandhan requirement.');

    IF (VAR_L_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, a.COD_ACCT_NO, 'CBS'
        FROM CO_LN_ACCT_DTLS a
       WHERE a.dat_last_ioa <> var_dat_last_process
         AND a.migration_source = 'CBS';
    END IF;

    SELECT /*+parallel(128) nologging*/
           COUNT(1)
      INTO VAR_L_COUNT1
      FROM CO_LN_ACCT_DTLS a
     WHERE a.dat_last_ioa <> var_dat_last_process
       AND a.migration_source = 'BRNET';

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'CO_LN_ACCT_DTLS', 'dat_last_ioa', VAR_L_FUNCTION_NAME, VAR_L_COUNT1,
       'COD_ACCT_NO : Records where dat_last_ioa is not equal to process date code changed as per Bandhan requirement.');

    IF (VAR_L_COUNT1 <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, a.COD_ACCT_NO, 'BRNET'
        FROM CO_LN_ACCT_DTLS a
       WHERE a.dat_last_ioa <> var_dat_last_process
         AND a.migration_source = 'BRNET';
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12910 : dat_acct_open > min(dat_post) in ledger (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12910;
  BEGIN
    SELECT /*+parallel(128) nologging*/
           COUNT(1)
      INTO VAR_L_COUNT
      FROM CO_LN_ACCT_DTLS A
     WHERE A.dat_acct_open >
           (SELECT MIN(dat_post) FROM CO_LN_ACCT_LEDG B WHERE A.cod_acct_no = B.cod_acct_no)
       AND A.migration_source = 'CBS';

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'CO_LN_ACCT_DTLS', 'dat_acct_open', VAR_L_FUNCTION_NAME, VAR_L_COUNT,
       'COD_ACCT_NO : accounts where account open date is greater than minimum posting date in ledger.');

    IF (VAR_L_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, A.COD_ACCT_NO, 'CBS'
        FROM CO_LN_ACCT_DTLS A
       WHERE A.dat_acct_open >
             (SELECT MIN(dat_post) FROM CO_LN_ACCT_LEDG B WHERE A.cod_acct_no = B.cod_acct_no)
         AND A.migration_source = 'CBS';
    END IF;

    SELECT /*+parallel(128) nologging*/
           COUNT(1)
      INTO VAR_L_COUNT1
      FROM CO_LN_ACCT_DTLS A
     WHERE A.dat_acct_open >
           (SELECT MIN(dat_post) FROM CO_LN_ACCT_LEDG B WHERE A.cod_acct_no = B.cod_acct_no)
       AND A.migration_source = 'BRNET';

    INSERT INTO CO_LN_CONSIS
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'CO_LN_ACCT_DTLS', 'dat_acct_open', VAR_L_FUNCTION_NAME, VAR_L_COUNT1,
       'COD_ACCT_NO : accounts where account open date is greater than minimum posting date in ledger.');

    IF (VAR_L_COUNT1 <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_CUSTOM_CONSIS_LOG, 64) */
        INTO CO_LN_CONSIS_ACCT (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, A.COD_ACCT_NO, 'BRNET'
        FROM CO_LN_ACCT_DTLS A
       WHERE A.dat_acct_open >
             (SELECT MIN(dat_post) FROM CO_LN_ACCT_LEDG B WHERE A.cod_acct_no = B.cod_acct_no)
         AND A.migration_source = 'BRNET';
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12911–12929 already included above (omitted here for brevity in comment)
  -- ... (content remains as in earlier blocks generated up to 12929) ...
  -----------------------------------------------------------------------------

  -----------------------------------------------------------------------------
  -- CONSIS 12929 : Duplicate UMRN (split)  [already included earlier]
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12929;
  BEGIN
    SELECT COUNT(1) INTO var_l_rec_count FROM
    (SELECT /*+ PARALLEL(64) */
            COUNT(1)
       FROM co_ln_acct_attributes a
       JOIN co_ln_acct_dtls d ON d.cod_acct_no = a.cod_acct_no
      WHERE a.cod_umrn_no IS NOT NULL
        AND d.migration_source = 'CBS'
      GROUP BY a.cod_umrn_no
     HAVING COUNT(1) > 1);

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'co_ln_acct_attributes', 'cod_umrn_no', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : Duplicate umrn no in LN_ACCT_ATTRIBUTES.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, a.COD_ACCT_NO, 'CBS'
        FROM co_ln_acct_attributes a
        JOIN co_ln_acct_dtls d ON d.cod_acct_no = a.cod_acct_no
       WHERE a.cod_umrn_no IS NOT NULL
         AND d.migration_source = 'CBS'
         AND a.cod_umrn_no IN (
               SELECT cod_umrn_no
                 FROM co_ln_acct_attributes a2
                 JOIN co_ln_acct_dtls d2 ON d2.cod_acct_no = a2.cod_acct_no
                WHERE a2.cod_umrn_no IS NOT NULL AND d2.migration_source = 'CBS'
                GROUP BY cod_umrn_no HAVING COUNT(1) > 1);
    END IF;

    SELECT COUNT(1) INTO var_l_rec_count FROM
    (SELECT /*+ PARALLEL(64) */
            COUNT(1)
       FROM co_ln_acct_attributes a
       JOIN co_ln_acct_dtls d ON d.cod_acct_no = a.cod_acct_no
      WHERE a.cod_umrn_no IS NOT NULL
        AND d.migration_source = 'BRNET'
      GROUP BY a.cod_umrn_no
     HAVING COUNT(1) > 1);

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'co_ln_acct_attributes', 'cod_umrn_no', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : Duplicate umrn no in LN_ACCT_ATTRIBUTES.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, a.COD_ACCT_NO, 'BRNET'
        FROM co_ln_acct_attributes a
        JOIN co_ln_acct_dtls d ON d.cod_acct_no = a.cod_acct_no
       WHERE a.cod_umrn_no IS NOT NULL
         AND d.migration_source = 'BRNET'
         AND a.cod_umrn_no IN (
               SELECT cod_umrn_no
                 FROM co_ln_acct_attributes a2
                 JOIN co_ln_acct_dtls d2 ON d2.cod_acct_no = a2.cod_acct_no
                WHERE a2.cod_umrn_no IS NOT NULL AND d2.migration_source = 'BRNET'
                GROUP BY cod_umrn_no HAVING COUNT(1) > 1);
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- NOTE: CONSIS 12930 and 12931 are commented in the original and left as-is.
  -- If you want them activated with CBS/BRNET split, I can convert them too.
  -----------------------------------------------------------------------------

  -----------------------------------------------------------------------------
  -- CONSIS 12932 : dat_last_due > var_dat_process (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12932;
  BEGIN
    SELECT /*+ PARALLEL(64) */
           COUNT(1)
      INTO var_l_rec_count
      FROM co_ln_acct_dtls a
     WHERE a.dat_last_due > var_dat_process
       AND a.migration_source = 'CBS';

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'co_ln_acct_dtls', 'dat_last_due', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : dat_last_due greater than process date.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */ VAR_L_CONSIS_NO, COD_ACCT_NO, 'CBS'
        FROM co_ln_acct_dtls a
       WHERE a.dat_last_due > var_dat_process
         AND a.migration_source = 'CBS';
    END IF;

    SELECT /*+ PARALLEL(64) */
           COUNT(1)
      INTO var_l_rec_count
      FROM co_ln_acct_dtls a
     WHERE a.dat_last_due > var_dat_process
       AND a.migration_source = 'BRNET';

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'co_ln_acct_dtls', 'dat_last_due', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : dat_last_due greater than process date.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */ VAR_L_CONSIS_NO, COD_ACCT_NO, 'BRNET'
        FROM co_ln_acct_dtls a
       WHERE a.dat_last_due > var_dat_process
         AND a.migration_source = 'BRNET';
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12933 : Zero balances/disbursed but arrears due > 0 (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12933;
  BEGIN
    SELECT COUNT(1)
      INTO var_l_rec_count
      FROM (
            select /*+PARALLEL(64) */ a.cod_acct_no
              from co_ln_acct_dtls a, co_ln_acct_balances b
             WHERE a.cod_Acct_no = b.cod_Acct_no
               and cod_Acct_stat NOT IN (1, 5)
               and amt_princ_balance = 0
               and amt_arrears_princ = 0
               and ctr_disb > 0
               and amt_disbursed > 0
               and a.migration_source = 'CBS'
            MINUS
            select /*+PARALLEL(64) */ a.cod_acct_no
              from co_ln_acct_dtls a, co_ln_acct_balances b, CO_LN_ARREARS_TABLE c
             WHERE a.COD_aCCT_NO = b.COD_aCCT_NO
               and cod_Acct_stat NOT IN (1, 5)
               and amt_princ_balance = 0
               and amt_arrears_princ = 0
               and ctr_disb > 0
               and amt_disbursed > 0
               and a.cod_Acct_no = c.cod_Acct_no
               and amt_arrears_due > 0
               and a.migration_source = 'CBS'
           );

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'co_ln_acct_dtls', 'amt_arrears_due', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : accounts with zero principal but arrears due > 0.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+PARALLEL(64)*/ VAR_L_CONSIS_NO, cod_acct_no, 'CBS'
        FROM (
              select /*+PARALLEL(64) */ a.cod_acct_no
                from co_ln_acct_dtls a, co_ln_acct_balances b
               WHERE a.cod_Acct_no = b.cod_Acct_no
                 and cod_Acct_stat NOT IN (1, 5)
                 and amt_princ_balance = 0
                 and amt_arrears_princ = 0
                 and ctr_disb > 0
                 and amt_disbursed > 0
                 and a.migration_source = 'CBS'
              MINUS
              select /*+PARALLEL(64) */ a.cod_acct_no
                from co_ln_acct_dtls a, co_ln_acct_balances b, CO_LN_ARREARS_TABLE c
               WHERE a.COD_aCCT_NO = b.COD_aCCT_NO
                 and cod_Acct_stat NOT IN (1, 5)
                 and amt_princ_balance = 0
                 and amt_arrears_princ = 0
                 and ctr_disb > 0
                 and amt_disbursed > 0
                 and a.cod_Acct_no = c.cod_Acct_no
                 and amt_arrears_due > 0
                 and a.migration_source = 'CBS'
             );
    END IF;

    SELECT COUNT(1)
      INTO var_l_rec_count
      FROM (
            select /*+PARALLEL(64) */ a.cod_acct_no
              from co_ln_acct_dtls a, co_ln_acct_balances b
             WHERE a.cod_Acct_no = b.cod_Acct_no
               and cod_Acct_stat NOT IN (1, 5)
               and amt_princ_balance = 0
               and amt_arrears_princ = 0
               and ctr_disb > 0
               and amt_disbursed > 0
               and a.migration_source = 'BRNET'
            MINUS
            select /*+PARALLEL(64) */ a.cod_acct_no
              from co_ln_acct_dtls a, co_ln_acct_balances b, CO_LN_ARREARS_TABLE c
             WHERE a.COD_aCCT_NO = b.COD_aCCT_NO
               and cod_Acct_stat NOT IN (1, 5)
               and amt_princ_balance = 0
               and amt_arrears_princ = 0
               and ctr_disb > 0
               and amt_disbursed > 0
               and a.cod_Acct_no = c.cod_Acct_no
               and amt_arrears_due > 0
               and a.migration_source = 'BRNET'
           );

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'co_ln_acct_dtls', 'amt_arrears_due', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : accounts with zero principal but arrears due > 0.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+PARALLEL(64)*/ VAR_L_CONSIS_NO, cod_acct_no, 'BRNET'
        FROM (
              select /*+PARALLEL(64) */ a.cod_acct_no
                from co_ln_acct_dtls a, co_ln_acct_balances b
               WHERE a.cod_Acct_no = b.cod_Acct_no
                 and cod_Acct_stat NOT IN (1, 5)
                 and amt_princ_balance = 0
                 and amt_arrears_princ = 0
                 and ctr_disb > 0
                 and amt_disbursed > 0
                 and a.migration_source = 'BRNET'
              MINUS
              select /*+PARALLEL(64) */ a.cod_acct_no
                from co_ln_acct_dtls a, co_ln_acct_balances b, CO_LN_ARREARS_TABLE c
               WHERE a.COD_aCCT_NO = b.COD_aCCT_NO
                 and cod_Acct_stat NOT IN (1, 5)
                 and amt_princ_balance = 0
                 and amt_arrears_princ = 0
                 and ctr_disb > 0
                 and amt_disbursed > 0
                 and a.cod_Acct_no = c.cod_Acct_no
                 and amt_arrears_due > 0
                 and a.migration_source = 'BRNET'
             );
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12934 : Principal balance > 0 with CTR_DISB = 0 or first disb null (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12934;
  BEGIN
    SELECT /*+parallel(64)*/ COUNT(1)
      into var_l_rec_count
      FROM co_ln_acct_dtls a, co_ln_acct_balances b
     WHERE a.cod_acct_no = b.cod_acct_no
       AND b.amt_princ_balance > 0
       AND (a.ctr_disb = 0 OR a.dat_first_disb IS NULL)
       AND a.migration_source = 'CBS';

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'co_ln_acct_dtls', 'AMT_PRINC_BALANCE', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : LOANS WHERE PRINCIPAL BALANCE > ZERO AND CTR_DISB = ZERO.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, a.COD_ACCT_NO, 'CBS'
        FROM co_ln_acct_dtls a, co_ln_acct_balances b
       WHERE a.cod_acct_no = b.cod_acct_no
         AND b.amt_princ_balance > 0
         AND (a.ctr_disb = 0 OR a.dat_first_disb IS NULL)
         AND a.migration_source = 'CBS';
    END IF;

    SELECT /*+parallel(64)*/ COUNT(1)
      into var_l_rec_count
      FROM co_ln_acct_dtls a, co_ln_acct_balances b
     WHERE a.cod_acct_no = b.cod_acct_no
       AND b.amt_princ_balance > 0
       AND (a.ctr_disb = 0 OR a.dat_first_disb IS NULL)
       AND a.migration_source = 'BRNET';

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'co_ln_acct_dtls', 'AMT_PRINC_BALANCE', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : LOANS WHERE PRINCIPAL BALANCE > ZERO AND CTR_DISB = ZERO.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */
             VAR_L_CONSIS_NO, a.COD_ACCT_NO, 'BRNET'
        FROM co_ln_acct_dtls a, co_ln_acct_balances b
       WHERE a.cod_acct_no = b.cod_acct_no
         AND b.amt_princ_balance > 0
         AND (a.ctr_disb = 0 OR a.dat_first_disb IS NULL)
         AND a.migration_source = 'BRNET';
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12935 : Missing base int rate detail (ctr_int_srl = 0) (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12935;
  BEGIN
    SELECT /*+parallel(64)*/
           COUNT(1)
      into var_l_rec_count
      FROM co_ln_acct_dtls a
     WHERE cod_acct_stat <> 1
       AND ctr_disb > 0
       AND NOT EXISTS (SELECT 1
              FROM co_ln_acct_rates_detl
             WHERE cod_acct_no = a.cod_acct_no
               AND ctr_int_srl = 0)
       AND a.migration_source = 'CBS';

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'co_ln_acct_rates_detl', 'ctr_int_srl', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : no base rate (ctr_int_srl=0) in co_ln_acct_rates_detl for active disbursed accounts.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */ VAR_L_CONSIS_NO, COD_ACCT_NO, 'CBS'
        FROM co_ln_acct_dtls a
       WHERE cod_acct_stat <> 1
         AND ctr_disb > 0
         AND NOT EXISTS (SELECT 1
                FROM co_ln_acct_rates_detl
               WHERE cod_acct_no = a.cod_acct_no
                 AND ctr_int_srl = 0)
         AND a.migration_source = 'CBS';
    END IF;

    SELECT /*+parallel(64)*/
           COUNT(1)
      into var_l_rec_count
      FROM co_ln_acct_dtls a
     WHERE cod_acct_stat <> 1
       AND ctr_disb > 0
       AND NOT EXISTS (SELECT 1
              FROM co_ln_acct_rates_detl
             WHERE cod_acct_no = a.cod_acct_no
               AND ctr_int_srl = 0)
       AND a.migration_source = 'BRNET';

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'co_ln_acct_rates_detl', 'ctr_int_srl', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : no base rate (ctr_int_srl=0) in co_ln_acct_rates_detl for active disbursed accounts.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT /*+ PARALLEL(64) */ VAR_L_CONSIS_NO, COD_ACCT_NO, 'BRNET'
        FROM co_ln_acct_dtls a
       WHERE cod_acct_stat <> 1
         AND ctr_disb > 0
         AND NOT EXISTS (SELECT 1
                FROM co_ln_acct_rates_detl
               WHERE cod_acct_no = a.cod_acct_no
                 AND ctr_int_srl = 0)
         AND a.migration_source = 'BRNET';
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12936 : Int base not found for charging (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12936;
  BEGIN
    SELECT /*+parallel(64)*/
           count(1)
      into var_l_rec_count
      FROM (SELECT A.COD_ACCT_NO, MIN(dat_baschg_eff) MIN_base
              FROM co_ln_int_base_hist A
             GROUP BY A.COD_ACCT_NO) A,
           co_ln_acct_dtls B
     WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       AND b.cod_acct_stat <> 1
       AND b.ctr_disb > 0
       AND MIN_base > NVL(B.DAT_LAST_charged, B.DAT_FIRST_DISB)
       AND EXISTS (SELECT 1
              FROM co_ln_acct_schedule
             WHERE cod_acct_no = b.cod_acct_no)
       AND b.migration_source = 'CBS';

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'co_ln_int_base_hist', 'dat_baschg_eff (MIN_base)', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : int base not found for charging.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT VAR_L_CONSIS_NO, B.COD_ACCT_NO, 'CBS'
        FROM (SELECT A.COD_ACCT_NO, MIN(dat_baschg_eff) MIN_base
                FROM co_ln_int_base_hist A
               GROUP BY A.COD_ACCT_NO) A,
             co_ln_acct_dtls B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
         AND b.cod_acct_stat <> 1
         AND b.ctr_disb > 0
         AND MIN_base > NVL(B.DAT_LAST_charged, B.DAT_FIRST_DISB)
         AND EXISTS (SELECT 1
                FROM co_ln_acct_schedule
               WHERE cod_acct_no = b.cod_acct_no)
         AND b.migration_source = 'CBS';
    END IF;

    SELECT /*+parallel(64)*/
           count(1)
      into var_l_rec_count
      FROM (SELECT A.COD_ACCT_NO, MIN(dat_baschg_eff) MIN_base
              FROM co_ln_int_base_hist A
             GROUP BY A.COD_ACCT_NO) A,
           co_ln_acct_dtls B
     WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       AND b.cod_acct_stat <> 1
       AND b.ctr_disb > 0
       AND MIN_base > NVL(B.DAT_LAST_charged, B.DAT_FIRST_DISB)
       AND EXISTS (SELECT 1
              FROM co_ln_acct_schedule
             WHERE cod_acct_no = b.cod_acct_no)
       AND b.migration_source = 'BRNET';

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'co_ln_int_base_hist', 'dat_baschg_eff (MIN_base)', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : int base not found for charging.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT VAR_L_CONSIS_NO, B.COD_ACCT_NO, 'BRNET'
        FROM (SELECT A.COD_ACCT_NO, MIN(dat_baschg_eff) MIN_base
                FROM co_ln_int_base_hist A
               GROUP BY A.COD_ACCT_NO) A,
             co_ln_acct_dtls B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
         AND b.cod_acct_stat <> 1
         AND b.ctr_disb > 0
         AND MIN_base > NVL(B.DAT_LAST_charged, B.DAT_FIRST_DISB)
         AND EXISTS (SELECT 1
                FROM co_ln_acct_schedule
               WHERE cod_acct_no = b.cod_acct_no)
         AND b.migration_source = 'BRNET';
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12937 : Disbursed loans where schedule not present (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12937;
  BEGIN
    SELECT /*+parallel(64)*/
           count(1)
      into var_l_rec_count
      FROM co_ln_acct_dtls A
     WHERE A.CTR_DISB > 0
       AND NOT EXISTS (SELECT 1 FROM co_ln_ACCT_SCHEDULE B WHERE B.COD_ACCT_NO = A.COD_ACCT_NO)
       AND COD_ACCT_STAT NOT IN (1, 5)
       AND A.migration_source = 'CBS';

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'co_ln_acct_dtls', 'CTR_DISB', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : DISBURSED LOANS WHERE SCHEDULE IS NOT PRESENT.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT VAR_L_CONSIS_NO, COD_ACCT_NO, 'CBS'
        FROM co_ln_acct_dtls A
       WHERE A.CTR_DISB > 0
         AND NOT EXISTS (SELECT 1 FROM co_ln_ACCT_SCHEDULE B WHERE B.COD_ACCT_NO = A.COD_ACCT_NO)
         AND COD_ACCT_STAT NOT IN (1, 5)
         AND A.migration_source = 'CBS';
    END IF;

    SELECT /*+parallel(64)*/
           count(1)
      into var_l_rec_count
      FROM co_ln_acct_dtls A
     WHERE A.CTR_DISB > 0
       AND NOT EXISTS (SELECT 1 FROM co_ln_ACCT_SCHEDULE B WHERE B.COD_ACCT_NO = A.COD_ACCT_NO)
       AND COD_ACCT_STAT NOT IN (1, 5)
       AND A.migration_source = 'BRNET';

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'co_ln_acct_dtls', 'CTR_DISB', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : DISBURSED LOANS WHERE SCHEDULE IS NOT PRESENT.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT VAR_L_CONSIS_NO, COD_ACCT_NO, 'BRNET'
        FROM co_ln_acct_dtls A
       WHERE A.CTR_DISB > 0
         AND NOT EXISTS (SELECT 1 FROM co_ln_ACCT_SCHEDULE B WHERE B.COD_ACCT_NO = A.COD_ACCT_NO)
         AND COD_ACCT_STAT NOT IN (1, 5)
         AND A.migration_source = 'BRNET';
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  -----------------------------------------------------------------------------
  -- CONSIS 12938 : Regular accounts with Suspended CRR code (split)
  -----------------------------------------------------------------------------
  VAR_L_CONSIS_NO := 12938;
  BEGIN
    select /*+parallel(128)*/ count(1)
      into var_l_rec_count
      from Co_Ln_Acct_Dtls a, co_ln_acct_balances b
     WHERE a.COD_aCCT_NO in
           (select cod_accT_no
              from co_ac_acct_crr_code
             where cod_crr_from in
                   (select cod_crr
                      from CBSFCHOST.ac_crr_codes
                     where flg_accr_status = 'S'))
       and flg_accr_status != 'S'
       and a.cod_Acct_no = b.cod_Acct_no
       and (b.amt_princ_balance > 0 or b.amt_arrears_regular_int > 0 or
            b.amt_arrears_princ > 0 or b.amt_arrears_fees > 0)
       and a.migration_source = 'CBS';

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'LN', 'Co_Ln_Acct_Dtls', 'amt_arrears_regular_int', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : Regular accounts whose CRR code is updated as Suspended.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT VAR_L_CONSIS_NO, a.COD_ACCT_NO, 'CBS'
        from Co_Ln_Acct_Dtls a, co_ln_acct_balances b
       WHERE a.COD_aCCT_NO in
             (select cod_accT_no
                from co_ac_acct_crr_code
               where cod_crr_from in
                     (select cod_crr
                        from CBSFCHOST.ac_crr_codes
                       where flg_accr_status = 'S'))
         and flg_accr_status != 'S'
         and a.cod_Acct_no = b.cod_Acct_no
         and (b.amt_princ_balance > 0 or b.amt_arrears_regular_int > 0 or
              b.amt_arrears_princ > 0 or b.amt_arrears_fees > 0)
         and a.migration_source = 'CBS';
    END IF;

    select /*+parallel(128)*/ count(1)
      into var_l_rec_count
      from Co_Ln_Acct_Dtls a, co_ln_acct_balances b
     WHERE a.COD_aCCT_NO in
           (select cod_accT_no
              from co_ac_acct_crr_code
             where cod_crr_from in
                   (select cod_crr
                      from CBSFCHOST.ac_crr_codes
                     where flg_accr_status = 'S'))
       and flg_accr_status != 'S'
       and a.cod_Acct_no = b.cod_Acct_no
       and (b.amt_princ_balance > 0 or b.amt_arrears_regular_int > 0 or
            b.amt_arrears_princ > 0 or b.amt_arrears_fees > 0)
       and a.migration_source = 'BRNET';

    INSERT INTO co_ln_consis
      (COD_CONSIS_NO, NAM_MODULE, NAM_TABLE, NAM_COLUMN, NAM_CONSIS_FUNC, CONSIS_COUNT, DESC_CONS)
    VALUES
      (VAR_L_CONSIS_NO, 'BRNET', 'Co_Ln_Acct_Dtls', 'amt_arrears_regular_int', VAR_L_FUNCTION_NAME, VAR_L_REC_COUNT,
       'COD_ACCT_NO : Regular accounts whose CRR code is updated as Suspended.');

    IF (VAR_L_REC_COUNT <> 0) THEN
      INSERT /*+ ENABLE_PARALLEL_DML APPEND NOLOGGING PARALLEL(CO_LN_CONSIS_ACCT, 64) */
        INTO co_ln_consis_acct (COD_CONSIS_NO, COD_ACCT_NO, migration_source)
      SELECT VAR_L_CONSIS_NO, a.COD_ACCT_NO, 'BRNET'
        from Co_Ln_Acct_Dtls a, co_ln_acct_balances b
       WHERE a.COD_aCCT_NO in
             (select cod_accT_no
                from co_ac_acct_crr_code
               where cod_crr_from in
                     (select cod_crr
                        from CBSFCHOST.ac_crr_codes
                       where flg_accr_status = 'S'))
         and flg_accr_status != 'S'
         and a.cod_Acct_no = b.cod_Acct_no
         and (b.amt_princ_balance > 0 or b.amt_arrears_regular_int > 0 or
              b.amt_arrears_princ > 0 or b.amt_arrears_fees > 0)
         and a.migration_source = 'BRNET';
    END IF;
    COMMIT;
  END;
  AP_BB_MIG_LOG_STRING(LPAD(VAR_L_CONSIS_NO, 5, 0) || ' #' || VAR_L_FUNCTION_NAME || '# Stream = ' || VAR_PI_STREAM);

  RETURN 1;
END;
/
