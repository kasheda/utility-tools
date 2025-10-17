CREATE OR REPLACE FUNCTION "AP_CONS_BA_COLL_HDR" (var_pi_stream IN NUMBER)
  RETURN NUMBER AS
  var_l_count      NUMBER;
  var_l_consis_no  NUMBER := 0;
  var_l_table_name VARCHAR2(100) := 'CO_BA_COLL_HDR';
BEGIN
  ap_bb_mig_log_string('Started #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream);
  DELETE FROM co_ba_consis
   WHERE cod_consis_no >= 61001
     AND cod_consis_no <= 61999;
  --WHERE upper(nam_table) = upper(var_l_table_name);

  DELETE FROM co_ba_consis_coll
   WHERE cod_consis_no >= 61001
     AND cod_consis_no <= 61999;

  COMMIT;
  ap_bb_mig_log_string('00000 #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --Beginning of function
  var_l_consis_no := 61001;
  /*consis 61001: COD_COLLAT_ID+COD_COLL must be unique */ -- 1. Ensure Unique - combination of COD_COLLAT_ID and COD_COLL / 2.can only be external collat id
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM (SELECT COUNT(*) cnt
              FROM co_ba_coll_hdr
             GROUP BY cod_collat_id, cod_coll, flg_mnt_status)
     WHERE cnt > 1;

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              40);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61001,
       'BA',
       var_l_table_name,
       'COD_COLLAT_ID',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'COD_COLLAT_ID,COD_COLL must be unique ');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              64);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61001, cod_collat_id
          FROM co_ba_coll_hdr
         GROUP BY cod_collat_id, cod_coll, flg_mnt_status
        HAVING COUNT(1) > 1;

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                86);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61002;
  /*consis 61002: AMT_LAST_VAL cannot be zero or less than zero*/

  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM --FA : 09-Apr-2024 Run : Begin : Consider only primary collateral
           co_ba_coll_hdr h
     WHERE h.amt_last_val <= 0
       and exists (select 1
              from co_ba_ho_coll_acct_xref x
             where x.cod_collat_id = h.cod_collat_id
               and x.flg_coll_sec = 'P');
    --FA : 09-Apr-2024 Run : End : Consider only primary collateral
    /*esaf_changes*/
    --FROM co_ba_coll_hdr
    --WHERE amt_last_val <= 0;

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              103);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61002,
       'BA',
       var_l_table_name,
       'AMT_LAST_VAL',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'AMT_LAST_VAL cannot be zero or less than zero');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              126);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61002, cod_collat_id
          FROM --FA : 09-Apr-2024 Run : Begin : Consider only primary collateral
               co_ba_coll_hdr h
         WHERE h.amt_last_val <= 0
           and exists (select 1
                  from co_ba_ho_coll_acct_xref x
                 where x.cod_collat_id = h.cod_collat_id
                   and x.flg_coll_sec = 'P');
      --FA : 09-Apr-2024 Run : End : Consider only primary collateral

      /* esaf_changes
            FROM co_ba_coll_hdr
           WHERE amt_last_val <= 0;
      */

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                145);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61003;
  /*consis 61003: AMT_ORIG_VALUE cannot be zero or less than zero*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM --FA : 09-Apr-2024 Run : Begin : Consider only primary collateral
           co_ba_coll_hdr h
     WHERE h.amt_orig_value <= 0
       and exists (select 1
              from co_ba_ho_coll_acct_xref x
             where x.cod_collat_id = h.cod_collat_id
               and x.flg_coll_sec = 'P');
    --FA : 09-Apr-2024 Run : End : Consider only primary collateral

    /*esaf_changes
       FROM co_ba_coll_hdr
      WHERE amt_orig_value <= 0;
    */

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              161);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61003,
       'BA',
       var_l_table_name,
       'AMT_ORIG_VALUE',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'AMT_ORIG_VALUE cannot be zero or less than zero');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              184);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61003, cod_collat_id
          FROM --FA : 09-Apr-2024 Run : Begin : Consider only primary collateral
               co_ba_coll_hdr h
         WHERE h.amt_orig_value <= 0
           and exists (select 1
                  from co_ba_ho_coll_acct_xref x
                 where x.cod_collat_id = h.cod_collat_id
                   and x.flg_coll_sec = 'P');
      --FA : 09-Apr-2024 Run : End : Consider only primary collateral

      /*esaf_changes
            FROM co_ba_coll_hdr
           WHERE amt_orig_value <= 0;
      */

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                203);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61004;
  /*consis 61004: AMT_MARKET_VAL  cannot be zero or less than zero*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr
     WHERE amt_market_val <= 0;

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              218);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61004,
       'BA',
       var_l_table_name,
       'AMT_MARKET_VAL',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'AMT_MARKET_VAL cannot be zero or less than zero');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              241);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61004, cod_collat_id
          FROM co_ba_coll_hdr
         WHERE amt_market_val <= 0;

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                260);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61005;
  /*consis 61005: COD_CCY must be equivalent to INR*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr
     WHERE cod_ccy NOT IN (SELECT cod_ccy
                             FROM CBSFCHOST.ba_ccy_code
                            WHERE nam_ccy_short = 'INR'
                              AND flg_mnt_status = 'A');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              283);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61005,
       'BA',
       var_l_table_name,
       'COD_CCY',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'COD_CCY must be equivalent to INR');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              307);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61005, cod_collat_id
          FROM co_ba_coll_hdr
         WHERE cod_ccy NOT IN (SELECT cod_ccy
                                 FROM cbsfchost.ba_ccy_code
                                WHERE nam_ccy_short = 'INR'
                                  AND flg_mnt_status = 'A');

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                334);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61006;
  /*consis 61006: COD_CHARGE_TYPE should not be other than 1,2,3*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr
     WHERE cod_charge_type NOT IN (1, 2, 3);

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              349);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61006,
       'BA',
       var_l_table_name,
       'COD_CHARGE_TYPE',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'COD_CHARGE_TYPE should not be other than 1,2,3');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              373);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61006, cod_collat_id
          FROM co_ba_coll_hdr
         WHERE cod_charge_type NOT IN (1, 2, 3);

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                392);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61007;
  /*consis 61007: COD_CUSTODY_STATUS cannot be null or other than 0,1,2,3,4,5*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr
     WHERE cod_custody_status NOT IN (0, 1, 2, 3, 4, 5);

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              408);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61007,
       'BA',
       var_l_table_name,
       'COD_CUSTODY_STATUS',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'COD_CUSTODY_STATUS cannot be null or other than 0,1,2,3,4,5');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              432);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61007, cod_collat_id
          FROM co_ba_coll_hdr
         WHERE cod_custody_status NOT IN (0, 1, 2, 3, 4, 5);

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                452);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61008;
  /*consis 61008: AMT_UNUSED_VAL should not be null and must be grater than equals to zero*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr
     WHERE amt_unused_val IS NULL
        OR amt_unused_val < 0;

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              468);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61008,
       'BA',
       var_l_table_name,
       'AMT_UNUSED_VAL',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'AMT_UNUSED_VAL should not be null and must be grater than equals to zero');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              492);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61008, cod_collat_id
          FROM co_ba_coll_hdr
         WHERE amt_unused_val IS NULL
            OR amt_unused_val < 0;

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                512);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61009;
  /*consis 61009: COD_COLL_HOMEBRN cannot be null */
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr
     WHERE (cod_coll_homebrn IS NULL OR cod_coll_homebrn = 0);

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              528);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61009,
       'BA',
       var_l_table_name,
       'COD_COLL_HOMEBRN',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'COD_COLL_HOMEBRN should not be null or 0(Zero)');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              553);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61009, cod_collat_id
          FROM co_ba_coll_hdr
         WHERE (cod_coll_homebrn IS NULL OR cod_coll_homebrn = 0);

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                574);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61010;
  /*consis 61010: DAT_LAST_VAL cannot be null*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr
     WHERE (dat_last_val IS NULL OR dat_last_val = '01-Jan-1950' OR
           dat_last_val = '01-Jan-1800');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              591);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61010,
       'BA',
       var_l_table_name,
       'DAT_LAST_VAL',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'DAT_LAST_VAL should not be null or default date.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              614);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61010, cod_collat_id
          FROM co_ba_coll_hdr
         WHERE (dat_last_val IS NULL OR dat_last_val = '01-Jan-1950' OR
               dat_last_val = '01-Jan-1800');

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                635);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61011;
  /*consis 61011: DAT_DEED_RETURN should be 01-jan-1950*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr
     WHERE dat_deed_return is null
        OR dat_deed_return IN
           (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'));

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              650);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61011,
       'BA',
       var_l_table_name,
       'DAT_DEED_RETURN',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'DAT_DEED_RETURN should not be 01-jan-1950/01-Jan-1800 ');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              674);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61011, cod_collat_id
          FROM co_ba_coll_hdr
         WHERE dat_deed_return is null
            OR dat_deed_return IN
               (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'));

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                693);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61012;
  /*consis 61012: DAT_DEED_SENT should be 01-jan-1950*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr
     WHERE dat_deed_sent is null
        OR dat_deed_sent IN
           (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'));

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              708);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61012,
       'BA',
       var_l_table_name,
       'DAT_DEED_SENT',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'DAT_DEED_SENT should not be 01-jan-1950/01-Jan-1800.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              732);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61012, cod_collat_id
          FROM co_ba_coll_hdr
         WHERE dat_deed_sent is null
            OR dat_deed_sent NOT IN
               (TO_DATE('01-jan-1950'), TO_DATE('01-jan-1800'));

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                751);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61013;
  /*consis 61013: DAT_ORIG_VALUE cannot be null*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr
     WHERE (dat_orig_value IS NULL OR dat_orig_value = '01-Jan-1950' OR
           dat_orig_value = '01-Jan-1800');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              768);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61013,
       'BA',
       var_l_table_name,
       'DAT_ORIG_VALUE',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'DAT_ORIG_VALUE cannot be null or default date.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              791);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61013, cod_collat_id
          FROM co_ba_coll_hdr
         WHERE (dat_orig_value IS NULL OR dat_orig_value = '01-Jan-1950' OR
               dat_orig_value = '01-Jan-1800');

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                812);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61014;
  /*consis 61014: NAM_LENDER cannot be null for COD_CHARGE_TYPE 2 or 3*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr
     WHERE nam_lender IS NULL
       AND cod_charge_type IN (2, 3);

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              828);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61014,
       'BA',
       var_l_table_name,
       'NAM_LENDER',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       ' NAM_LENDER cannot be null for COD_CHARGE_TYPE 2,3 ');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              852);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61014, cod_collat_id
          FROM co_ba_coll_hdr
         WHERE nam_lender IS NULL
           AND cod_charge_type IN (2, 3);

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                872);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61015;
  /*consis 61015: FLG_SHARED_COLL cannot be other than Y,N*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr
     WHERE flg_shared_coll NOT IN ('Y', 'N');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              887);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61015,
       'BA',
       var_l_table_name,
       'FLG_SHARED_COLL',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       ' FLG_SHARED_COLL cannot be other than Y,N');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              911);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61015, cod_collat_id
          FROM co_ba_coll_hdr
         WHERE flg_shared_coll NOT IN ('Y', 'N');

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                930);
    END;
  END IF;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61016;
  /*consis 61016: COD_COLL_CLASS should not be other than 0,1,2*/
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr
     WHERE cod_coll_class NOT IN ('0', '1', '2');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              945);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61016,
       'BA',
       var_l_table_name,
       'FLG_SHARED_COLL',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'COD_COLL_CLASS should not be other than 0,1,2');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              969);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61016, cod_collat_id
          FROM co_ba_coll_hdr
         WHERE cod_coll_class NOT IN ('0', '1', '2');

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                988);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61017;
  /*Consis 61017 */

  BEGIN
    SELECT COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr a
     WHERE (a.dat_last_val IS NULL OR a.dat_last_val = '01-Jan-1950' OR
           a.dat_last_val = '01-Jan-1800');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              1007);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61017,
       'BA',
       var_l_table_name,
       'dat_last_val',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'DAT_LAST_VAL - LAST VALUATION DATE CAN NOT BE NULL OR default date.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1031);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61017, cod_collat_id
          FROM co_ba_coll_hdr a
         WHERE a.dat_last_val IS NULL;

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                1050);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61018;
  /*61018 */

  BEGIN
    SELECT COUNT(1)
      INTO var_l_count
      FROM co_ba_ho_coll_acct_xref a
     WHERE cod_collat_id NOT IN (SELECT cod_collat_id FROM co_ba_coll_hdr);

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              1072);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61018,
       'BA',
       'co_ba_ho_coll_acct_xref',
       'cod_collat_id',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'INVALID COD_COLLAT_ID - COD_COLLAT_ID NOT PRESENT IN  co_ba_coll_hdr.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1096);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61018, cod_collat_id
          FROM co_ba_ho_coll_acct_xref a
         WHERE cod_collat_id NOT IN
               (SELECT cod_collat_id FROM co_ba_coll_hdr);

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                1119);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61019;

  /*61019*/

  BEGIN
    SELECT COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr a
     WHERE (a.cod_coll_homebrn) NOT IN
           (SELECT cod_cc_brn
              FROM cbsfchost.ba_cc_brn_mast
             WHERE flg_mnt_status = 'A');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR select from co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              1145);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61019,
       'BA',
       var_l_table_name,
       'COD_COLL_HOMEBRN',
       'ap_cons_BA_COLL_HDR',
       var_l_count,
       'Branch code present co_ba_coll_hdr that are absent in branch master.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Insert into co_ba_consis Failed.' ||
                              sqlerrm,
                              1169);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61019, cod_collat_id
          FROM co_ba_coll_hdr a
         WHERE (a.cod_coll_homebrn) NOT IN
               (SELECT cod_cc_brn
                  FROM cbsfchost.ba_cc_brn_mast
                 WHERE flg_mnt_status = 'A');

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR Insert into co_ba_coll_hdr Failed.' ||
                                sqlerrm,
                                1195);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61020;
  /***********/

  BEGIN
    SELECT COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr a
     WHERE (a.cod_coll) NOT IN
           (SELECT cod_coll
              FROM cbsfchost.ba_coll_codes
             WHERE flg_mnt_status = 'A');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR select from co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              1222);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61020,
       'BA',
       var_l_table_name,
       'cod_coll',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'Collateral code present co_ba_coll_hdr must be from the ba_coll_codes (master information).');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Insert into co_ba_consis Failed.' ||
                              sqlerrm,
                              1246);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4) */
         61020, cod_collat_id
          FROM co_ba_coll_hdr a
         WHERE (a.cod_coll) NOT IN
               (SELECT cod_coll
                  FROM cbsfchost.ba_coll_codes
                 WHERE flg_mnt_status = 'A');

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR Insert into co_ba_coll_hdr Failed.' ||
                                sqlerrm,
                                1272);
    END;
  END IF;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis
  COMMIT;

  var_l_consis_no := 61021;
  /*consis 61021: COD_COLL_HOMEBRN Should from the branch master list of ba_cc_brn_mast */
  /* below consis was commented in capri*/
  BEGIN
    SELECT /*+ PARALLEL(4)*/
     COUNT(1)
      INTO var_l_count
      FROM co_ba_coll_hdr
     WHERE cod_coll_homebrn NOT IN
           (SELECT cod_cc_brn
              FROM cbsfchost.ba_cc_brn_mast
             WHERE flg_mnt_status = 'A');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR Select From co_ba_coll_hdr Failed.' ||
                              sqlerrm,
                              1288);
  END;

  BEGIN
    INSERT INTO co_ba_consis
      (cod_consis_no,
       nam_module,
       nam_table,
       nam_column,
       nam_consis_func,
       consis_count,
       desc_cons)
    VALUES
      (61021,
       'BA',
       var_l_table_name,
       'COD_COLL_HOMEBRN',
       'AP_CONS_BA_COLL_HDR',
       var_l_count,
       'COD_COLL_HOMEBRN Should from the branch master list of ba_cc_brn_mast.');

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ora_raiserror(sqlcode,
                              'In AP_CONS_BA_COLL_HDR INSERT INTO co_ln_consis Failed.' ||
                              sqlerrm,
                              1312);
  END;

  IF (var_l_count > 0) THEN
    BEGIN
      INSERT INTO co_ba_consis_coll
        (cod_consis_no, cod_collat_id)
        SELECT /*+ PARALLEL(4)*/
         61021, cod_collat_id
          FROM co_ba_coll_hdr
         WHERE cod_coll_homebrn NOT IN
               (SELECT cod_cc_brn
                  FROM cbsfchost.ba_cc_brn_mast
                 WHERE flg_mnt_status = 'A');

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(sqlcode,
                                'In AP_CONS_BA_COLL_HDR INSERT INTO co_ba_consis_coll Failed.' ||
                                sqlerrm,
                                1331);
    END;
  END IF;
  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) ||
                       ' #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --after each consis

var_l_consis_no := 61022;
    /*consis 61022: For same date (DAT_LAST_VAL = DAT_ORIG_VALUE), AMT_LAST_VAL is not same as AMT_ORIG_VALUE*/
    BEGIN
        SELECT /*+ PARALLEL(8) */
            COUNT(1)
        INTO var_l_count
        FROM
            co_ba_coll_hdr a
        WHERE (a.dat_last_val = a.dat_orig_value and
               (a.amt_last_val <> a.amt_orig_value))
        ;

    EXCEPTION
        WHEN OTHERS THEN
            cbsfchost.ora_raiserror(sqlcode, 'In ap_cons_ba_coll_hdr Select From co_ba_coll_hdr Failed.' || sqlerrm, 1288);
    END;

    BEGIN
        INSERT INTO co_ba_consis (
            cod_consis_no,
            nam_module,
            nam_table,
            nam_column,
            nam_consis_func,
            consis_count,
            desc_cons
        ) VALUES (
            var_l_consis_no,
            'BA',
            var_l_table_name,
            'AMT_LAST_VAL',
            'ap_cons_BA_COLL_HDR',
            var_l_count,
            'For same date (DAT_LAST_VAL = DAT_ORIG_VALUE), AMT_LAST_VAL is not same as AMT_ORIG_VALUE'
        );

    EXCEPTION
        WHEN OTHERS THEN
            cbsfchost.ora_raiserror(sqlcode, 'In ap_cons_ba_coll_hdr INSERT INTO co_ln_consis Failed.' || sqlerrm, 1312);
    END;

    IF ( var_l_count > 0 ) THEN
        BEGIN
            INSERT INTO co_ba_consis_coll (
                cod_consis_no,
                cod_collat_id
            )
                SELECT /*+ PARALLEL(8) */
                    var_l_consis_no,
                    cod_collat_id
                FROM
                    co_ba_coll_hdr a
                WHERE (a.dat_last_val = a.dat_orig_value and
                       (a.amt_last_val <> a.amt_orig_value))
                ;

        EXCEPTION
            WHEN OTHERS THEN
                cbsfchost.ora_raiserror(sqlcode, 'In ap_cons_ba_coll_hdr INSERT INTO co_ba_consis_coll Failed.' || sqlerrm, 1331);
        END;
    END IF;
	ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #ap_cons_ba_coll_hdr# Stream = ' || var_pi_stream); --after each consis
    
    
	var_l_consis_no := 61023;
    /*consis 61022: For same date (DAT_LAST_VAL = DAT_ORIG_VALUE), AMT_LAST_VAL is not same as AMT_ORIG_VALUE*/
    BEGIN
        SELECT /*+ PARALLEL(8) */
            COUNT(1)
        INTO var_l_count
       FROM co_ba_coll_hdr a
     WHERE (a.amt_last_val = 0 or a.amt_orig_value = 0 or
           (a.amt_last_val = 0 and a.amt_orig_value > 0));

    EXCEPTION
        WHEN OTHERS THEN
            cbsfchost.ora_raiserror(sqlcode, 'In ap_cons_ba_coll_hdr Select From co_ba_coll_hdr Failed.' || sqlerrm, 1288);
    END;

    BEGIN
        INSERT INTO co_ba_consis (
            cod_consis_no,
            nam_module,
            nam_table,
            nam_column,
            nam_consis_func,
            consis_count,
            desc_cons
        ) VALUES (
            var_l_consis_no,
            'BA',
            var_l_table_name,
            'amt_last_val',
            'ap_cons_BA_COLL_HDR',
            var_l_count,
            'Loans  in table civ_ba_coll_hdr Where amt_last_val is zero or amt_orig_value = zero.'
        );

    EXCEPTION
        WHEN OTHERS THEN
            cbsfchost.ora_raiserror(sqlcode, 'In ap_cons_ba_coll_hdr INSERT INTO co_ln_consis Failed.' || sqlerrm, 1312);
    END;

    IF ( var_l_count > 0 ) THEN
        BEGIN
            INSERT INTO co_ba_consis_coll (
                cod_consis_no,
                cod_collat_id
            )
                SELECT /*+ PARALLEL(8) */
                    var_l_consis_no,
                    cod_collat_id
                FROM co_ba_coll_hdr a
     WHERE (a.amt_last_val = 0 or a.amt_orig_value = 0 or
           (a.amt_last_val = 0 and a.amt_orig_value > 0));

        EXCEPTION
            WHEN OTHERS THEN
                cbsfchost.ora_raiserror(sqlcode, 'In ap_cons_ba_coll_hdr INSERT INTO co_ba_consis_coll Failed.' || sqlerrm, 1331);
        END;
    END IF;
	ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #ap_cons_ba_coll_hdr# Stream = ' || var_pi_stream); --after each consis    



  COMMIT;
  ap_bb_mig_log_string('99999 #AP_CONS_BA_COLL_HDR_1# Stream = ' ||
                       var_pi_stream); --Ending of function
  COMMIT;
  RETURN 0;
END AP_CONS_BA_COLL_HDR;
/
