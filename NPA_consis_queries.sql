INSERT /*+enable_prallel_dml parallel(32)*/
        INTO npa_consis_check
          (consis_no, date_run, cod_Acct_no_pri, cod_acct_no_sec, npa_date)
          SELECT /*+parallel(32) FULL(LAD) FULL(B)*/
          DISTINCT 3, '15-June-25', '', a.cod_Acct_No, a.dat_npa
            FROM civ_ac_Acct_crr_code a, civ_ln_acct_attributes b, civ_LN_ACCT_DTLS LAD
           WHERE a.cod_acct_no = b.cod_acct_no
             AND a.cod_acct_no = LAD.cod_acct_no
             AND a.flg_mnt_status = 'A'
             AND b.flg_mnt_status = 'A'
             AND LAD.flg_mnt_status = 'A'
             AND LAD.COD_ACCT_STAT NOT IN (1, 5, 11)
             AND decode(a.Cod_Crr_Mvmt_Reason,
                        ' ',
                        'X',
                        a.Cod_Crr_Mvmt_Reason) != 'X'
             AND NVL(a.dat_npa, '01-JAN-1950') <>
                 NVL(b.dat_npl, '01-JAN-1950');
                 
INSERT /*+enable_prallel_dml parallel(32)*/
        INTO npa_consis_check
          (consis_no, date_run, cod_Acct_no_pri, cod_acct_no_sec, npa_date)
          SELECT /*+parallel(32) FULL(B)*/
          DISTINCT 4, '15-June-25', '', a.cod_Acct_No, a.dat_npa
            FROM civ_ac_Acct_crr_code a, civ_ch_acct_mast b
           WHERE a.cod_acct_no = b.cod_acct_no
             AND a.flg_mnt_status = 'A'
             AND b.flg_mnt_status = 'A'
             AND B.COD_ACCT_STAT NOT IN (1, 5)
             AND decode(a.Cod_Crr_Mvmt_Reason,
                        ' ',
                        'X',
                        a.Cod_Crr_Mvmt_Reason) != 'X'
             AND NVL(a.dat_npa, '01-JAN-1950') <>
                 NVL(b.dat_npl, '01-JAN-1950');
                 
                 
                 
                   INSERT /*+enable_prallel_dml parallel(32)*/
        INTO npa_consis_check
          (consis_no,
           date_run,
           cod_Acct_no_pri,
           cod_acct_no_sec,
           cod_crr_cust,
           flg_accr_status)
          SELECT /*+parallel(32)*/
          DISTINCT 5,
                   '15-June-25',
                   '',
                   a.cod_Acct_No,
                   a.cod_crr_cust,
                   b.flg_accr_status
            FROM civ_ac_Acct_crr_code a, civ_ln_acct_dtls b, cbsfchost.ac_crr_codes c
           WHERE a.cod_acct_no = b.cod_acct_no
             AND a.cod_crr_cust = c.cod_crr
             AND a.flg_mnt_status = 'A'
             AND b.flg_mnt_status = 'A'
             AND c.flg_mnt_status = 'A'
             AND b.cod_Acct_stat NOT IN (1, 5, 11)
             AND b.flg_accr_status <> c.flg_accr_status
             AND decode(a.Cod_Crr_Mvmt_Reason,
                        ' ',
                        'X',
                        a.Cod_Crr_Mvmt_Reason) != 'X';
                        
                        
                        
                        
                        
                          INSERT /*+enable_prallel_dml parallel(32)*/
        INTO npa_consis_check
          (consis_no,
           date_run,
           cod_Acct_no_pri,
           cod_acct_no_sec,
           cod_crr_cust,
           flg_accr_status)
          SELECT /*+parallel(32) FULL(B) FULL(C)*/
          DISTINCT 6,
                   '15-June-25',
                   '',
                   a.cod_Acct_No,
                   a.cod_crr_cust,
                   b.flg_accr_status

            FROM civ_ac_Acct_crr_code a, civ_ch_acct_mast b, cbsfchost.ac_crr_codes c
           WHERE a.cod_acct_no = b.cod_acct_no
             AND a.cod_crr_cust = c.cod_crr
             AND a.flg_mnt_status = 'A'
             AND b.flg_mnt_status = 'A'
             AND c.flg_mnt_status = 'A'
             AND b.cod_Acct_stat NOT IN (1, 5)
             AND b.flg_accr_status <> c.flg_accr_status
             AND decode(a.Cod_Crr_Mvmt_Reason,
                        ' ',
                        'X',
                        a.Cod_Crr_Mvmt_Reason) != 'X';
                        
                        
                        
                         INSERT /*+enable_prallel_dml parallel(32)*/
        INTO npa_consis_check
          (consis_no,
           date_run,
           cod_Acct_no_pri,
           cod_acct_no_sec,
           cod_crr_cust,
           npa_date)
          SELECT /*+parallel(32)*/
          DISTINCT 7,
                   '15-June-25',
                   '',
                   a.cod_Acct_No,
                   a.cod_crr_cust,
                   a.dat_npa

            FROM civ_ac_Acct_crr_code a, civ_ch_acct_cust_xref b
           WHERE a.cod_Acct_no = b.cod_Acct_no
             AND a.flg_mnt_status = 'A'
             AND b.flg_mnt_status = 'A'
             AND cod_crr_cust >= 1200
             AND NOT EXISTS (SELECT 1
                    FROM CIV_LN_ACCT_DTLS LAD
                   WHERE LAD.COD_ACCT_NO = B.COD_ACCT_NO
                     AND LAD.COD_ACCT_STAT IN (1, 5, 11)
                     AND FLG_MNT_STATUS = 'A')
             AND NOT EXISTS
           (SELECT 1
                    FROM CIV_CH_ACCT_MAST CAM
                   WHERE CAM.COD_ACCT_NO = B.COD_ACCT_NO
                     AND CAM.COD_ACCT_STAT IN (1, 5)
                     AND FLG_MNT_STATUS = 'A'
                     AND COD_PROD IN
                         (SELECT COD_PROD
                            FROM CBSFCHOST.CH_PROD_MAST CPM
                           WHERE CPM.COD_TYP_PROD = 'A'
                             AND FLG_MNT_STATUS = 'A'))
             AND nvl(dat_npa, '01-Jan-1950') = '01-Jan-1950'
             AND decode(a.Cod_Crr_Mvmt_Reason,
                        ' ',
                        'X',
                        a.Cod_Crr_Mvmt_Reason) != 'X';
                        
                        
                        
                        
                        
                          INSERT /*+enable_prallel_dml parallel(32)*/
        INTO npa_consis_check
          (consis_no,
           date_run,
           cod_Acct_no_pri,
           cod_acct_no_sec,
           cod_crr_cust,
           npa_date)
          SELECT /*+parallel(32)*/
          DISTINCT 8,
                   '15-June-25',
                   '',
                   a.cod_Acct_No,
                   a.cod_crr_cust,
                   a.dat_npa

            FROM civ_ac_Acct_crr_code a, civ_ch_acct_cust_xref b
           WHERE a.cod_Acct_no = b.cod_Acct_no
             AND a.flg_mnt_status = 'A'
             AND b.flg_mnt_status = 'A'
             AND cod_crr_cust < 1200
             AND NOT EXISTS (SELECT 1
                    FROM CIV_LN_ACCT_DTLS LAD
                   WHERE LAD.COD_ACCT_NO = B.COD_ACCT_NO
                     AND LAD.COD_ACCT_STAT IN (1, 5, 11)
                     AND FLG_MNT_STATUS = 'A')
             AND NOT EXISTS
           (SELECT 1
                    FROM CIV_CH_ACCT_MAST CAM
                   WHERE CAM.COD_ACCT_NO = B.COD_ACCT_NO
                     AND CAM.COD_ACCT_STAT IN (1, 5)
                     AND FLG_MNT_STATUS = 'A'
                     AND COD_PROD IN
                         (SELECT COD_PROD
                            FROM CBSFCHOST.CH_PROD_MAST CPM
                           WHERE CPM.COD_TYP_PROD = 'A'
                             AND FLG_MNT_STATUS = 'A'))
             AND nvl(dat_npa, '01-Jan-1950') <> '01-Jan-1950'
             AND decode(a.Cod_Crr_Mvmt_Reason,
                        ' ',
                        'X',
                        a.Cod_Crr_Mvmt_Reason) != 'X';
                        
                        
                        
                        
                          INSERT /*+enable_prallel_dml parallel(32)*/
        INTO NPA_CONSIS_CHECK
          (CONSIS_NO,
           DATE_RUN,
           COD_ACCT_NO_PRI,
           COD_CUST_ID,
           COD_ACCT_NO_SEC,
           NPA_DATE,
           COD_CRR_CUST,
           COD_CRR_TO,
           FLG_ACCR_STATUS,
           DAT_COOLING_PRD_END)
          SELECT /*+parallel(32)*/
           9,
           '15-June-25',
           NULL,
           COD_CUST_ID,
           COD_ACCT_NO,
           NULL,
           NULL,
           NULL,
           FLG_ACCR_STATUS,
           NULL
            FROM civ_LN_ACCT_DTLS LAD
           WHERE COD_ACCT_STAT = 10
             AND FLG_ACCR_STATUS = 'N'
             AND FLG_MNT_STATUS = 'A';
             
             
             
             
               INSERT /*+enable_prallel_dml parallel(32)*/
        INTO NPA_CONSIS_CHECK
          (CONSIS_NO,
           DATE_RUN,
           COD_ACCT_NO_PRI,
           COD_CUST_ID,
           COD_ACCT_NO_SEC,
           NPA_DATE,
           COD_CRR_CUST,
           COD_CRR_TO,
           FLG_ACCR_STATUS,
           DAT_COOLING_PRD_END,
           COD_ACCT_STAT)
          SELECT /*+parallel(32) FULL(CAM)*/
           10,
            '15-June-25',
           NULL,
           CAM.COD_CUST,
           CAM.COD_ACCT_NO,
           NULL,
           NULL,
           NULL,
           CAM.FLG_ACCR_STATUS,
           NULL,
           CAM.COD_ACCT_STAT
            FROM CIV_CH_ACCT_MAST CAM, CIV_AC_ACCT_CRR_CODE AC
           WHERE COD_ACCT_STAT NOT IN (2, 3, 10, 11, 1, 5)
             AND FLG_ACCR_STATUS = 'S'
             AND AC.COD_ACCT_NO = CAM.COD_ACCT_NO
                --AND AC.COD_CRR_CUST >= 1200
             AND AC.FLG_MNT_STATUS = 'A'
             AND CAM.FLG_MNT_STATUS = 'A';
             
             
             
             
              INSERT /*+enable_prallel_dml parallel(32)*/
        INTO NPA_CONSIS_CHECK
          (CONSIS_NO,
           DATE_RUN,
           COD_ACCT_NO_PRI,
           COD_CUST_ID,
           COD_ACCT_NO_SEC,
           NPA_DATE,
           COD_CRR_CUST,
           COD_CRR_TO,
           FLG_ACCR_STATUS,
           DAT_COOLING_PRD_END)
          SELECT /*+PARALLEL(32)*/
           14,
            '15-June-25',
           NULL,
           COD_CUST_ID,
           COD_ACCT_NO,
           DAT_NPA,
           COD_CRR_CUST,
           COD_CRR_TO,
           NULL,
           NULL
            FROM CIV_AC_ACCT_CRR_CODE AC
           WHERE AC.COD_CRR_CUST >= 1200
             AND AC.DPD_ARREARS = 0
             AND AC.COD_CRR_MVMT_REASON = 'E'
             and cod_Acct_no not in
                 (select cod_acct_no
                    from civ_ac_acct_preferences
                   where cod_plan_id in
                         (select cod_pref_id
                            from CBSFCHOST.ac_preferences
                           where cod_rev_mvmnt <> 2
                             and flg_mnt_status = 'A')
                     and flg_mnt_status = 'A');
                     
                     
                     
                     
                     
                      INSERT /*+enable_prallel_dml parallel(32)*/
        INTO NPA_CONSIS_CHECK
          (CONSIS_NO,
           DATE_RUN,
           COD_ACCT_NO_PRI,
           COD_CUST_ID,
           COD_ACCT_NO_SEC,
           NPA_DATE,
           COD_CRR_CUST,
           COD_CRR_TO,
           FLG_ACCR_STATUS,
           DAT_COOLING_PRD_END)
          SELECT /*+PARALLEL(32)*/
           15,
            '15-June-25',
           NULL,
           COD_CUST_ID,
           COD_ACCT_NO,
           DAT_NPA,
           COD_CRR_FROM, --COD_CRR_CUST,
           COD_CRR_TO,
           NULL,
           NULL
            FROM CBSFCHOST.AC_ACCT_CRR_CODE AC
           WHERE AC.COD_CRR_CUST >= 1200
             AND COD_CRR_TO >= 1200
             AND COD_CRR_TO < COD_CRR_FROM;
             
             
             
             
             
               INSERT /*+enable_prallel_dml parallel(32)*/
        INTO NPA_CONSIS_CHECK
          (CONSIS_NO,
           DATE_RUN,
           COD_ACCT_NO_PRI,
           COD_CUST_ID,
           COD_ACCT_NO_SEC,
           NPA_DATE,
           COD_CRR_CUST,
           COD_CRR_TO,
           FLG_ACCR_STATUS,
           DAT_COOLING_PRD_END)
          SELECT /*+PARALLEL(32)*/
          DISTINCT 16,
                   '15-June-25',
                   NULL,
                   AC1.COD_CUST_ID,
                   AC1.COD_ACCT_NO,
                   DAT_NPA,
                   NULL,
                   NULL,
                   NULL,
                   NULL
            FROM CIV_AC_ACCT_CRR_CODE AC1
           WHERE COD_CUST_ID IN
                 (SELECT COD_CUST_ID
                    FROM (SELECT /*+INDEX(CAC IN_CH_ACCT_CUST_XREF_1)*/
                          DISTINCT AC.COD_CUST_ID,
                                   NVL(AC.DAT_NPA, '01-JAN-1950')
                            FROM CIV_AC_ACCT_CRR_CODE AC, CIV_CH_ACCT_CUST_XREF CAC
                           WHERE NVL(AC.COD_CRR_MVMT_REASON, 'X') <> 'X'
                             AND NOT EXISTS
                           (SELECT 1
                                    FROM CIV_LN_ACCT_DTLS LAD
                                   WHERE LAD.COD_ACCT_NO = AC.COD_ACCT_NO
                                     AND LAD.COD_ACCT_STAT IN (1, 5, 11)
                                     AND FLG_MNT_STATUS = 'A')
                             AND NOT EXISTS
                           (SELECT 1
                                    FROM CIV_CH_ACCT_MAST CAM
                                   WHERE CAM.COD_ACCT_NO = AC.COD_ACCT_NO
                                     AND CAM.COD_ACCT_STAT IN (1, 5)
                                     AND FLG_MNT_STATUS = 'A'
                                     AND COD_PROD IN
                                         (SELECT COD_PROD
                                            FROM CBSFCHOST.CH_PROD_MAST CPM
                                           WHERE CPM.COD_TYP_PROD = 'A'
                                             AND FLG_MNT_STATUS = 'A'))
                             AND AC.FLG_MNT_STATUS = 'A'
                             AND CAC.FLG_MNT_STATUS = 'A'
                             AND AC.COD_ACCT_NO = CAC.COD_ACCT_NO)
                   GROUP BY COD_CUST_ID
                  HAVING COUNT(1) > 1)
             AND NVL(COD_CRR_MVMT_REASON, 'X') <> 'X'
             AND AC1.FLG_MNT_STATUS = 'A'
           ORDER BY COD_CUST_ID;
           
           
           
           
           
           
           
           
           
           
           
           
           
           
           
           
            INSERT /*+enable_prallel_dml parallel(32)*/
        INTO NPA_CONSIS_CHECK
          (CONSIS_NO,
           DATE_RUN,
           COD_ACCT_NO_PRI,
           COD_CUST_ID,
           COD_ACCT_NO_SEC,
           NPA_DATE,
           COD_CRR_CUST,
           COD_CRR_TO,
           FLG_ACCR_STATUS,
           DAT_COOLING_PRD_END)
          SELECT /*+PARALLEL(32)*/
          DISTINCT 17,
                   '15-June-25',
                   NULL,
                   COD_CUST_ID,
                   AC1.COD_ACCT_NO,
                   NULL,
                   COD_CRR_CUST,
                   NULL,
                   NULL,
                   NULL
            FROM CIV_AC_ACCT_CRR_CODE AC1
           WHERE COD_CUST_ID IN
                 (SELECT COD_CUST_ID
                    FROM (SELECT /*+INDEX(CAC IN_CH_ACCT_CUST_XREF_1)*/
                          DISTINCT AC.COD_CUST_ID, AC.COD_CRR_CUST
                            FROM CIV_AC_ACCT_CRR_CODE AC, CIV_CH_ACCT_CUST_XREF CAC
                           WHERE NVL(AC.COD_CRR_MVMT_REASON, 'X') <> 'X'
                             AND CAC.FLG_MNT_STATUS = 'A'
                             AND AC.COD_ACCT_NO = CAC.COD_ACCT_NO
                             AND AC.FLG_MNT_STATUS = 'A'
                             AND NOT EXISTS
                           (SELECT 1
                                    FROM CIV_LN_ACCT_DTLS LAD
                                   WHERE LAD.COD_ACCT_NO = AC.COD_ACCT_NO
                                     AND LAD.COD_ACCT_STAT IN (1, 5, 11)
                                     AND FLG_MNT_STATUS = 'A')
                             AND NOT EXISTS
                           (SELECT 1
                                    FROM CIV_CH_ACCT_MAST CAM
                                   WHERE CAM.COD_ACCT_NO = AC.COD_ACCT_NO
                                     AND CAM.COD_ACCT_STAT IN (1, 5)
                                     AND FLG_MNT_STATUS = 'A'
                                     AND COD_PROD IN
                                         (SELECT COD_PROD
                                            FROM CBSFCHOST.CH_PROD_MAST CPM
                                           WHERE CPM.COD_TYP_PROD = 'A'
                                             AND FLG_MNT_STATUS = 'A')))
                   GROUP BY COD_CUST_ID
                  HAVING COUNT(1) > 1)
             AND NVL(COD_CRR_MVMT_REASON, 'X') <> 'X'
             AND AC1.FLG_MNT_STATUS = 'A'
           ORDER BY COD_CUST_ID;
