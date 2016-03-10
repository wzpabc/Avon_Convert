CREATE OR REPLACE PACKAGE BODY GOMAC.UPK_ODS_GOMAC_LOAD
IS
   PARAMETERS_NULL        EXCEPTION;
   lv_field1              VARCHAR2 (100);
   lv_field2              VARCHAR2 (2);
   lv_field3              VARCHAR2 (1);
   lv_field4              VARCHAR2 (5);
   lv_field5              VARCHAR2 (5);
   lv_field6              VARCHAR2 (5);
   lv_field7              VARCHAR2 (6);
   lv_field8              VARCHAR2 (6);
   lv_field9              VARCHAR2 (50);
   lv_field10             VARCHAR2 (1000);
   lv_field11             VARCHAR2 (200);
   lv_field12             VARCHAR2 (1);
   lv_field15             VARCHAR2 (5);
   lv_field16             VARCHAR2 (5);
   lv_global_profile_id   avgomacprofile.avgomacprofileid%TYPE := 0;
   lv_global_offer_id     ods_offr.offr_id%TYPE;
   lv_sql                 VARCHAR2 (32767) := '';

   TYPE v_prom_claim_id IS TABLE OF VARCHAR2 (100)
                              INDEX BY PLS_INTEGER;

   lv_prom_claim_id       v_prom_claim_id;
   lv_my_prom_claim_id    VARCHAR2 (4000) := '';

   lv_my_own_variable     NUMBER (1) := 0;

   FUNCTION UF_IS_NUMERIC (STR IN VARCHAR2)
      RETURN NUMBER
   IS
   BEGIN
      IF STR IS NULL OR STR = CHR (2)
      THEN
         RETURN 0;
      ELSE
         RETURN TO_NUMBER (STR);
      END IF;
   EXCEPTION
      WHEN VALUE_ERROR
      THEN
         RETURN 0;
   END UF_IS_NUMERIC;

   FUNCTION UF_DOES_EXIST_SOME_STR (STR1 IN VARCHAR2, STR2 IN VARCHAR2)
      RETURN NUMBER
   IS
      V_CNT   NUMBER;
   BEGIN
      SELECT INSTR (STR1,
                    STR2,
                    1,
                    1)
        INTO V_CNT
        FROM DUAL;

      IF V_CNT > 0
      THEN
         RETURN 1;
      ELSE
         RETURN 0;
      END IF;
   END UF_DOES_EXIST_SOME_STR;

   PROCEDURE UPDATE_FSC (v_Market_Code   IN     avbuygetset.dataareaid%TYPE,
                         v_Campaign      IN     avbuygetset.campaign%TYPE,
                         v_linenum       IN     NUMBER,
                         v_altlinenum    IN     NUMBER,
                         v_fsc              OUT VARCHAR2,
                         v_altfsc           OUT VARCHAR2)
   IS
   BEGIN
      IF v_linenum <> 0
      THEN
         BEGIN
            lv_sql :=
                  'select ITEMRELATION from pricedisctable where dataareaid = '''
               || v_Market_Code
               || ''' and AVCAMPAIGNID = '''
               || v_Campaign
               || '''
                   and AVBROCHURELINENUM = '
               || v_linenum
               || '';

            EXECUTE IMMEDIATE lv_sql INTO v_fsc;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_fsc := '0';
         END;
      ELSIF LENGTH (TRIM (v_linenum)) = 0
      THEN
         v_fsc := '0';
      ELSE
         v_fsc := '0';
      END IF;

      IF v_altlinenum <> 0
      THEN
         BEGIN
            lv_sql :=
                  'select ITEMRELATION from pricedisctable where dataareaid = '''
               || v_Market_Code
               || ''' and AVCAMPAIGNID = '''
               || v_Campaign
               || '''
                   and AVBROCHURELINENUM = '
               || v_altlinenum
               || '';

            EXECUTE IMMEDIATE lv_sql INTO v_altfsc;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_altfsc := '0';
         END;
      ELSIF LENGTH (TRIM (v_altlinenum)) = 0
      THEN
         v_altfsc := '0';
      ELSE
         v_altfsc := '0';
      END IF;
   END;

   PROCEDURE UPLOAD_STEPPRICE (
      dMarket_Code    IN avofferheader.dataareaid%TYPE,
      dcampaign       IN avofferheader.campaign%TYPE,
      dodsofferid     IN ods_offr.offr_id%TYPE,
      dodsoffertype   IN ods_claim_offr_type.offer_type%TYPE,
      dofferid        IN avofferheader.OfferId%TYPE,
      --dpromoclaim   IN ods_offr_prfl_prc_pnt.promtn_clm_id%TYPE,
      dgroupnumber    IN avofferheader.groupnumber%TYPE,
      dnr_for         IN ods_offr_sku_line_prc.nr_for_qty%TYPE,
      dProducttype    IN avofferheader.producttype%TYPE)
   IS
      -- Insert the information in the Detail
      -- Create cursor for it
      CURSOR c_offer_detail
      IS
           SELECT DECODE (a.offr_sku_line_id,
                          NULL, 0,
                          '', 0,
                          a.offr_sku_line_id)
                     offr_sku_line_id,
                  DECODE (a.sku_id,  NULL, 0,  '', 0,  a.sku_id) sku_id,
                  DECODE (a.line_nr, NULL, 0, a.line_nr) line_nr,
                  DECODE (a.reg_prc_amt, NULL, 0, a.reg_prc_amt) reg_prc_amt,
                  DECODE (a.sls_prc_amt, NULL, 0, a.sls_prc_amt) sls_prc_amt,
                  DECODE (a.pymt_typ, NULL, CHR (2), a.pymt_typ) pymt_typ,
                  DECODE (a.instlmt_qty, NULL, 0, a.instlmt_qty) instlmt_qty,
                  DECODE (a.offr_prfl_prcpt_id, NULL, 0, a.offr_prfl_prcpt_id)
                     offr_prfl_prcpt_id,
                  DECODE (a.nr_for_qty, NULL, 0, a.nr_for_qty) nr_for_qty,
                  DECODE (a.promtn_clm_id, NULL, 0, a.promtn_clm_id)
                     promtn_clm_id,
                  DECODE (b.offr_id, NULL, 0, b.offr_id) offr_id,
                  DECODE (b.prfl_cd, NULL, 0, b.prfl_cd) prfl_cd,
                  DECODE (b.sls_cls_cd, NULL, CHR (2), b.sls_cls_cd) sls_cls_cd,
                  DECODE (b.sls_cls_desc_txt,
                          NULL, CHR (2),
                          b.sls_cls_desc_txt)
                     sls_cls_desc_txt,
                  DECODE (b.page_nr, NULL, 0, b.page_nr) page_nr
             FROM ods_offr_sku_line_prc a, ods_offr_prfl_prc_pnt b
            WHERE     a.dataareaid = b.dataareaid
                  AND a.dataareaid = dMarket_Code
                  AND a.offr_prfl_prcpt_id = b.offr_prfl_prcpt_id
                  AND b.offr_id = dodsofferid
                  AND a.nr_for_qty = dnr_for
                  AND a.line_nr > 0
         ORDER BY nr_for_qty;

      r_offer_detail       c_offer_detail%ROWTYPE;

      -- Initialize Variables
      lv_group             NUMBER := 1;
      lv_adaptertype       NVARCHAR2 (10) := '12';
      lv_controltag        NVARCHAR2 (3) := 'B';
      lv_linenum           NUMBER;
      lv_price             NUMBER (32, 16);
      lv_regularprice      NUMBER (32, 16);
      lv_accountrelation   NVARCHAR2 (10) := '0';
      lv_producttype       NVARCHAR2 (10);
      lv_promocode         NVARCHAR2 (10) := '0';
      lv_discount          NUMBER (10) := 0;
      lv_itemclass         NVARCHAR2 (10) := CHR (2);
      lv_altlinenum        NUMBER;
      lv_altprice          NUMBER (32, 16);
      lv_altspecialflag    NVARCHAR2 (100) := '0';
      lv_altproduttype     NVARCHAR2 (10) := '0';
      lv_altpromocode      NVARCHAR2 (10) := '0';
      lv_altitemclass      NVARCHAR2 (10) := CHR (2);
      lv_qty               NUMBER := 0;
      lv_createdby         NVARCHAR2 (10) := 'ODS';
      lv_default           NUMBER := 0;
      lv_qualifier         NUMBER (32, 16) := 0;
      lv_udt               NUMBER := 0;
      lv_fsc               NVARCHAR2 (20);
      lv_altfsc            NVARCHAR2 (20);
      lv_altregularprice   NUMBER (32, 16);
      lv_nr_for            NUMBER := 1;
      lv_templinenum       NUMBER := 0;
      lv_tempregprice      NUMBER (32, 16) := 0;
      lv_tempprice         NUMBER (32, 16) := 0;
      lv_tempfsc           NVARCHAR2 (20) := '0';
      lv_setcode           NVARCHAR2 (3) := '0';
   BEGIN
      SELECT TRIM (field12), TRIM (field15)
        INTO lv_field12, lv_field15
        FROM ods_claim_offr_attr
       WHERE dataareaid = dMarket_Code AND ROWNUM = 1;

      -- Set the Group Number
      lv_group := dgroupnumber;

      FOR r_offer_detail IN c_offer_detail
      LOOP
         -- The Line Number is coming with the Check digit and need to remove it
         --lv_linenum      := substr(r_offer_detail.line_nr, 1, 5);

         --if lv_linenum is null then
         --lv_linenum:=0;
         --end if;

         IF lv_field12 = 'Y'
         THEN
            lv_linenum :=
               SUBSTR (r_offer_detail.line_nr,
                       1,
                       (LENGTH (r_offer_detail.line_nr) - 1));
         ELSIF lv_field12 = 'N'
         THEN
            lv_linenum := r_offer_detail.line_nr;
         ELSE
            lv_linenum := r_offer_detail.line_nr;
         END IF;

         lv_regularprice := TO_NUMBER (r_offer_detail.reg_prc_amt);
         lv_fsc := r_offer_detail.sku_id;
         lv_nr_for := r_offer_detail.nr_for_qty;
         lv_price :=
            TRUNC (TO_NUMBER (r_offer_detail.sls_prc_amt) / lv_nr_for, 2);
         lv_producttype := dProducttype;

         -----setcode != C
         BEGIN
            SELECT avsetcode
              INTO lv_setcode
              FROM pricedisctable
             WHERE     AVBROCHURELINENUM = lv_linenum
                   AND dataareaid = dMarket_Code
                   AND AVCAMPAIGNID = dcampaign;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               lv_setcode := '0';
         END;


         IF lv_setcode = 'C'
         THEN
            CONTINUE;
         END IF;

         -- Check condition for Step Prices offer types
         IF dodsoffertype = 'STEP'
         THEN
            lv_templinenum := lv_linenum;
            lv_tempregprice := lv_regularprice;
            lv_tempprice := lv_price;
            lv_tempfsc := lv_fsc;

            -- Check specific Promotion Claims for offers sent from detail
            -- Promotion Claim
            --  114  - ONE FOR.. TWO FOR.. THREE FOR..

            IF r_offer_detail.promtn_clm_id = '114'
            THEN
               -- Insert the Buy
               lv_controltag := 'B';
               lv_altlinenum := 0;
               lv_altprice := 0;
               lv_altfsc := '0';

               --Change in the query to get the Original based on the Alternate Line Number
               BEGIN
                  lv_sql :=
                        'select a.line_nr,
                              to_number(decode(a.reg_prc_amt, null, 0, a.reg_prc_amt)),
                              to_number(decode(a.sls_prc_amt, null, 0, a.sls_prc_amt))
                from ods_offr_sku_line_prc a, ods_offr_prfl_prc_pnt b
               where a.dataareaid = b.dataareaid
                 and a.dataareaid = '''
                     || dMarket_Code
                     || '''
                 and a.offr_prfl_prcpt_id = b.offr_prfl_prcpt_id
                 and b.offr_id = '
                     || dodsofferid
                     || '
                 and a.promtn_clm_id in '
                     || lv_my_prom_claim_id
                     || '
                 and a.sku_id = '
                     || lv_fsc
                     || '
                 and a.line_nr > 0 and rownum = 1';

                  EXECUTE IMMEDIATE lv_sql
                     INTO lv_linenum, lv_regularprice, lv_price;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     lv_linenum := 0;
                     lv_regularprice := 0;
                     lv_price := 0;
                     lv_fsc := '0';
               END;

               --if lv_linenum is null then
               --lv_linenum:=0;
               --end if;

               IF lv_field12 = 'Y' AND lv_linenum <> 0
               THEN
                  lv_linenum :=
                     SUBSTR (lv_linenum, 1, (LENGTH (lv_linenum) - 1));
               /*elsif lv_field12 = 'N' then
                 lv_linenum := r_offer_detail.line_nr;
               else
                 lv_linenum := r_offer_detail.line_nr;*/
               END IF;

               --Update the FSC and ALTFSC values:
               UPDATE_FSC (dMarket_Code,
                           dCampaign,
                           lv_linenum,
                           lv_altlinenum,
                           lv_fsc,
                           lv_altfsc);

               INSERT INTO Avbuygetset (MODIFIEDBY,
                                        CREATEDDATE,
                                        CREATEDBY,
                                        GROUPNUMBER,
                                        AVADAPTERID,
                                        CAMPAIGN,
                                        REGULARPRICE,
                                        PRICE,
                                        CONTROLTAG,
                                        ACCOUNTRELATION,
                                        PRODUCTTYPE,
                                        PROMOTIONCODE,
                                        DISCOUNT,
                                        ITEMCLASS,
                                        ALTPRICE,
                                        SPECIALPROCESSINGFLAG,
                                        ALTPRODUCTTYPE,
                                        ALTPROMOTIONCODE,
                                        ALTITEMCLASS,
                                        UDT,
                                        AVDEFAULT,
                                        ALTSPECIALPROCESSINGFLAG,
                                        LINENUMBER,
                                        ALTLINENUMBER,
                                        ADAPTERTYPE,
                                        AVQUALIFIER,
                                        QUANTITY,
                                        FSC,
                                        ALTFSC,
                                        DATAAREAID,
                                        MODIFIEDDATE)
                    VALUES (lv_createdby,
                            TRUNC (SYSDATE),
                            lv_createdby,
                            lv_group,
                            dofferid,
                            dcampaign,
                            lv_regularprice,
                            lv_price,
                            lv_controltag,
                            lv_accountrelation,
                            lv_producttype,
                            lv_promocode,
                            lv_discount,
                            lv_itemclass,
                            lv_altprice,
                            lv_altspecialflag,
                            lv_altproduttype,
                            lv_altpromocode,
                            lv_altitemclass,
                            lv_udt,
                            lv_default,
                            lv_altspecialflag,
                            lv_linenum,
                            lv_altlinenum,
                            lv_adaptertype,
                            lv_qualifier,
                            lv_qty,
                            lv_fsc,
                            lv_altfsc,
                            dMarket_Code,
                            TRUNC (SYSDATE));

               -- Insert the Get
               lv_controltag := 'G';

               -- First Assign Alternative Products with the values for the Regula Products
               lv_altlinenum := lv_linenum;
               lv_altregularprice := lv_regularprice;
               lv_altprice := lv_price;
               lv_altfsc := lv_fsc;

               -- Second Assign the products at Sepcial Price
               lv_linenum := lv_templinenum;
               lv_regularprice := lv_tempregprice;
               lv_price := lv_tempprice;
               lv_fsc := lv_tempfsc;

               --Update the FSC and ALTFSC values:
               UPDATE_FSC (dMarket_Code,
                           dCampaign,
                           lv_linenum,
                           lv_altlinenum,
                           lv_fsc,
                           lv_altfsc);

               INSERT INTO Avbuygetset (MODIFIEDBY,
                                        CREATEDDATE,
                                        CREATEDBY,
                                        GROUPNUMBER,
                                        AVADAPTERID,
                                        CAMPAIGN,
                                        REGULARPRICE,
                                        PRICE,
                                        CONTROLTAG,
                                        ACCOUNTRELATION,
                                        PRODUCTTYPE,
                                        PROMOTIONCODE,
                                        DISCOUNT,
                                        ITEMCLASS,
                                        ALTPRICE,
                                        SPECIALPROCESSINGFLAG,
                                        ALTPRODUCTTYPE,
                                        ALTPROMOTIONCODE,
                                        ALTITEMCLASS,
                                        UDT,
                                        AVDEFAULT,
                                        ALTSPECIALPROCESSINGFLAG,
                                        LINENUMBER,
                                        ALTLINENUMBER,
                                        ADAPTERTYPE,
                                        AVQUALIFIER,
                                        QUANTITY,
                                        FSC,
                                        ALTFSC,
                                        DATAAREAID,
                                        MODIFIEDDATE)
                    VALUES (lv_createdby,
                            TRUNC (SYSDATE),
                            lv_createdby,
                            lv_group,
                            dofferid,
                            dcampaign,
                            lv_regularprice,
                            lv_price,
                            lv_controltag,
                            lv_accountrelation,
                            lv_producttype,
                            lv_promocode,
                            lv_discount,
                            lv_itemclass,
                            lv_altprice,
                            lv_altspecialflag,
                            lv_altproduttype,
                            lv_altpromocode,
                            lv_altitemclass,
                            lv_udt,
                            lv_default,
                            lv_altspecialflag,
                            lv_linenum,
                            lv_altlinenum,
                            lv_adaptertype,
                            lv_qualifier,
                            lv_qty,
                            lv_fsc,
                            lv_altfsc,
                            dMarket_Code,
                            TRUNC (SYSDATE));
            -- Promotion Claim
            --   2 - LOWEST/BEST PRICE/VALUE
            --  39  - SPECIAL INTRO PRICE
            --  40  - DISCOUNTED PRICE
            --  76  - LAST TIME
            -- 132  - PACK PRICE
            -- 147  - REGULAR PRICE
            -- 160  - REPRESENTATIVE OFFER
            -- 1162  - AUTOMATIC INSTANT/EXPRESS DELIVERY
            -- 1163  - ORDERED INSTANT/EXPRESS DELIVERY
            -- 1180  - PRICED TO GUARANTEE PROFITS FOR REPRESENTATIVES
            ELSIF UF_DOES_EXIST_SOME_STR (lv_my_prom_claim_id,
                                          r_offer_detail.promtn_clm_id) > 0
            THEN
               -- Insert the Buy
               lv_controltag := 'B';
               lv_altlinenum := 0;
               lv_altregularprice := 0;
               lv_altprice := 0;
               lv_altfsc := '0';

               --Update the FSC and ALTFSC values:
               UPDATE_FSC (dMarket_Code,
                           dCampaign,
                           lv_linenum,
                           lv_altlinenum,
                           lv_fsc,
                           lv_altfsc);

               INSERT INTO Avbuygetset (MODIFIEDBY,
                                        CREATEDDATE,
                                        CREATEDBY,
                                        GROUPNUMBER,
                                        AVADAPTERID,
                                        CAMPAIGN,
                                        REGULARPRICE,
                                        PRICE,
                                        CONTROLTAG,
                                        ACCOUNTRELATION,
                                        PRODUCTTYPE,
                                        PROMOTIONCODE,
                                        DISCOUNT,
                                        ITEMCLASS,
                                        ALTPRICE,
                                        SPECIALPROCESSINGFLAG,
                                        ALTPRODUCTTYPE,
                                        ALTPROMOTIONCODE,
                                        ALTITEMCLASS,
                                        UDT,
                                        AVDEFAULT,
                                        ALTSPECIALPROCESSINGFLAG,
                                        LINENUMBER,
                                        ALTLINENUMBER,
                                        ADAPTERTYPE,
                                        AVQUALIFIER,
                                        QUANTITY,
                                        FSC,
                                        ALTFSC,
                                        DATAAREAID,
                                        MODIFIEDDATE)
                    VALUES (lv_createdby,
                            TRUNC (SYSDATE),
                            lv_createdby,
                            lv_group,
                            dofferid,
                            dcampaign,
                            lv_regularprice,
                            lv_price,
                            lv_controltag,
                            lv_accountrelation,
                            lv_producttype,
                            lv_promocode,
                            lv_discount,
                            lv_itemclass,
                            lv_altprice,
                            lv_altspecialflag,
                            lv_altproduttype,
                            lv_altpromocode,
                            lv_altitemclass,
                            lv_udt,
                            lv_default,
                            lv_altspecialflag,
                            lv_linenum,
                            lv_altlinenum,
                            lv_adaptertype,
                            lv_qualifier,
                            lv_qty,
                            lv_fsc,
                            lv_altfsc,
                            dMarket_Code,
                            TRUNC (SYSDATE));

               -- Insert the Get
               lv_controltag := 'G';
               lv_altlinenum := lv_linenum;
               lv_altprice := lv_price;
               lv_altfsc := lv_fsc;

               --Update the FSC and ALTFSC values:
               UPDATE_FSC (dMarket_Code,
                           dCampaign,
                           lv_linenum,
                           lv_altlinenum,
                           lv_fsc,
                           lv_altfsc);

               --Change in the query to get the Original based on the Alternate Line Number

               INSERT INTO Avbuygetset (MODIFIEDBY,
                                        CREATEDDATE,
                                        CREATEDBY,
                                        GROUPNUMBER,
                                        AVADAPTERID,
                                        CAMPAIGN,
                                        REGULARPRICE,
                                        PRICE,
                                        CONTROLTAG,
                                        ACCOUNTRELATION,
                                        PRODUCTTYPE,
                                        PROMOTIONCODE,
                                        DISCOUNT,
                                        ITEMCLASS,
                                        ALTPRICE,
                                        SPECIALPROCESSINGFLAG,
                                        ALTPRODUCTTYPE,
                                        ALTPROMOTIONCODE,
                                        ALTITEMCLASS,
                                        UDT,
                                        AVDEFAULT,
                                        ALTSPECIALPROCESSINGFLAG,
                                        LINENUMBER,
                                        ALTLINENUMBER,
                                        ADAPTERTYPE,
                                        AVQUALIFIER,
                                        QUANTITY,
                                        FSC,
                                        ALTFSC,
                                        DATAAREAID,
                                        MODIFIEDDATE)
                    VALUES (lv_createdby,
                            TRUNC (SYSDATE),
                            lv_createdby,
                            lv_group,
                            dofferid,
                            dcampaign,
                            lv_regularprice,
                            lv_price,
                            lv_controltag,
                            lv_accountrelation,
                            lv_producttype,
                            lv_promocode,
                            lv_discount,
                            lv_itemclass,
                            lv_altprice,
                            lv_altspecialflag,
                            lv_altproduttype,
                            lv_altpromocode,
                            lv_altitemclass,
                            lv_udt,
                            lv_default,
                            lv_altspecialflag,
                            lv_linenum,
                            lv_altlinenum,
                            lv_adaptertype,
                            lv_qualifier,
                            lv_qty,
                            lv_fsc,
                            lv_altfsc,
                            dMarket_Code,
                            TRUNC (SYSDATE));
            --if (r_offer_detail.promtn_clm_id = '1182')  or (r_offer_detail.promtn_clm_id = '170') then
            END IF;
         --if dodsoffertype = 'STEP' then
         END IF;
      END LOOP;

      COMMIT;
   END;                                                     --End-Of-This-Code

   PROCEDURE UPLOAD_STEPPRICEHDR (
      dMarket_Code    IN avofferheader.dataareaid%TYPE,
      dcampaign       IN avofferheader.campaign%TYPE,
      dodsofferid     IN ods_offr.offr_id%TYPE,
      dodsoffertype   IN ods_claim_offr_type.offer_type%TYPE,
      dofferid        IN avofferheader.OfferId%TYPE,
      dpromoclaim     IN ods_offr_prfl_prc_pnt.promtn_clm_id%TYPE,
      dodsofferdesc   IN ods_offr.OFFR_DESC_TXT%TYPE,
      dGroupNum       IN avofferheader.groupnumber%TYPE,
      dQualifytype    IN avofferheader.Qualifytype%TYPE,
      dProducttype    IN avofferheader.producttype%TYPE,
      dPriority       IN avofferheader.priority%TYPE,
      dOrmore         IN avofferheader.ormore%TYPE,
      dEnabled        IN avofferheader.enabled%TYPE,
      dCombine        IN avofferheader.combine%TYPE,
      dBackout        IN avofferheader.backout%TYPE,
      dBackorder      IN avofferheader.backorder%TYPE,
      dZerostep       IN avofferheader.avzerostep%TYPE,
      dUseop          IN ods_claim_offr_type.use_online_promotion%TYPE,
      dLimitbase      IN avofferheader.avlimitbase%TYPE)
   IS
      CURSOR c_offer_step
      IS
           SELECT DISTINCT a.nr_for_qty no_for
             FROM ods_offr_sku_line_prc a, ods_offr_prfl_prc_pnt b
            WHERE     a.dataareaid = b.dataareaid
                  AND a.dataareaid = dMarket_Code
                  AND a.offr_prfl_prcpt_id = b.offr_prfl_prcpt_id
                  AND b.offr_id = dodsofferid
         ORDER BY a.nr_for_qty;

      r_offer_step       c_offer_step%ROWTYPE;

      -- Initialize Variables
      --lv2_sqlmsg       VARCHAR2(255);
      lv_offerid         NUMBER;
      lv_group           NUMBER;
      lv_description     VARCHAR2 (150);
      lv_enabled         VARCHAR2 (5);
      lv_priority        NUMBER;
      lv_qualifytype     VARCHAR2 (10);
      --lv_offertype     NVARCHAR2(10);
      lv_offertype       VARCHAR2 (20);
      lv_buyqualifier    NUMBER;
      lv_getqty          NUMBER;
      lv_producttype     NUMBER;
      lv_combine         VARCHAR2 (10);
      lv_backorder       VARCHAR2 (5);
      lv_backout         NUMBER;
      lv_ormore          VARCHAR2 (5);
      lv_default         NUMBER := 0;
      lv_distinct        NUMBER := 0;
      lv_zerostep        NUMBER;
      lv_offerflag       NUMBER := 0;
      lv_profiletype     NUMBER := 0;
      lv_limitbase       NUMBER;
      lv_limitqty        NUMBER := 0;
      lv_scheduletype    NUMBER := 0;
      lv_usebuysetone    NUMBER := 0;
      lv_byofferids      VARCHAR2 (10) := CHR (2);
      lv_applyonce       NUMBER := 0;
      lv_interfactive    NUMBER := 0;
      lv_extdesc         VARCHAR2 (50) := CHR (2);
      lv_usetagupdate    NUMBER := 0;
      lv_bypassmonitem   NUMBER := 0;
      lv_exceedoption    NUMBER := 0;
      lv_distedefault    NUMBER := 0;
      lv_udt             NUMBER := 0;
      lv_useop           NUMBER;
      lv_opdesc          VARCHAR2 (50) := CHR (2);
      lv_opcaption       VARCHAR2 (50) := CHR (2);
      lv_oppriority      NUMBER := 0;
      lv_opcategory      NUMBER := 0;
      lv_opmessage       VARCHAR2 (50) := CHR (2);
      lv_opmessagedual   NUMBER := 0;
      lv_opmesageget     VARCHAR2 (50) := CHR (2);
      lv_nodefaultitem   NUMBER := 0;
      lv_odsofferid      NUMBER := 0;
      lv_promoclaim      VARCHAR2 (10);
      --lv_offr_ntes_txt ods_offr.offr_desc_txt%type;

      --lv_countstep NUMBER := 0;
      --lv_recid         NUMBER := 0;
      lv_nofor           NUMBER := 0;
      lv_loop_index      NUMBER := 0;
   BEGIN
      --lv_countstep := 1;

      FOR r_offer_step IN c_offer_step
      LOOP
         lv_loop_index := lv_loop_index + 1;

         -- Get values into variables
         lv_odsofferid := dodsofferid;
         lv_offertype := dodsoffertype;
         lv_promoclaim := dpromoclaim;
         lv_offerid := dofferid;
         lv_group := dGroupNum;
         lv_qualifytype := dQualifytype;
         lv_producttype := dProducttype;
         lv_priority := dPriority;
         lv_ormore := dOrmore;
         lv_enabled := dEnabled;
         lv_combine := dCombine;
         lv_backout := dBackout;
         lv_backorder := dBackorder;
         lv_zerostep := dZerostep;
         lv_useop := dUseop;
         lv_limitbase := dLimitbase;

         -- Variables impacted
         -- Process to insert the Offer Header information
         lv_buyqualifier := r_offer_step.no_for;
         lv_getqty := r_offer_step.no_for;
         lv_nofor := r_offer_step.no_for;

         lv_description :=
            'C' || SUBSTR (dcampaign, 5, 2) || 'OF' || ' ' || dodsofferdesc;
         lv_description := SUBSTR (lv_description, 1, 50);

         lv_group := lv_group + (lv_loop_index - 1);

         INSERT INTO avOfferHeader (dataareaid,
                                    OfferId,
                                    GroupNumber,
                                    Description,
                                    Enabled,
                                    Priority,
                                    Campaign,
                                    DateFrom,
                                    DateTo,
                                    QualifyType,
                                    OfferType,
                                    BuyQualifier,
                                    GetQuantity,
                                    ProductType,
                                    Combine,
                                    BackOrder,
                                    Backout,
                                    OrMore,
                                    avDefault,
                                    avDistinct,
                                    AVZEROSTEP,
                                    OFFERFLAG,
                                    PROFILETYPE,
                                    AVLIMITBASE,
                                    AVLIMITQTY,
                                    AVSCHEDULETYPE,
                                    AVUSEBUYSETONCE,
                                    AVBYOFFERIDS,
                                    AVAPPLYONCE,
                                    AVINTERACTIVE,
                                    AVEXTDESC,
                                    AVUSETAGUPDATE,
                                    AVBYPASSMONITOREDITEMS,
                                    AVEXCEEDOPTION,
                                    AVdistinctDefault,
                                    AVUDT,
                                    AVUSEOP,
                                    AVOPDESC,
                                    AVOPCAPTION,
                                    AVOPPRIORITY,
                                    AVOPCATEGORY,
                                    AVOPMESSAGE,
                                    AVOPMESSAGEDUAL,
                                    AVOPMESSAGEGET,
                                    AVNODEFAULTITEM)
              VALUES (dMarket_Code,
                      lv_offerid,
                      lv_group,
                      lv_description,
                      lv_enabled,
                      lv_priority,
                      dcampaign,
                      TRUNC (SYSDATE),
                      TRUNC (SYSDATE),
                      lv_qualifytype,
                      lv_offertype,
                      lv_buyqualifier,
                      lv_getqty,
                      lv_producttype,
                      lv_combine,
                      lv_backorder,
                      lv_backout,
                      lv_ormore,
                      lv_default,
                      lv_distinct,
                      lv_zerostep,
                      lv_offerflag,
                      lv_profiletype,
                      lv_limitbase,
                      lv_limitqty,
                      lv_scheduletype,
                      lv_usebuysetone,
                      lv_byofferids,
                      lv_applyonce,
                      lv_interfactive,
                      lv_extdesc,
                      lv_usetagupdate,
                      lv_bypassmonitem,
                      lv_exceedoption,
                      lv_distedefault,
                      lv_udt,
                      lv_useop,
                      lv_opdesc,
                      lv_opcaption,
                      lv_oppriority,
                      lv_opcategory,
                      lv_opmessage,
                      lv_opmessagedual,
                      lv_opmesageget,
                      lv_nodefaultitem);

         -- Call offer detail Step Price procedure
         UPLOAD_STEPPRICE (dMarket_Code,
                           dcampaign,
                           lv_odsofferid,
                           lv_offertype,
                           lv_offerid,
                           --lv_promoclaim,
                           lv_group,
                           lv_nofor,
                           lv_producttype);
      --lv_countstep := lv_countstep + 1;

      END LOOP;

      COMMIT;
   END;                                                     --End-Of-This-Code

   PROCEDURE UPLOAD_DETAIL (
      dMarket_Code     IN avofferheader.dataareaid%TYPE,
      dcampaign        IN avofferheader.campaign%TYPE,
      dodsofferid      IN ods_offr.offr_id%TYPE,
      dodsoffertype    IN ods_claim_offr_type.offer_type%TYPE,
      dofferid         IN avofferheader.OfferId%TYPE,
      dpromoclaim      IN ods_offr_prfl_prc_pnt.promtn_clm_id%TYPE,
      dOffr_ntes_txt   IN ods_offr.OFFR_NTES_TXT%TYPE)
   IS
      -- Insert the information in the Detail
      -- Create cursor for it
      CURSOR c_offer_detail
      IS
           SELECT DISTINCT
                  DECODE (a.offr_sku_line_id, NULL, 0, a.offr_sku_line_id)
                     offr_sku_line_id,
                  DECODE (a.sku_id, NULL, 0, a.sku_id) sku_id,
                  DECODE (a.line_nr, NULL, 0, a.line_nr) line_nr,
                  DECODE (a.reg_prc_amt, NULL, 0, a.reg_prc_amt) reg_prc_amt,
                  DECODE (a.sls_prc_amt, NULL, 0, a.sls_prc_amt) sls_prc_amt,
                  DECODE (a.pymt_typ, NULL, CHR (2), a.pymt_typ) pymt_typ,
                  DECODE (a.instlmt_qty, NULL, 0, a.instlmt_qty) instlmt_qty,
                  DECODE (a.offr_prfl_prcpt_id, NULL, 0, a.offr_prfl_prcpt_id)
                     offr_prfl_prcpt_id,
                  DECODE (a.nr_for_qty, NULL, 0, a.nr_for_qty) nr_for_qty,
                  DECODE (a.promtn_clm_id, NULL, 0, a.promtn_clm_id)
                     promtn_clm_id,
                  DECODE (b.offr_id, NULL, 0, b.offr_id) offr_id,
                  DECODE (b.prfl_cd, NULL, 0, b.prfl_cd) prfl_cd,
                  DECODE (b.sls_cls_cd, NULL, CHR (2), b.sls_cls_cd) sls_cls_cd,
                  DECODE (b.sls_cls_desc_txt,
                          NULL, CHR (2),
                          b.sls_cls_desc_txt)
                     sls_cls_desc_txt,
                  DECODE (b.page_nr, NULL, 0, b.page_nr) page_nr,
                  DECODE (c.LIMIT_QTY, '', 0, c.LIMIT_QTY) LIMIT_QTY,
                  DECODE (c.use_online_promotion,
                          '', '0',
                          c.use_online_promotion)
                     use_online_promotion,
                  DECODE (c.groupnum, '', 1, c.groupnum) groupnum,
                  DECODE (c.product_type, '', '0', c.product_type) product_type,
                  DECODE (c.qual_type, '', 'UNITS', c.qual_type) qual_type
             FROM ods_offr_sku_line_prc a,
                  ods_offr_prfl_prc_pnt b,
                  ods_claim_offr_type c
            WHERE     a.dataareaid = b.dataareaid
                  AND a.dataareaid = c.dataareaid
                  AND b.promtn_clm_id = c.promotion_claim_id
                  AND a.dataareaid = dMarket_Code
                  AND a.offr_prfl_prcpt_id = b.offr_prfl_prcpt_id
                  --and c.offer_type <> 'ITEM'
                  AND b.offr_id = dodsofferid
                  AND a.line_nr > 0
         ORDER BY offr_prfl_prcpt_id;

      r_offer_detail            c_offer_detail%ROWTYPE;

      -- Initialize Variables
      lv_group                  NUMBER;
      lv_adaptertype            NVARCHAR2 (10) := '12';
      lv_controltag             NVARCHAR2 (3) := 'B';
      lv_linenum                NUMBER;
      lv_price                  NUMBER (32, 16);
      lv_regularprice           NUMBER (32, 16);
      lv_accountrelation        NVARCHAR2 (10) := '0';
      lv_produttype             NVARCHAR2 (10);
      lv_promocode              NVARCHAR2 (10) := '0';
      lv_discount               NUMBER (10) := 0;
      lv_itemclass              NVARCHAR2 (10) := CHR (2);
      lv_altlinenum             NUMBER;
      lv_altprice               NUMBER (32, 16);
      lv_altspecialflag         NVARCHAR2 (100) := '0';
      lv_altproduttype          NVARCHAR2 (10) := '0';
      lv_altpromocode           NVARCHAR2 (10) := '0';
      lv_altitemclass           NVARCHAR2 (10) := CHR (2);
      lv_qty                    NUMBER := 0;
      lv_getqty                 NUMBER;
      lv_createdby              NVARCHAR2 (10) := 'ODS';
      lv_default                NUMBER := 0;
      lv_qualifier              NUMBER (32, 16) := 0;
      lv_udt                    NUMBER := 0;
      lv_fsc                    NVARCHAR2 (20);
      lv_altfsc                 NVARCHAR2 (20);
      lv_countfree              NUMBER := 0;
      lv_nr_for                 NUMBER := 1;
      lv_altregularprice        NUMBER (32, 16);
      lv_modified_time          NUMBER;
      lv_description            VARCHAR2 (150);
      lv_seqno                  VARCHAR2 (15) := 'GMID';
      lv_useop                  ods_claim_offr_type.use_online_promotion%TYPE;

      lv_convertid              NUMBER;
      lv_productlimitid         NUMBER;

      lv_price_for_compare1     NUMBER (32, 16); --for preparing to insert into AvConvertLineNum,need to compare according to SM.
      lv_price_for_compare2     NUMBER (32, 16); --for preparing to insert into AvConvertLineNum,need to compare according to SM.
      lv_line_nr_for_compare1   NUMBER;
      lv_line_nr_for_compare2   NUMBER;
      lv_convertlinenum         avconvertlinenum.avconvertlinenum%TYPE;
      lv_brochurelinenum        avconvertlinenum.avbrochurelinenum%TYPE;

      lv_myfound                NUMBER (10) := 0;
      --lv_mycnt number := 0;
      lv_local_cnt              NUMBER (10) := 0;
      lv_setcode                NVARCHAR2 (3) := '0';
   BEGIN
      SELECT TRIM (field12),
             TRIM (field15),
             TRIM (field6),
             TRIM (field7),
             TRIM (field8)
        INTO lv_field12,
             lv_field15,
             lv_field6,
             lv_field7,
             lv_field8
        FROM ods_claim_offr_attr
       WHERE dataareaid = dMarket_Code AND ROWNUM = 1;

      FOR r_offer_detail IN c_offer_detail
      LOOP
         -- The Line Number is coming with the Check digit and need to remove it
         --lv_linenum      := substr(r_offer_detail.line_nr, 1, 5);
         --if lv_linenum is null then
         --lv_linenum:=0;
         --end if;

         IF lv_field12 = 'Y'
         THEN
            lv_linenum :=
               SUBSTR (r_offer_detail.line_nr,
                       1,
                       (LENGTH (r_offer_detail.line_nr) - 1));
         ELSIF lv_field12 = 'N'
         THEN
            lv_linenum := r_offer_detail.line_nr;
         ELSE
            lv_linenum := r_offer_detail.line_nr;
         END IF;

         lv_price := TO_NUMBER (r_offer_detail.sls_prc_amt);
         lv_regularprice := TO_NUMBER (r_offer_detail.reg_prc_amt);
         lv_fsc := r_offer_detail.sku_id;
         lv_nr_for := r_offer_detail.nr_for_qty;

         lv_produttype := r_offer_detail.product_type;
         lv_group := r_offer_detail.groupnum;
         lv_useop := r_offer_detail.use_online_promotion;

         lv_convertid := GOMAC.UF_GET_GM_SEQNO (dMarket_Code, lv_seqno);

         ---setcode != C
         BEGIN
            SELECT avsetcode
              INTO lv_setcode
              FROM pricedisctable
             WHERE     AVBROCHURELINENUM = lv_linenum
                   AND dataareaid = dMarket_Code
                   AND AVCAMPAIGNID = dcampaign;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               lv_setcode := '0';
         END;

         IF lv_setcode = 'C'
         THEN
            CONTINUE;
         END IF;

         -- Check condition for Generate Free offer types
         IF dodsoffertype = 'GFRE'
         THEN
            IF (dpromoclaim = '1181' OR dpromoclaim = '169')
            THEN
               IF    (r_offer_detail.promtn_clm_id = '1181')
                  OR (r_offer_detail.promtn_clm_id = '169')
               THEN
                  lv_controltag := 'G';
                  lv_altlinenum := 0;
                  lv_altprice := 0;
                  lv_altfsc := '0';
                  lv_countfree := lv_countfree + 1;

                  --Update the FSC and ALTFSC values:
                  UPDATE_FSC (dMarket_Code,
                              dCampaign,
                              lv_linenum,
                              lv_altlinenum,
                              lv_fsc,
                              lv_altfsc);

                  --lv_altregularprice := lv_regularprice;

                  --lv_global_offer_id := dodsofferid;
                  IF lv_price = 0
                  THEN
                     INSERT INTO Avbuygetset (MODIFIEDBY,
                                              CREATEDDATE,
                                              CREATEDBY,
                                              GROUPNUMBER,
                                              AVADAPTERID,
                                              CAMPAIGN,
                                              REGULARPRICE,
                                              PRICE,
                                              CONTROLTAG,
                                              ACCOUNTRELATION,
                                              PRODUCTTYPE,
                                              PROMOTIONCODE,
                                              DISCOUNT,
                                              ITEMCLASS,
                                              ALTPRICE,
                                              SPECIALPROCESSINGFLAG,
                                              ALTPRODUCTTYPE,
                                              ALTPROMOTIONCODE,
                                              ALTITEMCLASS,
                                              UDT,
                                              AVDEFAULT,
                                              ALTSPECIALPROCESSINGFLAG,
                                              LINENUMBER,
                                              ALTLINENUMBER,
                                              ADAPTERTYPE,
                                              AVQUALIFIER,
                                              QUANTITY,
                                              FSC,
                                              ALTFSC,
                                              DATAAREAID,
                                              MODIFIEDDATE)
                          VALUES (lv_createdby,
                                  TRUNC (SYSDATE),
                                  lv_createdby,
                                  lv_group,
                                  dofferid,
                                  dcampaign,
                                  lv_regularprice,
                                  lv_price,
                                  lv_controltag,
                                  lv_accountrelation,
                                  lv_produttype,
                                  lv_promocode,
                                  lv_discount,
                                  lv_itemclass,
                                  lv_altprice,
                                  lv_altspecialflag,
                                  lv_altproduttype,
                                  lv_altpromocode,
                                  lv_altitemclass,
                                  lv_udt,
                                  lv_default,
                                  lv_altspecialflag,
                                  lv_linenum,
                                  lv_altlinenum,
                                  lv_adaptertype,
                                  lv_qualifier,
                                  lv_qty,
                                  lv_fsc,
                                  lv_altfsc,
                                  dMarket_Code,
                                  TRUNC (SYSDATE));
                  ELSE
                     SELECT COUNT (*)
                       INTO lv_local_cnt
                       FROM (SELECT *
                               FROM ods_offr_sku_line_prc a,
                                    ods_offr_prfl_prc_pnt b,
                                    ods_claim_offr_type c
                              WHERE     a.dataareaid = b.dataareaid
                                    AND a.dataareaid = c.dataareaid
                                    AND b.promtn_clm_id =
                                           c.promotion_claim_id
                                    AND a.dataareaid = dMarket_Code
                                    AND a.offr_prfl_prcpt_id =
                                           b.offr_prfl_prcpt_id
                                    AND a.line_nr > 0
                                    AND b.offr_id = dodsofferid     --dofferid
                                    AND b.promtn_clm_id IN (1181, 169)
                                    AND TO_NUMBER (
                                           DECODE (a.sls_prc_amt,
                                                   NULL, 0,
                                                   a.sls_prc_amt)) = 0);

                     IF lv_local_cnt = 0
                     THEN
                        DELETE FROM avofferheader
                              WHERE     dataareaid = dMarket_Code
                                    AND offerid = dofferid;

                        lv_my_own_variable := 1;
                     END IF;
                  END IF;
               -- Update the information for the Offer Header with Buy Set 2
               /*If lv_countfree = 2 then
                 UPDATE avOfferHeader
                    SET BuyQualifier = 2
                  where dataareaid = dMarket_Code
                    and offerid = dofferid;

               End if;*/

               -- Promotion Claim
               --   2 - LOWEST/BEST PRICE/VALUE
               --  39  - SPECIAL INTRO PRICE
               --  40  - DISCOUNTED PRICE
               --  76  - LAST TIME
               -- 132  - PACK PRICE
               -- 147  - REGULAR PRICE
               -- 160  - REPRESENTATIVE OFFER
               -- 1162  - AUTOMATIC INSTANT/EXPRESS DELIVERY
               -- 1163  - ORDERED INSTANT/EXPRESS DELIVERY
               -- 1180  - PRICED TO GUARANTEE PROFITS FOR REPRESENTATIVES
               ELSIF UF_DOES_EXIST_SOME_STR (lv_my_prom_claim_id,
                                             r_offer_detail.Promtn_Clm_Id) >
                        0
               THEN
                  lv_controltag := 'B';
                  lv_altlinenum := 0;
                  lv_altprice := 0;
                  lv_altfsc := '0';

                  --Update the FSC and ALTFSC values:
                  UPDATE_FSC (dMarket_Code,
                              dCampaign,
                              lv_linenum,
                              lv_altlinenum,
                              lv_fsc,
                              lv_altfsc);

                  INSERT INTO Avbuygetset (MODIFIEDBY,
                                           CREATEDDATE,
                                           CREATEDBY,
                                           GROUPNUMBER,
                                           AVADAPTERID,
                                           CAMPAIGN,
                                           REGULARPRICE,
                                           PRICE,
                                           CONTROLTAG,
                                           ACCOUNTRELATION,
                                           PRODUCTTYPE,
                                           PROMOTIONCODE,
                                           DISCOUNT,
                                           ITEMCLASS,
                                           ALTPRICE,
                                           SPECIALPROCESSINGFLAG,
                                           ALTPRODUCTTYPE,
                                           ALTPROMOTIONCODE,
                                           ALTITEMCLASS,
                                           UDT,
                                           AVDEFAULT,
                                           ALTSPECIALPROCESSINGFLAG,
                                           LINENUMBER,
                                           ALTLINENUMBER,
                                           ADAPTERTYPE,
                                           AVQUALIFIER,
                                           QUANTITY,
                                           FSC,
                                           ALTFSC,
                                           DATAAREAID,
                                           MODIFIEDDATE)
                       VALUES (lv_createdby,
                               TRUNC (SYSDATE),
                               lv_createdby,
                               lv_group,
                               dofferid,
                               dcampaign,
                               lv_regularprice,
                               lv_price,
                               lv_controltag,
                               lv_accountrelation,
                               lv_produttype,
                               lv_promocode,
                               lv_discount,
                               lv_itemclass,
                               lv_altprice,
                               lv_altspecialflag,
                               lv_altproduttype,
                               lv_altpromocode,
                               lv_altitemclass,
                               lv_udt,
                               lv_default,
                               lv_altspecialflag,
                               lv_linenum,
                               lv_altlinenum,
                               lv_adaptertype,
                               lv_qualifier,
                               lv_qty,
                               lv_fsc,
                               lv_altfsc,
                               dMarket_Code,
                               TRUNC (SYSDATE));
               END IF;
            ELSIF dpromoclaim = '1175'
            THEN
               IF r_offer_detail.promtn_clm_id = '1175'
               THEN
                  lv_controltag := 'G';
                  lv_altlinenum := 0;
                  lv_altprice := 0;
                  lv_altfsc := '0';

                  --lv_altregularprice := lv_regularprice;
                  --lv_countfree  := lv_countfree + 1;

                  -- Set the Price to Zero for the Free item
                  --lv_price := 0;

                  --Update the FSC and ALTFSC values:
                  UPDATE_FSC (dMarket_Code,
                              dCampaign,
                              lv_linenum,
                              lv_altlinenum,
                              lv_fsc,
                              lv_altfsc);

                  INSERT INTO Avbuygetset (MODIFIEDBY,
                                           CREATEDDATE,
                                           CREATEDBY,
                                           GROUPNUMBER,
                                           AVADAPTERID,
                                           CAMPAIGN,
                                           REGULARPRICE,
                                           PRICE,
                                           CONTROLTAG,
                                           ACCOUNTRELATION,
                                           PRODUCTTYPE,
                                           PROMOTIONCODE,
                                           DISCOUNT,
                                           ITEMCLASS,
                                           ALTPRICE,
                                           SPECIALPROCESSINGFLAG,
                                           ALTPRODUCTTYPE,
                                           ALTPROMOTIONCODE,
                                           ALTITEMCLASS,
                                           UDT,
                                           AVDEFAULT,
                                           ALTSPECIALPROCESSINGFLAG,
                                           LINENUMBER,
                                           ALTLINENUMBER,
                                           ADAPTERTYPE,
                                           AVQUALIFIER,
                                           QUANTITY,
                                           FSC,
                                           ALTFSC,
                                           DATAAREAID,
                                           MODIFIEDDATE)
                       VALUES (lv_createdby,
                               TRUNC (SYSDATE),
                               lv_createdby,
                               lv_group,
                               dofferid,
                               dcampaign,
                               lv_regularprice,
                               lv_price,
                               lv_controltag,
                               lv_accountrelation,
                               lv_produttype,
                               lv_promocode,
                               lv_discount,
                               lv_itemclass,
                               lv_altprice,
                               lv_altspecialflag,
                               lv_altproduttype,
                               lv_altpromocode,
                               lv_altitemclass,
                               lv_udt,
                               lv_default,
                               lv_altspecialflag,
                               lv_linenum,
                               lv_altlinenum,
                               lv_adaptertype,
                               lv_qualifier,
                               lv_qty,
                               lv_fsc,
                               lv_altfsc,
                               dMarket_Code,
                               TRUNC (SYSDATE));
               END IF;
            END IF;
         END IF;

         IF dodsoffertype = 'SPRC'
         THEN
            IF dpromoclaim = '1185'
            THEN
               IF r_offer_detail.promtn_clm_id = '1185'
               THEN
                  lv_controltag := 'G';
                  lv_altfsc := lv_fsc;
                  lv_altlinenum := lv_linenum;
                  lv_altprice := lv_price;
                  lv_altregularprice := lv_regularprice;

                  BEGIN
                     lv_sql :=
                           'select a.line_nr,
                                to_number(decode(a.reg_prc_amt, null, 0, a.reg_prc_amt)),
                                to_number(decode(a.sls_prc_amt, null, 0, a.sls_prc_amt))
                from ods_offr_sku_line_prc a, ods_offr_prfl_prc_pnt b
               where a.dataareaid = b.dataareaid
                 and a.dataareaid = '''
                        || dMarket_Code
                        || '''
                 and a.offr_prfl_prcpt_id = b.offr_prfl_prcpt_id
                 and b.offr_id = '
                        || dodsofferid
                        || '
                 and a.promtn_clm_id in '
                        || lv_my_prom_claim_id
                        || '
                 and a.sku_id = '
                        || lv_fsc
                        || '
                 and a.line_nr > 0 and rownum = 1';

                     EXECUTE IMMEDIATE lv_sql
                        INTO lv_linenum, lv_regularprice, lv_price;
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        lv_myfound := 1;
                        lv_linenum := lv_altlinenum;
                        lv_price := lv_altprice;
                        lv_regularprice := lv_altregularprice;
                        lv_fsc := lv_altfsc;
                        lv_altlinenum := 0;
                        lv_altprice := 0;
                        lv_altfsc := '0';
                  END;

                  --if lv_linenum is null then
                  --lv_linenum:=0;
                  --end if;

                  IF lv_field12 = 'Y' AND lv_linenum <> 0 AND lv_myfound = 0
                  THEN
                     lv_linenum :=
                        SUBSTR (lv_linenum, 1, (LENGTH (lv_linenum) - 1));
                  /* elsif lv_field12 = 'N' then
                    lv_linenum := r_offer_detail.line_nr;
                  else
                    lv_linenum := r_offer_detail.line_nr;*/
                  END IF;

                  lv_price_for_compare1 := lv_altprice;
                  lv_line_nr_for_compare1 := lv_altlinenum;

                  lv_myfound := 0;

                  --Update the FSC and ALTFSC values:
                  UPDATE_FSC (dMarket_Code,
                              dCampaign,
                              lv_linenum,
                              lv_altlinenum,
                              lv_fsc,
                              lv_altfsc);

                  INSERT INTO Avbuygetset (MODIFIEDBY,
                                           CREATEDDATE,
                                           CREATEDBY,
                                           GROUPNUMBER,
                                           AVADAPTERID,
                                           CAMPAIGN,
                                           REGULARPRICE,
                                           PRICE,
                                           CONTROLTAG,
                                           ACCOUNTRELATION,
                                           PRODUCTTYPE,
                                           PROMOTIONCODE,
                                           DISCOUNT,
                                           ITEMCLASS,
                                           ALTPRICE,
                                           SPECIALPROCESSINGFLAG,
                                           ALTPRODUCTTYPE,
                                           ALTPROMOTIONCODE,
                                           ALTITEMCLASS,
                                           UDT,
                                           AVDEFAULT,
                                           ALTSPECIALPROCESSINGFLAG,
                                           LINENUMBER,
                                           ALTLINENUMBER,
                                           ADAPTERTYPE,
                                           AVQUALIFIER,
                                           QUANTITY,
                                           FSC,
                                           ALTFSC,
                                           DATAAREAID,
                                           MODIFIEDDATE)
                       VALUES (lv_createdby,
                               TRUNC (SYSDATE),
                               lv_createdby,
                               lv_group,
                               dofferid,
                               dcampaign,
                               lv_regularprice,
                               lv_price,
                               lv_controltag,
                               lv_accountrelation,
                               lv_produttype,
                               lv_promocode,
                               lv_discount,
                               lv_itemclass,
                               lv_altprice,
                               lv_altspecialflag,
                               lv_altproduttype,
                               lv_altpromocode,
                               lv_altitemclass,
                               lv_udt,
                               lv_default,
                               lv_altspecialflag,
                               lv_linenum,
                               lv_altlinenum,
                               lv_adaptertype,
                               lv_qualifier,
                               lv_qty,
                               lv_fsc,
                               lv_altfsc,
                               dMarket_Code,
                               TRUNC (SYSDATE));
               -- Promotion Claim
               --   2 - LOWEST/BEST PRICE/VALUE
               --  39  - SPECIAL INTRO PRICE
               --  40  - DISCOUNTED PRICE
               --  76  - LAST TIME
               -- 132  - PACK PRICE
               -- 147  - REGULAR PRICE
               -- 160  - REPRESENTATIVE OFFER
               -- 1162  - AUTOMATIC INSTANT/EXPRESS DELIVERY
               -- 1163  - ORDERED INSTANT/EXPRESS DELIVERY
               -- 1180  - PRICED TO GUARANTEE PROFITS FOR REPRESENTATIVES
               ELSIF UF_DOES_EXIST_SOME_STR (lv_my_prom_claim_id,
                                             r_offer_detail.promtn_clm_id) >
                        0
               THEN
                  lv_controltag := 'B';
                  lv_altlinenum := 0;
                  lv_altprice := 0;
                  lv_altfsc := '0';

                  lv_price_for_compare2 := lv_price;
                  lv_line_nr_for_compare2 := lv_linenum;

                  --Update the FSC and ALTFSC values:
                  UPDATE_FSC (dMarket_Code,
                              dCampaign,
                              lv_linenum,
                              lv_altlinenum,
                              lv_fsc,
                              lv_altfsc);

                  INSERT INTO Avbuygetset (MODIFIEDBY,
                                           CREATEDDATE,
                                           CREATEDBY,
                                           GROUPNUMBER,
                                           AVADAPTERID,
                                           CAMPAIGN,
                                           REGULARPRICE,
                                           PRICE,
                                           CONTROLTAG,
                                           ACCOUNTRELATION,
                                           PRODUCTTYPE,
                                           PROMOTIONCODE,
                                           DISCOUNT,
                                           ITEMCLASS,
                                           ALTPRICE,
                                           SPECIALPROCESSINGFLAG,
                                           ALTPRODUCTTYPE,
                                           ALTPROMOTIONCODE,
                                           ALTITEMCLASS,
                                           UDT,
                                           AVDEFAULT,
                                           ALTSPECIALPROCESSINGFLAG,
                                           LINENUMBER,
                                           ALTLINENUMBER,
                                           ADAPTERTYPE,
                                           AVQUALIFIER,
                                           QUANTITY,
                                           FSC,
                                           ALTFSC,
                                           DATAAREAID,
                                           MODIFIEDDATE)
                       VALUES (lv_createdby,
                               TRUNC (SYSDATE),
                               lv_createdby,
                               lv_group,
                               dofferid,
                               dcampaign,
                               lv_regularprice,
                               lv_price,
                               lv_controltag,
                               lv_accountrelation,
                               lv_produttype,
                               lv_promocode,
                               lv_discount,
                               lv_itemclass,
                               lv_altprice,
                               lv_altspecialflag,
                               lv_altproduttype,
                               lv_altpromocode,
                               lv_altitemclass,
                               lv_udt,
                               lv_default,
                               lv_altspecialflag,
                               lv_linenum,
                               lv_altlinenum,
                               lv_adaptertype,
                               lv_qualifier,
                               lv_qty,
                               lv_fsc,
                               lv_altfsc,
                               dMarket_Code,
                               TRUNC (SYSDATE));
               END IF;
            ELSIF dpromoclaim = '1174'
            THEN
               IF r_offer_detail.promtn_clm_id = '1174'
               THEN
                  lv_controltag := 'G';
                  lv_altlinenum := lv_linenum;
                  lv_altprice := lv_price;
                  lv_altfsc := lv_fsc;
                  lv_altregularprice := lv_regularprice;

                  -- The idea is to create the Get with the original product if doesn't find any Alternative
                  BEGIN
                     lv_sql :=
                           'select a.line_nr,
                                to_number(decode(a.reg_prc_amt, null, 0, a.reg_prc_amt)),
                                to_number(decode(a.sls_prc_amt, null, 0, a.sls_prc_amt))
                from ods_offr_sku_line_prc a, ods_offr_prfl_prc_pnt b
               where a.dataareaid = b.dataareaid
                 and a.dataareaid = '''
                        || dMarket_Code
                        || '''
                 and a.offr_prfl_prcpt_id = b.offr_prfl_prcpt_id
                 and b.offr_id = '
                        || dodsofferid
                        || '
                 and a.promtn_clm_id in '
                        || lv_my_prom_claim_id
                        || '
                 and a.sku_id = '
                        || lv_fsc
                        || '
                 and a.line_nr > 0 and rownum = 1';

                     EXECUTE IMMEDIATE lv_sql
                        INTO lv_linenum, lv_regularprice, lv_price;
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        lv_myfound := 1;
                        lv_linenum := lv_altlinenum;
                        lv_price := lv_altprice;
                        lv_regularprice := lv_altregularprice;
                        lv_fsc := lv_altfsc;
                        lv_altlinenum := 0;
                        lv_altprice := 0;
                        lv_altfsc := '0';
                  END;

                  --if lv_linenum is null then
                  --lv_linenum:=0;
                  --end if;

                  IF lv_field12 = 'Y' AND lv_linenum <> 0 AND lv_myfound = 0
                  THEN
                     lv_linenum :=
                        SUBSTR (lv_linenum, 1, (LENGTH (lv_linenum) - 1));
                  /*elsif lv_field12 = 'N' then
                    lv_linenum := r_offer_detail.line_nr;
                  else
                    lv_linenum := r_offer_detail.line_nr;*/
                  END IF;

                  lv_myfound := 0;

                  --Update the FSC and ALTFSC values:
                  UPDATE_FSC (dMarket_Code,
                              dCampaign,
                              lv_linenum,
                              lv_altlinenum,
                              lv_fsc,
                              lv_altfsc);

                  INSERT INTO Avbuygetset (MODIFIEDBY,
                                           CREATEDDATE,
                                           CREATEDBY,
                                           GROUPNUMBER,
                                           AVADAPTERID,
                                           CAMPAIGN,
                                           REGULARPRICE,
                                           PRICE,
                                           CONTROLTAG,
                                           ACCOUNTRELATION,
                                           PRODUCTTYPE,
                                           PROMOTIONCODE,
                                           DISCOUNT,
                                           ITEMCLASS,
                                           ALTPRICE,
                                           SPECIALPROCESSINGFLAG,
                                           ALTPRODUCTTYPE,
                                           ALTPROMOTIONCODE,
                                           ALTITEMCLASS,
                                           UDT,
                                           AVDEFAULT,
                                           ALTSPECIALPROCESSINGFLAG,
                                           LINENUMBER,
                                           ALTLINENUMBER,
                                           ADAPTERTYPE,
                                           AVQUALIFIER,
                                           QUANTITY,
                                           FSC,
                                           ALTFSC,
                                           DATAAREAID,
                                           MODIFIEDDATE)
                       VALUES (lv_createdby,
                               TRUNC (SYSDATE),
                               lv_createdby,
                               lv_group,
                               dofferid,
                               dcampaign,
                               lv_regularprice,
                               lv_price,
                               lv_controltag,
                               lv_accountrelation,
                               lv_produttype,
                               lv_promocode,
                               lv_discount,
                               lv_itemclass,
                               lv_altprice,
                               lv_altspecialflag,
                               lv_altproduttype,
                               lv_altpromocode,
                               lv_altitemclass,
                               lv_udt,
                               lv_default,
                               lv_altspecialflag,
                               lv_linenum,
                               lv_altlinenum,
                               lv_adaptertype,
                               lv_qualifier,
                               lv_qty,
                               lv_fsc,
                               lv_altfsc,
                               dMarket_Code,
                               TRUNC (SYSDATE));
               END IF;
            ELSE
               IF    (r_offer_detail.promtn_clm_id = '1182')
                  OR (r_offer_detail.promtn_clm_id = '170')
               THEN
                  lv_controltag := 'G';
                  lv_altlinenum := lv_linenum;
                  lv_altprice := lv_price;
                  lv_altfsc := lv_fsc;
                  lv_altregularprice := lv_regularprice;

                  --Change in the query to get the Original based on the Alternate Line Number
                  BEGIN
                     lv_sql :=
                           'select a.line_nr,
                                to_number(decode(a.reg_prc_amt, null, 0, a.reg_prc_amt)),
                                to_number(decode(a.sls_prc_amt, null, 0, a.sls_prc_amt))
                from ods_offr_sku_line_prc a, ods_offr_prfl_prc_pnt b
               where a.dataareaid = b.dataareaid
                 and a.dataareaid = '''
                        || dMarket_Code
                        || '''
                 and a.offr_prfl_prcpt_id = b.offr_prfl_prcpt_id
                 and b.offr_id = '
                        || dodsofferid
                        || '
                 and a.promtn_clm_id in '
                        || lv_my_prom_claim_id
                        || '
                 and a.sku_id = '
                        || lv_fsc
                        || '
                 and a.line_nr > 0 ';

                     EXECUTE IMMEDIATE lv_sql
                        INTO lv_linenum, lv_regularprice, lv_price;
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        lv_myfound := 1;
                        lv_linenum := lv_altlinenum;
                        lv_price := lv_altprice;
                        lv_regularprice := lv_altregularprice;
                        lv_fsc := lv_altfsc;
                        lv_altlinenum := 0;
                        lv_altprice := 0;
                        lv_altfsc := '0';
                  END;

                  IF r_offer_detail.promtn_clm_id = '170'
                  THEN
                     lv_price_for_compare1 := lv_altprice;
                     lv_line_nr_for_compare1 := lv_altlinenum;
                  END IF;

                  --if lv_linenum is null then
                  --lv_linenum:=0;
                  --end if;

                  IF lv_field12 = 'Y' AND lv_linenum <> 0 AND lv_myfound = 0
                  THEN
                     lv_linenum :=
                        SUBSTR (lv_linenum, 1, (LENGTH (lv_linenum) - 1));
                  /* elsif lv_field12 = 'N' then
                    lv_linenum := r_offer_detail.line_nr;
                  else
                    lv_linenum := r_offer_detail.line_nr;*/
                  END IF;

                  lv_myfound := 0;

                  --Update the FSC and ALTFSC values:
                  UPDATE_FSC (dMarket_Code,
                              dCampaign,
                              lv_linenum,
                              lv_altlinenum,
                              lv_fsc,
                              lv_altfsc);

                  INSERT INTO Avbuygetset (MODIFIEDBY,
                                           CREATEDDATE,
                                           CREATEDBY,
                                           GROUPNUMBER,
                                           AVADAPTERID,
                                           CAMPAIGN,
                                           REGULARPRICE,
                                           PRICE,
                                           CONTROLTAG,
                                           ACCOUNTRELATION,
                                           PRODUCTTYPE,
                                           PROMOTIONCODE,
                                           DISCOUNT,
                                           ITEMCLASS,
                                           ALTPRICE,
                                           SPECIALPROCESSINGFLAG,
                                           ALTPRODUCTTYPE,
                                           ALTPROMOTIONCODE,
                                           ALTITEMCLASS,
                                           UDT,
                                           AVDEFAULT,
                                           ALTSPECIALPROCESSINGFLAG,
                                           LINENUMBER,
                                           ALTLINENUMBER,
                                           ADAPTERTYPE,
                                           AVQUALIFIER,
                                           QUANTITY,
                                           FSC,
                                           ALTFSC,
                                           DATAAREAID,
                                           MODIFIEDDATE)
                       VALUES (lv_createdby,
                               TRUNC (SYSDATE),
                               lv_createdby,
                               lv_group,
                               dofferid,
                               dcampaign,
                               lv_regularprice,
                               lv_price,
                               lv_controltag,
                               lv_accountrelation,
                               lv_produttype,
                               lv_promocode,
                               lv_discount,
                               lv_itemclass,
                               lv_altprice,
                               lv_altspecialflag,
                               lv_altproduttype,
                               lv_altpromocode,
                               lv_altitemclass,
                               lv_udt,
                               lv_default,
                               lv_altspecialflag,
                               lv_linenum,
                               lv_altlinenum,
                               lv_adaptertype,
                               lv_qualifier,
                               lv_qty,
                               lv_fsc,
                               lv_altfsc,
                               dMarket_Code,
                               TRUNC (SYSDATE));
               -- Promotion Claim
               --   2 - LOWEST/BEST PRICE/VALUE
               --  39  - SPECIAL INTRO PRICE
               --  40  - DISCOUNTED PRICE
               --  76  - LAST TIME
               -- 132  - PACK PRICE
               -- 147  - REGULAR PRICE
               -- 160  - REPRESENTATIVE OFFER
               -- 1162  - AUTOMATIC INSTANT/EXPRESS DELIVERY
               -- 1163  - ORDERED INSTANT/EXPRESS DELIVERY
               -- 1180  - PRICED TO GUARANTEE PROFITS FOR REPRESENTATIVES
               ELSIF UF_DOES_EXIST_SOME_STR (lv_my_prom_claim_id,
                                             r_offer_detail.promtn_clm_id) >
                        0
               THEN
                  lv_controltag := 'B';
                  lv_altlinenum := 0;
                  lv_altprice := 0;
                  lv_altfsc := '0';

                  --Update the FSC and ALTFSC values Convert:
                  UPDATE_FSC (dMarket_Code,
                              dCampaign,
                              lv_linenum,
                              lv_altlinenum,
                              lv_fsc,
                              lv_altfsc);

                  lv_price_for_compare2 := lv_price;
                  lv_line_nr_for_compare2 := lv_linenum;

                  INSERT INTO Avbuygetset (MODIFIEDBY,
                                           CREATEDDATE,
                                           CREATEDBY,
                                           GROUPNUMBER,
                                           AVADAPTERID,
                                           CAMPAIGN,
                                           REGULARPRICE,
                                           PRICE,
                                           CONTROLTAG,
                                           ACCOUNTRELATION,
                                           PRODUCTTYPE,
                                           PROMOTIONCODE,
                                           DISCOUNT,
                                           ITEMCLASS,
                                           ALTPRICE,
                                           SPECIALPROCESSINGFLAG,
                                           ALTPRODUCTTYPE,
                                           ALTPROMOTIONCODE,
                                           ALTITEMCLASS,
                                           UDT,
                                           AVDEFAULT,
                                           ALTSPECIALPROCESSINGFLAG,
                                           LINENUMBER,
                                           ALTLINENUMBER,
                                           ADAPTERTYPE,
                                           AVQUALIFIER,
                                           QUANTITY,
                                           FSC,
                                           ALTFSC,
                                           DATAAREAID,
                                           MODIFIEDDATE)
                       VALUES (lv_createdby,
                               TRUNC (SYSDATE),
                               lv_createdby,
                               lv_group,
                               dofferid,
                               dcampaign,
                               lv_regularprice,
                               lv_price,
                               lv_controltag,
                               lv_accountrelation,
                               lv_produttype,
                               lv_promocode,
                               lv_discount,
                               lv_itemclass,
                               lv_altprice,
                               lv_altspecialflag,
                               lv_altproduttype,
                               lv_altpromocode,
                               lv_altitemclass,
                               lv_udt,
                               lv_default,
                               lv_altspecialflag,
                               lv_linenum,
                               lv_altlinenum,
                               lv_adaptertype,
                               lv_qualifier,
                               lv_qty,
                               lv_fsc,
                               lv_altfsc,
                               dMarket_Code,
                               TRUNC (SYSDATE));
               END IF;
            END IF;

            IF    (r_offer_detail.promtn_clm_id = '1185')
               OR (r_offer_detail.promtn_clm_id = '170')
            THEN
               IF     TRIM (LV_FIELD15) IS NOT NULL
                  AND TRIM (LV_FIELD15) <> CHR (2)
               THEN
                  IF UF_DOES_EXIST_SOME_STR (dOffr_ntes_txt,
                                             TRIM (LV_FIELD15)) > 0
                  THEN
                     SELECT OFFR_DESC_TXT
                       INTO lv_description
                       FROM ods_offr
                      WHERE     Dataareaid = dMarket_Code
                            AND    SUBSTR (mvpvs_id, 7, 4)
                                || SUBSTR (mvpvs_id, 13, 2) = dcampaign
                            AND OFFR_ID = dodsofferid;

                     lv_description :=
                           'C'
                        || SUBSTR (dcampaign, 5, 2)
                        || 'CO'
                        || ' '
                        || lv_description;
                     lv_description := SUBSTR (lv_description, 1, 50);

                     SELECT TO_NUMBER (
                                 TO_CHAR (SYSDATE, 'HH24') * 60 * 60
                               + TO_CHAR (SYSDATE, 'MI') * 60
                               + TO_CHAR (SYSDATE, 'SS'))
                       INTO lv_modified_time
                       FROM DUAL;

                     INSERT INTO AvConvert (avuseop,
                                            avcampaignid,
                                            avdescription,
                                            avconvertid,
                                            avactive,
                                            avset,
                                            avpriority,
                                            avopdesc,
                                            avopcaption,
                                            avoppriority,
                                            avopcategory,
                                            avopmessage,
                                            avprocessseq,
                                            dataareaid,
                                            modifieddate,
                                            modifiedtime,
                                            modifiedby,
                                            createddate,
                                            createdtime,
                                            createdby)
                          VALUES (lv_useop,
                                  dcampaign,
                                  lv_description,
                                  lv_convertid,
                                  1,
                                  0,
                                  1,
                                  CHR (2),
                                  CHR (2),
                                  0,
                                  CHR (2),
                                  CHR (2),
                                  CHR (2),
                                  dMarket_Code,
                                  TRUNC (SYSDATE),
                                  lv_modified_time,
                                  lv_createdby,
                                  TRUNC (SYSDATE),
                                  lv_modified_time,
                                  lv_createdby);

                     IF lv_price_for_compare1 >= lv_price_for_compare2
                     THEN
                        IF lv_field12 = 'Y'
                        THEN
                           lv_convertlinenum :=
                              SUBSTR (lv_line_nr_for_compare1,
                                      1,
                                      (LENGTH (lv_line_nr_for_compare1) - 1));
                        ELSIF lv_field12 = 'N'
                        THEN
                           lv_convertlinenum := lv_line_nr_for_compare1;
                        ELSE
                           lv_convertlinenum := lv_line_nr_for_compare1;
                        END IF;
                     ELSE
                        IF lv_field12 = 'Y'
                        THEN
                           lv_convertlinenum :=
                              SUBSTR (lv_line_nr_for_compare2,
                                      1,
                                      (LENGTH (lv_line_nr_for_compare2) - 1));
                        ELSIF lv_field12 = 'N'
                        THEN
                           lv_convertlinenum := lv_line_nr_for_compare2;
                        ELSE
                           lv_convertlinenum := lv_line_nr_for_compare2;
                        END IF;
                     END IF;

                     IF lv_price_for_compare1 <= lv_price_for_compare2
                     THEN
                        IF lv_field12 = 'Y'
                        THEN
                           lv_brochurelinenum :=
                              SUBSTR (lv_line_nr_for_compare1,
                                      1,
                                      (LENGTH (lv_line_nr_for_compare1) - 1));
                        ELSIF lv_field12 = 'N'
                        THEN
                           lv_brochurelinenum := lv_line_nr_for_compare1;
                        ELSE
                           lv_brochurelinenum := lv_line_nr_for_compare1;
                        END IF;
                     ELSE
                        IF lv_field12 = 'Y'
                        THEN
                           lv_brochurelinenum :=
                              SUBSTR (lv_line_nr_for_compare2,
                                      1,
                                      (LENGTH (lv_line_nr_for_compare2) - 1));
                        ELSIF lv_field12 = 'N'
                        THEN
                           lv_brochurelinenum := lv_line_nr_for_compare2;
                        ELSE
                           lv_brochurelinenum := lv_line_nr_for_compare2;
                        END IF;
                     END IF;

                     --Update the FSC and ALTFSC values:
                     UPDATE_FSC (dMarket_Code,
                                 dCampaign,
                                 lv_brochurelinenum,
                                 lv_convertlinenum,
                                 lv_fsc,
                                 lv_altfsc);

                     INSERT INTO AvConvertLineNum (avbrochurelinenum,
                                                   avconvertlinenum,
                                                   avcampaignid,
                                                   avconvertid,
                                                   avitemclass,
                                                   avproducttype,
                                                   avpromotioncode,
                                                   avprice,
                                                   avdiscount,
                                                   avconvertfsc,
                                                   avfsc,
                                                   modifieddate,
                                                   modifiedtime,
                                                   modifiedby,
                                                   createddate,
                                                   createdtime,
                                                   createdby,
                                                   dataareaid)
                          VALUES (lv_brochurelinenum,
                                  lv_convertlinenum,
                                  dCampaign,
                                  lv_convertid,
                                  CHR (2),
                                  CHR (2),
                                  CHR (2),
                                  0,
                                  0,
                                  lv_fsc,
                                  lv_altfsc,
                                  TRUNC (SYSDATE),
                                  lv_modified_time,
                                  lv_createdby,
                                  TRUNC (SYSDATE),
                                  lv_modified_time,
                                  lv_createdby,
                                  dMarket_Code);

                     --no request to insert profile
                     --                     INSERT INTO AvAdapterProfile (avadapter,
                     --                                                   avprofileid,
                     --                                                   avadapterid,
                     --                                                   avflag,
                     --                                                   avgroup,
                     --                                                   dataareaid)
                     --                          VALUES (12,
                     --                                  -1,
                     --                                  lv_convertid,
                     --                                  0,
                     --                                  0,
                     --                                  dMarket_Code);

                     INSERT INTO ODS_OFFR_LINK (MAPS_OFFER_ID,
                                                PROMO_CLAIM_ID,
                                                GOMAC_OFFER_ID,
                                                OFFER_TYPE,
                                                dataareaid,
                                                campaign,
                                                createddate)
                          VALUES (dodsofferid,
                                  r_offer_detail.promtn_clm_id,
                                  lv_convertid,
                                  'Covert',
                                  dMarket_Code,
                                  dcampaign,
                                  SYSDATE);

                     INSERT INTO AVUSERLOG (AVUSERID,
                                            AVADAPTER,
                                            AVACTION,
                                            AVADAPTERID,
                                            DATETIME,
                                            DATAAREAID)
                          VALUES (
                                    'ODS_GOMAC',
                                    'LOAD',
                                       'Offer_Type: Convert'
                                    || ' - Prom_Claim_ID:'
                                    || r_offer_detail.promtn_clm_id
                                    || ' - ODS Offer ID:'
                                    || dodsofferid
                                    || ' - GOMAC Offer ID: '
                                    || lv_convertid,
                                    'UPK_ODS_GOMAC_LOAD',
                                    SYSDATE,
                                    dMarket_Code);
                  END IF;
               END IF;
            END IF;
         END IF;

         IF dodsoffertype = 'GCBO'
         THEN
            IF dpromoclaim = '170'
            THEN
               IF               /*(r_offer_detail.promtn_clm_id = '1182') or*/
                  (r_offer_detail.promtn_clm_id = '170')
               THEN
                  lv_controltag := 'G';
                  lv_altlinenum := lv_linenum;
                  lv_altprice := lv_price;
                  lv_altfsc := lv_fsc;
                  lv_altregularprice := lv_regularprice;

                  BEGIN
                     lv_sql :=
                           'select a.line_nr,
                              to_number(decode(a.reg_prc_amt, null, 0, a.reg_prc_amt)),
                              to_number(decode(a.sls_prc_amt, null, 0, a.sls_prc_amt))
                from ods_offr_sku_line_prc a, ods_offr_prfl_prc_pnt b
               where a.dataareaid = b.dataareaid
                 and a.dataareaid = '''
                        || dMarket_Code
                        || '''
                 and a.offr_prfl_prcpt_id = b.offr_prfl_prcpt_id
                 and b.offr_id = '
                        || dodsofferid
                        || '
                 and a.promtn_clm_id in '
                        || lv_my_prom_claim_id
                        || '
                 and a.sku_id = '
                        || lv_fsc
                        || '
                 and a.line_nr > 0 and rownum = 1';

                     EXECUTE IMMEDIATE lv_sql
                        INTO lv_linenum, lv_regularprice, lv_price;
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        lv_myfound := 1;
                        lv_linenum := lv_altlinenum;
                        lv_price := lv_altprice;
                        lv_regularprice := lv_altregularprice;
                        lv_fsc := lv_altfsc;
                        lv_altlinenum := 0;
                        lv_altprice := 0;
                        lv_altfsc := '0';
                  END;

                  --if lv_linenum is null then
                  --lv_linenum:=0;
                  --end if;

                  IF lv_field12 = 'Y' AND lv_linenum <> 0 AND lv_myfound = 0
                  THEN
                     lv_linenum :=
                        SUBSTR (lv_linenum, 1, (LENGTH (lv_linenum) - 1));
                  /* elsif lv_field12 = 'N' then
                    lv_linenum := r_offer_detail.line_nr;
                  else
                    lv_linenum := r_offer_detail.line_nr;*/
                  END IF;

                  lv_myfound := 0;

                  --Update the FSC and ALTFSC values:
                  UPDATE_FSC (dMarket_Code,
                              dCampaign,
                              lv_linenum,
                              lv_altlinenum,
                              lv_fsc,
                              lv_altfsc);

                  INSERT INTO Avbuygetset (MODIFIEDBY,
                                           CREATEDDATE,
                                           CREATEDBY,
                                           GROUPNUMBER,
                                           AVADAPTERID,
                                           CAMPAIGN,
                                           REGULARPRICE,
                                           PRICE,
                                           CONTROLTAG,
                                           ACCOUNTRELATION,
                                           PRODUCTTYPE,
                                           PROMOTIONCODE,
                                           DISCOUNT,
                                           ITEMCLASS,
                                           ALTPRICE,
                                           SPECIALPROCESSINGFLAG,
                                           ALTPRODUCTTYPE,
                                           ALTPROMOTIONCODE,
                                           ALTITEMCLASS,
                                           UDT,
                                           AVDEFAULT,
                                           ALTSPECIALPROCESSINGFLAG,
                                           LINENUMBER,
                                           ALTLINENUMBER,
                                           ADAPTERTYPE,
                                           AVQUALIFIER,
                                           QUANTITY,
                                           FSC,
                                           ALTFSC,
                                           DATAAREAID,
                                           MODIFIEDDATE)
                       VALUES (lv_createdby,
                               TRUNC (SYSDATE),
                               lv_createdby,
                               lv_group,
                               dofferid,
                               dcampaign,
                               lv_regularprice,
                               lv_price,
                               lv_controltag,
                               lv_accountrelation,
                               lv_produttype,
                               lv_promocode,
                               lv_discount,
                               lv_itemclass,
                               lv_altprice,
                               lv_altspecialflag,
                               lv_altproduttype,
                               lv_altpromocode,
                               lv_altitemclass,
                               lv_udt,
                               lv_default,
                               lv_altspecialflag,
                               lv_linenum,
                               lv_altlinenum,
                               lv_adaptertype,
                               lv_qualifier,
                               lv_qty,
                               lv_fsc,
                               lv_altfsc,
                               dMarket_Code,
                               TRUNC (SYSDATE));
               -- Promotion Claim
               --   2 - LOWEST/BEST PRICE/VALUE
               --  39  - SPECIAL INTRO PRICE
               --  40  - DISCOUNTED PRICE
               --  76  - LAST TIME
               -- 132  - PACK PRICE
               -- 147  - REGULAR PRICE
               -- 160  - REPRESENTATIVE OFFER
               -- 1162  - AUTOMATIC INSTANT/EXPRESS DELIVERY
               -- 1163  - ORDERED INSTANT/EXPRESS DELIVERY
               -- 1180  - PRICED TO GUARANTEE PROFITS FOR REPRESENTATIVES
               ELSIF UF_DOES_EXIST_SOME_STR (lv_my_prom_claim_id,
                                             r_offer_detail.promtn_clm_id) >
                        0
               THEN
                  lv_controltag := 'B';
                  lv_altlinenum := 0;
                  lv_altprice := 0;
                  lv_altfsc := '0';

                  --Update the FSC and ALTFSC values:
                  UPDATE_FSC (dMarket_Code,
                              dCampaign,
                              lv_linenum,
                              lv_altlinenum,
                              lv_fsc,
                              lv_altfsc);

                  INSERT INTO Avbuygetset (MODIFIEDBY,
                                           CREATEDDATE,
                                           CREATEDBY,
                                           GROUPNUMBER,
                                           AVADAPTERID,
                                           CAMPAIGN,
                                           REGULARPRICE,
                                           PRICE,
                                           CONTROLTAG,
                                           ACCOUNTRELATION,
                                           PRODUCTTYPE,
                                           PROMOTIONCODE,
                                           DISCOUNT,
                                           ITEMCLASS,
                                           ALTPRICE,
                                           SPECIALPROCESSINGFLAG,
                                           ALTPRODUCTTYPE,
                                           ALTPROMOTIONCODE,
                                           ALTITEMCLASS,
                                           UDT,
                                           AVDEFAULT,
                                           ALTSPECIALPROCESSINGFLAG,
                                           LINENUMBER,
                                           ALTLINENUMBER,
                                           ADAPTERTYPE,
                                           AVQUALIFIER,
                                           QUANTITY,
                                           FSC,
                                           ALTFSC,
                                           DATAAREAID,
                                           MODIFIEDDATE)
                       VALUES (lv_createdby,
                               TRUNC (SYSDATE),
                               lv_createdby,
                               lv_group,
                               dofferid,
                               dcampaign,
                               lv_regularprice,
                               lv_price,
                               lv_controltag,
                               lv_accountrelation,
                               lv_produttype,
                               lv_promocode,
                               lv_discount,
                               lv_itemclass,
                               lv_altprice,
                               lv_altspecialflag,
                               lv_altproduttype,
                               lv_altpromocode,
                               lv_altitemclass,
                               lv_udt,
                               lv_default,
                               lv_altspecialflag,
                               lv_linenum,
                               lv_altlinenum,
                               lv_adaptertype,
                               lv_qualifier,
                               lv_qty,
                               lv_fsc,
                               lv_altfsc,
                               dMarket_Code,
                               TRUNC (SYSDATE));
               END IF;
            END IF;
         END IF;

         IF dodsoffertype = 'DEMO'
         THEN
            IF dpromoclaim = '40'
            THEN
               IF (    r_offer_detail.promtn_clm_id = '40'
                   AND r_offer_detail.sls_cls_cd = '4')
               THEN
                  lv_controltag := 'B';
                  lv_altlinenum := 0;
                  lv_altprice := 0;
                  lv_altfsc := '0';

                  --Update the FSC and ALTFSC values:
                  UPDATE_FSC (dMarket_Code,
                              dCampaign,
                              lv_linenum,
                              lv_altlinenum,
                              lv_fsc,
                              lv_altfsc);

                  INSERT INTO Avbuygetset (MODIFIEDBY,
                                           CREATEDDATE,
                                           CREATEDBY,
                                           GROUPNUMBER,
                                           AVADAPTERID,
                                           CAMPAIGN,
                                           REGULARPRICE,
                                           PRICE,
                                           CONTROLTAG,
                                           ACCOUNTRELATION,
                                           PRODUCTTYPE,
                                           PROMOTIONCODE,
                                           DISCOUNT,
                                           ITEMCLASS,
                                           ALTPRICE,
                                           SPECIALPROCESSINGFLAG,
                                           ALTPRODUCTTYPE,
                                           ALTPROMOTIONCODE,
                                           ALTITEMCLASS,
                                           UDT,
                                           AVDEFAULT,
                                           ALTSPECIALPROCESSINGFLAG,
                                           LINENUMBER,
                                           ALTLINENUMBER,
                                           ADAPTERTYPE,
                                           AVQUALIFIER,
                                           QUANTITY,
                                           FSC,
                                           ALTFSC,
                                           DATAAREAID,
                                           MODIFIEDDATE)
                       VALUES (lv_createdby,
                               TRUNC (SYSDATE),
                               lv_createdby,
                               lv_group,
                               dofferid,
                               dcampaign,
                               lv_regularprice,
                               lv_price,
                               lv_controltag,
                               lv_accountrelation,
                               lv_produttype,
                               lv_promocode,
                               lv_discount,
                               lv_itemclass,
                               lv_altprice,
                               lv_altspecialflag,
                               lv_altproduttype,
                               lv_altpromocode,
                               lv_altitemclass,
                               lv_udt,
                               lv_default,
                               lv_altspecialflag,
                               lv_linenum,
                               lv_altlinenum,
                               '12',
                               lv_qualifier,
                               lv_qty,
                               lv_fsc,
                               lv_altfsc,
                               dMarket_Code,
                               TRUNC (SYSDATE));
               END IF;
            END IF;
         END IF;

         IF dodsoffertype = 'LIMIT'
         THEN
            IF dpromoclaim = '160'
            THEN
               IF r_offer_detail.promtn_clm_id = '160'
               THEN
                  lv_controltag := 'B';
                  lv_price := 0;
                  lv_altlinenum := 0;
                  lv_altprice := 0;
                  lv_fsc := '0';
                  lv_altfsc := '0';

                  --Update the FSC and ALTFSC values:
                  UPDATE_FSC (dMarket_Code,
                              dCampaign,
                              lv_linenum,
                              lv_altlinenum,
                              lv_fsc,
                              lv_altfsc);

                  INSERT INTO Avbuygetset (MODIFIEDBY,
                                           CREATEDDATE,
                                           CREATEDBY,
                                           GROUPNUMBER,
                                           AVADAPTERID,
                                           CAMPAIGN,
                                           REGULARPRICE,
                                           PRICE,
                                           CONTROLTAG,
                                           ACCOUNTRELATION,
                                           PRODUCTTYPE,
                                           PROMOTIONCODE,
                                           DISCOUNT,
                                           ITEMCLASS,
                                           ALTPRICE,
                                           SPECIALPROCESSINGFLAG,
                                           ALTPRODUCTTYPE,
                                           ALTPROMOTIONCODE,
                                           ALTITEMCLASS,
                                           UDT,
                                           AVDEFAULT,
                                           ALTSPECIALPROCESSINGFLAG,
                                           LINENUMBER,
                                           ALTLINENUMBER,
                                           ADAPTERTYPE,
                                           AVQUALIFIER,
                                           QUANTITY,
                                           FSC,
                                           ALTFSC,
                                           DATAAREAID,
                                           MODIFIEDDATE)
                       VALUES (lv_createdby,
                               TRUNC (SYSDATE),
                               lv_createdby,
                               lv_group,
                               dofferid,
                               dcampaign,
                               lv_regularprice,
                               lv_price,
                               lv_controltag,
                               lv_accountrelation,
                               lv_produttype,
                               lv_promocode,
                               lv_discount,
                               lv_itemclass,
                               lv_altprice,
                               lv_altspecialflag,
                               lv_altproduttype,
                               lv_altpromocode,
                               lv_altitemclass,
                               lv_udt,
                               lv_default,
                               lv_altspecialflag,
                               lv_linenum,
                               lv_altlinenum,
                               --lv_adaptertype,
                               '11',
                               lv_qualifier,
                               lv_qty,
                               lv_fsc,
                               lv_altfsc,
                               dMarket_Code,
                               TRUNC (SYSDATE));
               END IF;
            END IF;
         --END IF;
         END IF;
      END LOOP;

      COMMIT;
   END;                                                     --End-Of-This-Code

   PROCEDURE UPLOAD_PROFILE (dMarket_Code   IN avofferheader.dataareaid%TYPE,
                             dofferid       IN avofferheader.OfferId%TYPE)
   IS
      -- Initialize Variables
      lv_adaptertype   NVARCHAR2 (10) := '12';
      lv_profileid     NUMBER := -1;
      lv_flag          NUMBER := 0;
      lv_group         NUMBER := 0;
      --lv_recversion  NUMBER := 1;

      i_cnt            NUMBER := 0;
   BEGIN
      SELECT COUNT (*)
        INTO i_cnt
        FROM AvGomacProfile
       WHERE dataareaid = dMarket_Code;

      IF i_cnt > 0
      THEN
         /*select max(avgomacprofileid)
          into lv_profileid
          from AvGomacProfile
         where dataareaid = dMarket_Code;*/
         lv_profileid := lv_global_profile_id;
      END IF;

      -- Insert information in the AVADAPTERPROFILE table with default value "Apply to All"

      INSERT INTO AVADAPTERPROFILE (AVADAPTER,
                                    AVPROFILEID,
                                    AVADAPTERID,
                                    AVFLAG,
                                    AVGROUP,
                                    DATAAREAID)
           VALUES (lv_adaptertype,
                   lv_profileid,
                   dofferid,
                   lv_flag,
                   lv_group,
                   dMarket_Code);
   END;                                                     --End-Of-This-Code

   --##########################################################
   --Primary controlling procedure being called externally    #
   --##########################################################

   PROCEDURE USP_ODS_GOMAC_MAIN (
      Market_Code   IN avofferheader.dataareaid%TYPE,
      myCampaign    IN avofferheader.campaign%TYPE)
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;

      --Individual known transaction code numbers
      CURSOR c_offer
      IS
           SELECT DISTINCT
                  DECODE (a.offr_id, NULL, 0, a.offr_id) offer_id,
                  DECODE (a.offr_ntes_txt, NULL, CHR (2), a.offr_ntes_txt)
                     offr_ntes_txt,
                  DECODE (a.offr_lyot_cmnts_txt,
                          NULL, CHR (2),
                          a.offr_lyot_cmnts_txt)
                     offr_lyot_cmnts_txt,
                  DECODE (a.frnt_cvr_ind, NULL, CHR (2), a.frnt_cvr_ind)
                     frnt_cvr_ind,
                  DECODE (a.offr_desc_txt, NULL, CHR (2), a.offr_desc_txt)
                     offr_desc_txt,
                  SUBSTR (a.mvpvs_id, 7, 4) || SUBSTR (a.mvpvs_id, 13, 2)
                     offr_campaign,
                  REGEXP_SUBSTR (MVPVS_ID,
                                 '[^_]+',
                                 1,
                                 2)
                     offr_BROCUHRE,
                  DECODE (a.offr_desc_txt, NULL, CHR (2), a.offr_desc_txt)
                     offer_desc,
                  --b.sls_cls_cd offer_sls_cls,
                  --b.sls_cls_desc_txt offer_sls_txt,
                  DECODE (b.promtn_clm_id, NULL, CHR (2), b.promtn_clm_id)
                     offer_promo_clm,
                  --b.offr_prfl_prcpt_id,
                  DECODE (c.promotion_claim_desc,
                          NULL, ' ',
                          c.promotion_claim_desc)
                     offer_promo_clm_desc,
                  DECODE (c.offer_type, NULL, ' ', c.offer_type) offer_type,
                  DECODE (c.offer_type_desc, NULL, ' ', c.offer_type_desc)
                     offer_type_desc,
                  DECODE (c.product_type, NULL, '0', c.product_type)
                     product_type,
                  DECODE (c.priority, NULL, 1, c.priority) priority,
                  DECODE (c.qual_type, NULL, 'UNITS', c.qual_type) qual_type,
                  DECODE (c.ormore, NULL, 'F', c.ormore) ormore,
                  DECODE (c.groupnum, NULL, 1, c.groupnum) groupnum,
                  DECODE (c.Min_Get_Quantity, NULL, 0, c.Min_Get_Quantity)
                     Min_Get_Quantity,
                  DECODE (c.LIMIT_QTY, '', 0, c.LIMIT_QTY) LIMIT_QTY,
                  DECODE (c.enabled, NULL, 'F', c.enabled) enabled,
                  DECODE (c.combine, NULL, 'ANY', c.combine) combine,
                  DECODE (c.min_qualifier, NULL, 0, c.min_qualifier)
                     Min_Qualifier,
                  DECODE (c.back_order, NULL, 'T', c.back_order) back_order,
                  DECODE (c.back_out, NULL, '1', c.back_out) back_out,
                  DECODE (c.zero_step, NULL, 1, c.zero_step) zero_step,
                  DECODE (c.use_online_promotion,
                          NULL, '0',
                          c.use_online_promotion)
                     use_online_promotion,
                  DECODE (c.limit_base, NULL, '-1', c.limit_base) limit_base,
                  DECODE (b.page_nr, NULL, 0, b.page_nr) page_nr,
                  DECODE (b.sls_cls_cd, NULL, CHR (2), b.sls_cls_cd) sls_cls_cd
             FROM ods_offr a, ods_offr_prfl_prc_pnt b, ods_claim_offr_type c
            WHERE     a.dataareaid = b.dataareaid
                  AND a.dataareaid = c.dataareaid
                  AND a.offr_id = b.offr_id
                  AND b.promtn_clm_id = c.promotion_claim_id
                  AND c.offer_type <> 'ITEM'
                  AND a.dataareaid = Market_Code
         ORDER BY offer_id;

      r_offer                  c_offer%ROWTYPE;

      -- Initialize Variables
      lv2_sqlmsg               VARCHAR2 (255);
      lv_offerid               NUMBER;
      lv_seqno                 VARCHAR2 (15) := 'GMID';
      lv_group                 NUMBER;
      lv_description           VARCHAR2 (150);
      lv_description_pagenr    VARCHAR2 (20);
      lv_enabled               VARCHAR2 (5);
      lv_priority              NUMBER;
      lv_qualifytype           VARCHAR2 (10);
      --lv_offertype           NVARCHAR2(10);
      lv_offertype             VARCHAR2 (20);
      lv_buyqualifier          NUMBER;
      lv_getqty                NUMBER;
      lv_producttype           NUMBER;
      lv_combine               VARCHAR2 (10);
      lv_backorder             VARCHAR2 (5);
      lv_backout               NUMBER;
      lv_ormore                VARCHAR2 (5);
      lv_default               NUMBER := 0;
      lv_distinct              NUMBER := 0;
      lv_zerostep              NUMBER;
      lv_offerflag             NUMBER := 0;
      lv_profiletype           NUMBER := 0;
      lv_limitbase             NUMBER;
      lv_limitqty              NUMBER := 0;
      lv_scheduletype          NUMBER := 0;
      lv_usebuysetone          NUMBER := 0;
      lv_byofferids            VARCHAR2 (10) := CHR (2);
      lv_applyonce             NUMBER := 0;
      lv_interfactive          NUMBER := 0;
      lv_extdesc               VARCHAR2 (50) := CHR (2);
      lv_usetagupdate          NUMBER := 0;
      lv_bypassmonitem         NUMBER := 0;
      lv_exceedoption          NUMBER := 0;
      lv_distedefault          NUMBER := 0;
      lv_udt                   NUMBER := 0;
      lv_useop                 NUMBER;
      lv_opdesc                VARCHAR2 (50) := CHR (2);
      lv_opcaption             VARCHAR2 (50) := CHR (2);
      lv_oppriority            NUMBER := 0;
      lv_opcategory            NUMBER := 0;
      lv_opmessage             VARCHAR2 (50) := CHR (2);
      lv_opmessagedual         NUMBER := 0;
      lv_opmesageget           VARCHAR2 (50) := CHR (2);
      lv_nodefaultitem         NUMBER := 0;
      lv_odsofferid            NUMBER := 0;
      lv_promoclaim            VARCHAR2 (10);
      lv_campaign              NVARCHAR2 (6);
      lv_validation            NUMBER := 0;
      --lv_offr_ntes_txt         ods_offr.offr_desc_txt%TYPE;
      lv_offr_ntes_txt         ods_offr.OFFR_NTES_TXT%TYPE;
      lv_profile_id            NUMBER (10);
      lv_change_time           NUMBER (10);
      lv_avfiled_name          VARCHAR2 (100);
      lv_current_campaign      VARCHAR2 (10);
      lv_commit_rows_control   NUMBER := 0;

      lv_modified_time         NUMBER;
      lv_productlimitid        NUMBER;
      lv_createdby             NVARCHAR2 (10) := 'ODS';

      lv_of_records_cnt        NUMBER := 0;
      lv_co_records_cnt        NUMBER := 0;

      lv_pl_records_cnt        NUMBER := 0;

      lv_linenumber            NUMBER;

      TYPE v_adapterid IS TABLE OF VARCHAR2 (100)
                             INDEX BY PLS_INTEGER;

      lv_adapterid             v_adapterid;
   BEGIN
      lv_current_campaign := myCampaign;

      SELECT TRIM (field1),
             TRIM (field2),
             TRIM (field3),
             TRIM (field4),
             TRIM (field5),
             TRIM (field7),
             TRIM (field8),
             TRIM (field9),
             TRIM (field10),
             TRIM (field11),
             TRIM (field12),
             TRIM (field16)
        INTO lv_field1,
             lv_field2,
             lv_field3,
             lv_field4,
             lv_field5,
             lv_field7,
             lv_field8,
             lv_field9,
             lv_field10,
             lv_field11,
             lv_field12,
             lv_field16
        FROM ods_claim_offr_attr
       WHERE dataareaid = Market_Code AND ROWNUM = 1;

      BEGIN
         SELECT AvCodeID
           INTO lv_avfiled_name
           FROM AvCodeTable
          WHERE     dataareaid = Market_Code
                AND avcodetype = 2000
                AND AVDESCRIPTION =
                       (SELECT field1
                          FROM ODS_CLAIM_OFFR_ATTR
                         WHERE dataareaid = Market_Code AND ROWNUM = 1);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            lv_avfiled_name := CHR (2);
      END;

      SELECT   TO_CHAR (SYSDATE, 'HH24') * 60 * 60
             + TO_CHAR (SYSDATE, 'MI') * 60
             + TO_CHAR (SYSDATE, 'SS')
        INTO lv_change_time
        FROM DUAL;

      SELECT COUNT (*)
        INTO lv_of_records_cnt
        FROM AvOfferHeader
       WHERE dataareaid = Market_Code AND campaign = lv_current_campaign;

      SELECT COUNT (*)
        INTO lv_co_records_cnt
        FROM AvConvert
       WHERE dataareaid = Market_Code AND AVCAMPAIGNID = lv_current_campaign;

      SELECT COUNT (*)
        INTO lv_pl_records_cnt
        FROM AVPRODUCTLIMIT
       WHERE     dataareaid = Market_Code
             AND AVCAMPAIGN = lv_field7
             AND AVCAMPAIGNTO = lv_field8;

          SELECT DISTINCT TRIM (REGEXP_SUBSTR (REPLACE (field14, '''', ''),
                                               '[^,]+',
                                               1,
                                               LEVEL))
            BULK COLLECT INTO lv_prom_claim_id
            FROM ods_claim_offr_attr
           WHERE dataareaid = Market_Code AND LENGTH (TRIM (field14)) > 0
      START WITH dataareaid = Market_Code
      CONNECT BY     dataareaid = Market_Code
                 AND LEVEL <=
                        REGEXP_COUNT (REPLACE (field14, '''', ''), ',') + 1
                 AND field14 = PRIOR field14
                 AND PRIOR DBMS_RANDOM.VALUE IS NOT NULL
        ORDER BY TO_NUMBER (TRIM (REGEXP_SUBSTR (REPLACE (field14, '''', ''),
                                                 '[^,]+',
                                                 1,
                                                 LEVEL)));

      FOR i IN lv_prom_claim_id.FIRST () .. lv_prom_claim_id.LAST ()
      LOOP
         IF lv_my_prom_claim_id IS NULL
         THEN
            lv_my_prom_claim_id :=
                  '('
               || lv_my_prom_claim_id
               || ''''
               || lv_prom_claim_id (i)
               || '''';
         ELSE
            lv_my_prom_claim_id :=
                  lv_my_prom_claim_id
               || ','
               || ''''
               || lv_prom_claim_id (i)
               || '''';
         END IF;
      END LOOP;

      IF lv_my_prom_claim_id IS NOT NULL
      THEN
         lv_my_prom_claim_id := lv_my_prom_claim_id || ')';
         lv_my_prom_claim_id :=
            SUBSTR (lv_my_prom_claim_id,
                    INSTR (lv_my_prom_claim_id,
                           '(',
                           1,
                           1),
                    INSTR (lv_my_prom_claim_id,
                           ')',
                           1,
                           1));
      END IF;

          --Seperate field11 default values into different values and store each of them
          --as individual number, users have confirmed that field11 and field14 will be like
          --8, 1, 2, 4, 5, 18, 10, 12, 16, 9, 11.
          SELECT DISTINCT TRIM (REGEXP_SUBSTR (REPLACE (field11, '''', ''),
                                               '[^,]+',
                                               1,
                                               LEVEL))
            BULK COLLECT INTO lv_adapterid
            FROM ods_claim_offr_attr
           WHERE dataareaid = Market_Code                     --and rownum = 1
                                         AND LENGTH (TRIM (field11)) > 0
      START WITH dataareaid = Market_Code
      CONNECT BY     dataareaid = Market_Code
                 AND LEVEL <=
                        REGEXP_COUNT (REPLACE (field11, '''', ''), ',') + 1
                 AND field11 = PRIOR field11
                 AND PRIOR DBMS_RANDOM.VALUE IS NOT NULL
        ORDER BY TO_NUMBER (TRIM (REGEXP_SUBSTR (REPLACE (field11, '''', ''),
                                                 '[^,]+',
                                                 1,
                                                 LEVEL)));

      IF lv_field3 = 'Y'
      THEN
         DELETE FROM AVGOMACPROFILEDETAIL
               WHERE AVGOMACPROFILEID =
                        (SELECT AVPROFILEID
                           FROM AVADAPTERPROFILE
                          WHERE     ROWNUM = 1
                                AND AVADAPTERID IN
                                       (SELECT OFFERID
                                          FROM AVOFFERHEADER
                                         WHERE     DATAAREAID = Market_Code
                                               AND CAMPAIGN =
                                                      lv_current_campaign));

         DELETE FROM AVGOMACPROFILE
               WHERE AVGOMACPROFILEID =
                        (SELECT AVPROFILEID
                           FROM AVADAPTERPROFILE
                          WHERE     ROWNUM = 1
                                AND AVADAPTERID IN
                                       (SELECT OFFERID
                                          FROM AVOFFERHEADER
                                         WHERE     DATAAREAID = Market_Code
                                               AND CAMPAIGN =
                                                      lv_current_campaign));

         DELETE FROM AvGomacProfileAssign
               WHERE AVGOMACPROFILEID =
                        (SELECT AVPROFILEID
                           FROM AVADAPTERPROFILE
                          WHERE     ROWNUM = 1
                                AND AVADAPTERID IN
                                       (SELECT OFFERID
                                          FROM AVOFFERHEADER
                                         WHERE     DATAAREAID = Market_Code
                                               AND CAMPAIGN =
                                                      lv_current_campaign));

         COMMIT;

         FOR c_cur
            IN (SELECT offerid
                  FROM AvOfferHeader
                 WHERE     dataareaid = Market_Code
                       AND campaign = lv_current_campaign)
         LOOP
            DELETE FROM AvAdapterProfile
                  WHERE     dataareaid = Market_Code --AND avadapterid = r_offer.offer_id;
                        AND avadapterid = c_cur.offerid;

            IF lv_of_records_cnt <= 10000
            THEN
               COMMIT;
            ELSE
               lv_commit_rows_control := lv_commit_rows_control + 1;

               IF MOD (lv_commit_rows_control, 10000) = 0
               THEN
                  COMMIT;
               END IF;
            --remanent records will be committed out of the loop
            END IF;
         END LOOP;

         lv_commit_rows_control := 0;
         COMMIT;

         FOR c_cur
            IN (SELECT AVCONVERTID
                  FROM AVCONVERT
                 WHERE     dataareaid = Market_Code
                       AND AVCAMPAIGNID = lv_current_campaign)
         LOOP
            DELETE FROM AvAdapterProfile
                  WHERE     dataareaid = Market_Code
                        AND avadapterid = c_cur.AVCONVERTID;

            IF lv_co_records_cnt <= 10000
            THEN
               COMMIT;
            ELSE
               lv_commit_rows_control := lv_commit_rows_control + 1;

               IF MOD (lv_commit_rows_control, 10000) = 0
               THEN
                  COMMIT;
               END IF;
            --remanent records will be committed out of the loop
            END IF;
         END LOOP;

         lv_commit_rows_control := 0;
         COMMIT;

         FOR c_cur
            IN (SELECT AVPRODUCTLIMITID
                  FROM AVPRODUCTLIMIT
                 WHERE     dataareaid = Market_Code
                       AND AVCAMPAIGN = lv_field7
                       AND AVCAMPAIGNTO = lv_field8)
         LOOP
            DELETE FROM AvAdapterProfile
                  WHERE     dataareaid = Market_Code
                        AND avadapterid = c_cur.AVPRODUCTLIMITID;

            IF lv_pl_records_cnt <= 10000
            THEN
               COMMIT;
            ELSE
               lv_commit_rows_control := lv_commit_rows_control + 1;

               IF MOD (lv_commit_rows_control, 10000) = 0
               THEN
                  COMMIT;
               END IF;
            --remanent records will be committed out of the loop
            END IF;
         END LOOP;

         lv_commit_rows_control := 0;
         COMMIT;
      END IF;

      IF lv_field3 = 'Y'
      THEN
         DELETE FROM AvOfferHeader
               WHERE     dataareaid = Market_Code
                     AND campaign = lv_current_campaign;

         DELETE FROM AvBuyGetSet
               WHERE     dataareaid = Market_Code
                     AND campaign = lv_current_campaign
                     AND ADAPTERTYPE IN (11, 12);

         DELETE FROM ODS_OFFR_LINK
               WHERE     dataareaid = Market_Code
                     AND campaign = lv_current_campaign;

         DELETE FROM AVCONVERT
               WHERE     dataareaid = Market_Code
                     AND AVCAMPAIGNID = lv_current_campaign;

         DELETE FROM AVCONVERTLINENUM
               WHERE     dataareaid = Market_Code
                     AND AVCAMPAIGNID = lv_current_campaign;

         DELETE FROM AVPRODUCTLIMITGROUP
               WHERE AVPRODUCTLIMITID IN
                        (SELECT AVPRODUCTLIMITID
                           FROM AVPRODUCTLIMIT
                          WHERE     dataareaid = Market_Code
                                AND AVCAMPAIGN = lv_field7
                                AND AVCAMPAIGNTO = lv_field8);

         DELETE FROM AVPRODUCTLIMIT
               WHERE     dataareaid = Market_Code
                     AND AVCAMPAIGN = lv_field7
                     AND AVCAMPAIGNTO = lv_field8;

         COMMIT;
      END IF;

      IF lv_field3 = 'N'
      THEN
         IF    (    LENGTH (TRIM (lv_field1)) = 0
                AND LENGTH (TRIM (lv_field2)) > 0)
            OR (    LENGTH (TRIM (lv_field1)) > 0
                AND LENGTH (TRIM (lv_field2)) = 0)
         THEN
            RAISE parameters_null;
         END IF;
      END IF;

      IF (LENGTH (TRIM (lv_field1)) <> 0 AND LENGTH (TRIM (lv_field2)) <> 0)
      THEN
         SELECT GOMAC.UF_GET_GM_SEQNO (Market_Code, 'GMID')
           INTO lv_profile_id
           FROM DUAL;

         --Global Variable:may be used later in other places:
         lv_global_profile_id := lv_profile_id;

         INSERT INTO AvGomacProfile (dataareaid,
                                     Avgomacprofileid,
                                     Avdescription,
                                     avcomments,
                                     Modifieddate,
                                     modifiedtime,
                                     modifiedby,
                                     createddate,
                                     createdtime,
                                     createdby)
              VALUES (
                        Market_Code,
                        lv_profile_id,
                           'C'
                        || SUBSTR (lv_current_campaign, 5, 2)
                        || 'PR'
                        || lv_field9
                        || lv_current_campaign,
                        lv_field10 || lv_current_campaign,
                        TRUNC (SYSDATE),
                        lv_change_time,
                        'UPK_ODS_GOMAC_LOAD',
                        TRUNC (SYSDATE),
                        lv_change_time,
                        'UPK_ODS_GOMAC_LOAD');

         INSERT INTO AvGomacProfileDetail (avgomacprofileid,
                                           avlogical,
                                           avfieldname,
                                           avvalue1,
                                           avvalue2,
                                           avformulaid,
                                           avaccumid,
                                           avflag,
                                           dataareaid)
              VALUES (lv_profile_id,
                      lv_field2,
                      lv_avfiled_name,
                      lv_current_campaign,
                      ' ',
                      0,
                      0,
                      0,
                      Market_Code);

         COMMIT;

         --create one profile and one profile detail each time for running
         --this package as requested by Arthur but for each profile may have
         --different adapter id(means different AvGomacProfileAssign.AvAdapter values):
         FOR i IN lv_adapterid.FIRST () .. lv_adapterid.LAST ()
         LOOP
            INSERT
              INTO AvGomacProfileAssign (avgomacprofileid,
                                         avadapter,
                                         dataareaid)
            VALUES (lv_profile_id, lv_adapterid (i), Market_Code);

            COMMIT;
         END LOOP;

         COMMIT;
      END IF;

      --Log the start of job message
      -- Log Start
      INSERT INTO AVUSERLOG (AVUSERID,
                             AVADAPTER,
                             AVACTION,
                             AVADAPTERID,
                             DATETIME,
                             DATAAREAID)
           VALUES ('ODS_GOMAC',
                   'LOAD',
                   'ODS to GOMAC Interface has started',
                   'UPK_ODS_GOMAC_LOAD',
                   SYSDATE,
                   Market_Code);

      COMMIT;

      --------------------------------------------hardcode
      lv_offerid := GOMAC.UF_GET_GM_SEQNO (Market_Code, lv_seqno);

      lv_description :=
            'C'
         || SUBSTR (lv_current_campaign, 5, 2)                       --|| 'OF'
         || ' '
         || 'WEBE CONTROL DE OFERTAS';

      lv_description := SUBSTR (lv_description, 1, 50);

      INSERT INTO avOfferHeader (                                    --TIMETO,
                                 --TIMEFROM,
                                 --RECVERSION,
                                 --RECID,
                                 QUALIFYTYPE,
                                 PROFILETYPE,
                                 PRODUCTTYPE,
                                 PRIORITY,
                                 ORMORE,
                                 OFFERTYPE,
                                 OFFERID,
                                 OFFERFLAG,
                                 GROUPNUMBER,
                                 GETQUANTITY,
                                 ENABLED,
                                 DESCRIPTION,
                                 DEMO,
                                 DATETO,
                                 DATEFROM,
                                 DATAAREAID,
                                 COMBINE,
                                 CAMPAIGN,
                                 BUYQUALIFIER,
                                 BACKOUT,
                                 BACKORDER,
                                 AVZEROSTEP,
                                 AVUSETAGUPDATE,
                                 AVUSEOP,
                                 AVUSELINEPRICE,
                                 AVUSEBUYSETONCE,
                                 AVUDT,
                                 --AVSEGMENTED,
                                 AVSCHEDULETYPE,
                                 AVREJECTEXCEEDQUAL,
                                 AVREJECTBELOWQUAL,
                                 --AVOVERLAPPING,
                                 --AVOPSHOWSOLDOUT,
                                 --AVOPSCHEDULEBYTIME,
                                 AVOPPRIORITY,
                                 --AVOPPARENTOFFER,
                                 AVOPMESSAGEGET,
                                 AVOPMESSAGEDUAL,
                                 AVOPMESSAGE,
                                 --AVOPLIMITEDTIMEOFFER,
                                 --AVOPLIMITEDQUANTITY,
                                 AVOPDESC,
                                 --AVOPCLEARANCEOFFER,
                                 AVOPCATEGORY,
                                 AVOPCAPTION,
                                 AVOFFEROPTION,
                                 AVNODEFAULTITEM,
                                 AVMONITOR,
                                 AVLIMITQTY,
                                 AVLIMITINTERACTIVE,
                                 AVLIMITBASE,
                                 AVINTERACTIVE,
                                 AVIGNORECOBO,
                                 AVEXTDESC,
                                 AVEXECUTEAFTERDISCOUNT,
                                 AVEXCEEDOPTION,
                                 AVDISTINCTDEFAULT,
                                 AVDISTINCT,
                                 AVDEFAULT,
                                 AVBYPASSMONITOREDITEMS,
                                 AVBYOFFERIDS,
                                 AVAPPLYONCE)
           VALUES (                                                      --'',
                                                                         --'',
                  --1,
                  --GOMAC.UF_GET_GM_SEQNO (Market_Code, lv_seqno),
                  'UNITS',
                  '0',
                  '0',
                  '1',
                  'F',
                  'SPRC',
                  lv_offerid,
                  '0',
                  '1',
                  '1',
                  'T',
                  lv_description,
                  ' ',
                  TRUNC (SYSDATE),
                  TRUNC (SYSDATE),
                  Market_Code,
                  'ANY',
                  lv_current_campaign,
                  '1',
                  '1',
                  'T',
                  '1',
                  '0',
                  '0',
                  '0',
                  '0',
                  '0',
                  --'',
                  '0',
                  '0',
                  '0',
                  --'',
                  --'',
                  --'',
                  '0',
                  --'',
                  CHR (2),
                  '0',
                  CHR (2),
                  --'',
                  --'',
                  CHR (2),
                  --'',
                  '0',
                  CHR (2),
                  ' ',
                  '0',
                  '0',
                  '0',
                  '0',
                  '-1',
                  '0',
                  '0',
                  CHR (2),
                  '0',
                  '0',
                  '0',
                  '0',
                  '0',
                  '0',
                  CHR (2),
                  '0');

      --Buy Set
      INSERT INTO Avbuygetset (UDT,
                               SPECIALPROCESSINGFLAG,
                               REGULARPRICE,
                               --RECVERSION,
                               --RECID,
                               QUANTITY,
                               PROMOTIONCODE,
                               PRODUCTTYPE,
                               PRICE,
                               --ONLINEDESC,
                               LINENUMBER,
                               ITEMCLASS,
                               GROUPNUMBER,
                               FSC,
                               DISCOUNT,
                               DATAAREAID,
                               CREATEDDATE,
                               CREATEDBY,
                               CONTROLTAG,
                               CAMPAIGN,
                               AVQUALIFIER,
                               --AVHASDISC,
                               AVDEFAULT,
                               AVADAPTERID,
                               ALTSPECIALPROCESSINGFLAG,
                               ALTPROMOTIONCODE,
                               ALTPRODUCTTYPE,
                               ALTPRICE,
                               ALTLINENUMBER,
                               ALTITEMCLASS,
                               ALTFSC,
                               ADAPTERTYPE,
                               ACCOUNTRELATION)
           VALUES ('0',
                   '0',
                   '0.01',
                   --RECVERSION,
                   --RECID,
                   '0',
                   '0',
                   '0',
                   '0.01',
                   --ONLINEDESC,
                   '95998',
                   CHR (2),
                   '1',
                   '-9',
                   0,
                   Market_Code,
                   TRUNC (SYSDATE),
                   'ODS',
                   'B',
                   lv_current_campaign,
                   '0',
                   --AVHASDISC,
                   '0',
                   lv_offerid,
                   '0',
                   '0',
                   '0',
                   '0',
                   '0',
                   CHR (2),
                   0,
                   12,
                   0);

      DECLARE
         CURSOR c_hardoffer
         IS
              SELECT DISTINCT
                     DECODE (A.SKU_ID, NULL, 0, A.SKU_ID) SKU_ID,
                     DECODE (A.LINE_NR, NULL, 0, A.LINE_NR) LINE_NR,
                     DECODE (A.REG_PRC_AMT, NULL, 0, A.REG_PRC_AMT) REG_PRC_AMT,
                     DECODE (A.SLS_PRC_AMT, NULL, 0, A.SLS_PRC_AMT) SLS_PRC_AMT,
                     DECODE (B.PROMTN_CLM_ID, NULL, CHR (2), B.PROMTN_CLM_ID)
                        OFFER_PROMO_CLM
                FROM ODS_OFFR_SKU_LINE_PRC A,
                     ODS_OFFR_PRFL_PRC_PNT B,
                     ODS_CLAIM_OFFR_TYPE C,
                     ODS_OFFR D,
                     ODS_OFFR_SKU_LINE E
               WHERE     A.DATAAREAID = B.DATAAREAID
                     AND A.DATAAREAID = C.DATAAREAID
                     AND C.DATAAREAID = D.DATAAREAID
                     AND B.PROMTN_CLM_ID = C.PROMOTION_CLAIM_ID
                     AND A.DATAAREAID = '101'
                     AND A.OFFR_PRFL_PRCPT_ID = B.OFFR_PRFL_PRCPT_ID
                     AND A.LINE_NR > 0
                     AND REGEXP_SUBSTR (D.MVPVS_ID,
                                        '[^_]+',
                                        1,
                                        2) = '43'
                     AND A.SKU_ID = E.SKU_ID
                     AND E.OFFR_ID = D.OFFR_ID
            ORDER BY LINE_NR;

         r_hardoffer   c_hardoffer%ROWTYPE;
      BEGIN
         FOR r_hardoffer IN c_hardoffer
         LOOP
            IF lv_field12 = 'Y'
            THEN
               lv_linenumber :=
                  SUBSTR (r_hardoffer.line_nr,
                          1,
                          (LENGTH (r_hardoffer.line_nr) - 1));
            ELSIF lv_field12 = 'N'
            THEN
               lv_linenumber := r_hardoffer.line_nr;
            ELSE
               lv_linenumber := r_hardoffer.line_nr;
            END IF;

            IF UF_DOES_EXIST_SOME_STR (lv_my_prom_claim_id,
                                       r_hardoffer.OFFER_PROMO_CLM) > 0
            THEN
               --GET Set

               INSERT INTO Avbuygetset (UDT,
                                        SPECIALPROCESSINGFLAG,
                                        REGULARPRICE,
                                        --RECVERSION,
                                        --RECID,
                                        QUANTITY,
                                        PROMOTIONCODE,
                                        PRODUCTTYPE,
                                        PRICE,
                                        --ONLINEDESC,
                                        LINENUMBER,
                                        ITEMCLASS,
                                        GROUPNUMBER,
                                        FSC,
                                        DISCOUNT,
                                        DATAAREAID,
                                        CREATEDDATE,
                                        CREATEDBY,
                                        CONTROLTAG,
                                        CAMPAIGN,
                                        AVQUALIFIER,
                                        --AVHASDISC,
                                        AVDEFAULT,
                                        AVADAPTERID,
                                        ALTSPECIALPROCESSINGFLAG,
                                        ALTPROMOTIONCODE,
                                        ALTPRODUCTTYPE,
                                        ALTPRICE,
                                        ALTLINENUMBER,
                                        ALTITEMCLASS,
                                        ALTFSC,
                                        ADAPTERTYPE,
                                        ACCOUNTRELATION)
                    VALUES ('0',
                            '0',
                            r_hardoffer.REG_PRC_AMT,
                            --RECVERSION,
                            --RECID,
                            '0',
                            '0',
                            '0',
                            r_hardoffer.SLS_PRC_AMT,
                            --ONLINEDESC,
                            lv_linenumber,
                            CHR (2),
                            '1',
                            r_hardoffer.SKU_ID,
                            0,
                            Market_Code,
                            TRUNC (SYSDATE),
                            'ODS',
                            'G',
                            lv_current_campaign,
                            '0',
                            --AVHASDISC,
                            '0',
                            lv_offerid,
                            '0',
                            '0',
                            '0',
                            '0',
                            '0',
                            CHR (2),
                            0,
                            12,
                            0);
            END IF;
         END LOOP;
      END;

      INSERT INTO AVADAPTERPROFILE (AVADAPTER,
                                    AVPROFILEID,
                                    AVADAPTERID,
                                    AVFLAG,
                                    AVGROUP,
                                    DATAAREAID)
           VALUES ('11',
                   lv_profile_id,
                   lv_offerid,
                   '0',
                   '0',
                   Market_Code);

      INSERT INTO AVUSERLOG (AVUSERID,
                             AVADAPTER,
                             AVACTION,
                             AVADAPTERID,
                             DATETIME,
                             DATAAREAID)
           VALUES ('ODS_GOMAC',
                   'LOAD',
                   'Offer_Type: SPRC' || ' - hard offer:' || lv_offerid,
                   'UPK_ODS_GOMAC_LOAD',
                   SYSDATE,
                   Market_Code);

      --------------------------------------------hardcode end

      FOR r_offer IN c_offer
      LOOP
         lv_campaign := r_offer.offr_campaign;

         --lv_current_campaign := myCampaign;

         -- Validate if the Campaign processed belong to the campaign in the file
         -- Will process only the values in the value with the same campaign
         IF lv_campaign = lv_current_campaign
         THEN
            -- Get the Offer Id and the Offer Type to valida with the Table Link
            -- This table is used to validate if the offer was already send
            lv_odsofferid := r_offer.offer_id;
            lv_global_offer_id := lv_odsofferid;
            lv_offertype := r_offer.offer_type;
            lv_promoclaim := r_offer.offer_promo_clm;

            lv_producttype := r_offer.product_type;
            lv_priority := r_offer.priority;
            lv_qualifytype := r_offer.qual_type;
            lv_ormore := r_offer.ormore;
            lv_group := r_offer.groupnum;
            lv_enabled := r_offer.enabled;
            lv_combine := r_offer.combine;
            lv_backorder := r_offer.back_order;
            lv_backout := r_offer.back_out;
            lv_zerostep := r_offer.zero_step;
            lv_useop := r_offer.use_online_promotion;
            lv_limitbase := r_offer.limit_base;
            lv_offr_ntes_txt := r_offer.offr_ntes_txt;

            --Verify if the offer was already uploaded
            SELECT COUNT (*)
              INTO lv_validation
              FROM ods_offr_link
             WHERE     dataareaid = Market_Code
                   AND maps_offer_id = lv_odsofferid
                   AND promo_claim_id = lv_promoclaim;

            -- Means that the offer doesn't exist, proceed to execute the process
            IF lv_validation = 0
            THEN
               -- Get the next sequence Number to add in GOMAC
               lv_offerid := GOMAC.UF_GET_GM_SEQNO (Market_Code, lv_seqno);

               --lv_convertid := lv_offerid;

               IF r_offer.offer_type = 'GFRE'
               THEN
                  IF     TRIM (LV_FIELD5) IS NOT NULL
                     AND TRIM (LV_FIELD5) <> CHR (2)
                  THEN
                     IF UF_DOES_EXIST_SOME_STR (R_OFFER.OFFR_NTES_TXT,
                                                TRIM (LV_FIELD5)) = 0
                     THEN
                        IF TRIM (R_OFFER.Min_Get_Quantity) IS NULL
                        THEN
                           RAISE PARAMETERS_NULL;
                        ELSE
                           IF UF_IS_NUMERIC (R_OFFER.Min_Get_Quantity) = 0
                           THEN
                              LV_GETQTY := 0;
                           ELSE
                              LV_GETQTY := R_OFFER.Min_Get_Quantity;
                           END IF;
                        END IF;
                     ELSE
                        IF R_OFFER.OFFER_PROMO_CLM = '169'
                        THEN
                           IF UF_IS_NUMERIC (
                                 SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                           INSTR (R_OFFER.OFFR_NTES_TXT,
                                                  TRIM (LV_FIELD5),
                                                  1,
                                                  1)
                                         + LENGTH (TRIM (LV_FIELD5)),
                                         2)) = 0
                           THEN
                              IF TRIM (R_OFFER.Min_Get_Quantity) IS NULL
                              THEN
                                 RAISE PARAMETERS_NULL;
                              ELSE
                                 IF UF_IS_NUMERIC (R_OFFER.Min_Get_Quantity) =
                                       0
                                 THEN
                                    LV_GETQTY := 0;
                                 ELSE
                                    LV_GETQTY := R_OFFER.Min_Get_Quantity;
                                 END IF;
                              END IF;
                           ELSE
                              LV_GETQTY :=
                                 SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                           INSTR (R_OFFER.OFFR_NTES_TXT,
                                                  TRIM (LV_FIELD5),
                                                  1,
                                                  1)
                                         + LENGTH (TRIM (LV_FIELD5)),
                                         2);
                           END IF;
                        ELSIF    (R_OFFER.OFFER_PROMO_CLM = '1181')
                              OR (R_OFFER.OFFER_PROMO_CLM = '1175')
                        THEN
                           IF UF_IS_NUMERIC (
                                 SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                           INSTR (R_OFFER.OFFR_NTES_TXT,
                                                  TRIM (LV_FIELD5),
                                                  1,
                                                  1)
                                         + LENGTH (TRIM (LV_FIELD5)),
                                         1)) = 0
                           THEN
                              IF TRIM (R_OFFER.Min_Get_Quantity) IS NULL
                              THEN
                                 RAISE PARAMETERS_NULL;
                              ELSE
                                 IF UF_IS_NUMERIC (R_OFFER.Min_Get_Quantity) =
                                       0
                                 THEN
                                    LV_GETQTY := 0;
                                 ELSE
                                    LV_GETQTY := R_OFFER.Min_Get_Quantity;
                                 END IF;
                              END IF;
                           ELSE
                              LV_GETQTY :=
                                 SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                           INSTR (R_OFFER.OFFR_NTES_TXT,
                                                  TRIM (LV_FIELD5),
                                                  1,
                                                  1)
                                         + LENGTH (TRIM (LV_FIELD5)),
                                         1);
                           END IF;
                        END IF;
                     END IF;
                  ELSE
                     IF TRIM (R_OFFER.Min_Get_Quantity) IS NULL
                     THEN
                        RAISE PARAMETERS_NULL;
                     ELSE
                        IF UF_IS_NUMERIC (R_OFFER.Min_Get_Quantity) = 0
                        THEN
                           LV_GETQTY := 0;
                        ELSE
                           LV_GETQTY := R_OFFER.Min_Get_Quantity;
                        END IF;
                     END IF;
                  END IF;

                  IF     TRIM (LV_FIELD4) IS NOT NULL
                     AND TRIM (LV_FIELD4) <> CHR (2)
                  THEN
                     IF UF_DOES_EXIST_SOME_STR (R_OFFER.OFFR_NTES_TXT,
                                                TRIM (LV_FIELD4)) = 0
                     THEN
                        IF TRIM (R_OFFER.MIN_QUALIFIER) IS NULL
                        THEN
                           RAISE PARAMETERS_NULL;
                        ELSE
                           IF UF_IS_NUMERIC (R_OFFER.MIN_QUALIFIER) = 0
                           THEN
                              LV_BUYQUALIFIER := 0;
                           ELSE
                              LV_BUYQUALIFIER := R_OFFER.MIN_QUALIFIER;
                           END IF;
                        END IF;
                     ELSE
                        IF R_OFFER.OFFER_PROMO_CLM = '169'
                        THEN
                           IF UF_IS_NUMERIC (
                                 SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                           INSTR (R_OFFER.OFFR_NTES_TXT,
                                                  TRIM (LV_FIELD4),
                                                  1,
                                                  1)
                                         + LENGTH (TRIM (LV_FIELD4)),
                                         2)) = 0
                           THEN
                              IF TRIM (R_OFFER.MIN_QUALIFIER) IS NULL
                              THEN
                                 RAISE PARAMETERS_NULL;
                              ELSE
                                 IF UF_IS_NUMERIC (R_OFFER.MIN_QUALIFIER) = 0
                                 THEN
                                    LV_BUYQUALIFIER := 0;
                                 ELSE
                                    LV_BUYQUALIFIER := R_OFFER.MIN_QUALIFIER;
                                 END IF;
                              END IF;
                           ELSE
                              LV_BUYQUALIFIER :=
                                 SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                           INSTR (R_OFFER.OFFR_NTES_TXT,
                                                  TRIM (LV_FIELD4),
                                                  1,
                                                  1)
                                         + LENGTH (TRIM (LV_FIELD4)),
                                         2);
                           END IF;
                        ELSIF R_OFFER.OFFER_PROMO_CLM = '1181'
                        THEN
                           IF UF_IS_NUMERIC (
                                 SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                           INSTR (R_OFFER.OFFR_NTES_TXT,
                                                  TRIM (LV_FIELD4),
                                                  1,
                                                  1)
                                         + LENGTH (TRIM (LV_FIELD4)),
                                         1)) = 0
                           THEN
                              IF TRIM (R_OFFER.MIN_QUALIFIER) IS NULL
                              THEN
                                 RAISE PARAMETERS_NULL;
                              ELSE
                                 IF UF_IS_NUMERIC (R_OFFER.MIN_QUALIFIER) = 0
                                 THEN
                                    LV_BUYQUALIFIER := 0;
                                 ELSE
                                    LV_BUYQUALIFIER := R_OFFER.MIN_QUALIFIER;
                                 END IF;
                              END IF;
                           ELSE
                              LV_BUYQUALIFIER :=
                                 SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                           INSTR (R_OFFER.OFFR_NTES_TXT,
                                                  TRIM (LV_FIELD4),
                                                  1,
                                                  1)
                                         + LENGTH (TRIM (LV_FIELD4)),
                                         1);
                           END IF;
                        END IF;
                     END IF;
                  ELSE
                     IF TRIM (R_OFFER.MIN_QUALIFIER) IS NULL
                     THEN
                        RAISE PARAMETERS_NULL;
                     ELSE
                        IF UF_IS_NUMERIC (R_OFFER.MIN_QUALIFIER) = 0
                        THEN
                           LV_BUYQUALIFIER := 0;
                        ELSE
                           LV_BUYQUALIFIER := R_OFFER.MIN_QUALIFIER;
                        END IF;
                     END IF;
                  END IF;

                  -- Get values into variables
                  --lv_odsofferid  := r_offer.offer_id;
                  lv_description :=
                        'C'
                     || SUBSTR (lv_current_campaign, 5, 2)
                     || 'OF'
                     || ' '
                     || r_offer.offr_desc_txt;
                  lv_description_pagenr := ' P.' || r_offer.page_nr || ' ';

                  IF r_offer.offr_BROCUHRE = '42'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'FH';
                  ELSIF r_offer.offr_BROCUHRE = '11'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'CF';
                  ELSIF r_offer.offr_BROCUHRE = '35'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'AC';
                  ELSIF r_offer.offr_BROCUHRE = '43'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'WE';
                  END IF;

                  lv_description :=
                     SUBSTR (lv_description,
                             1,
                             50 - LENGTH (lv_description_pagenr));
                  lv_description := lv_description || lv_description_pagenr;

                  --lv_description := SUBSTR (lv_description, 1, 50);

                  -- Process to insert the Offer Header information
                  INSERT INTO avOfferHeader (dataareaid,
                                             OfferId,
                                             GroupNumber,
                                             Description,
                                             Enabled,
                                             Priority,
                                             Campaign,
                                             DateFrom,
                                             DateTo,
                                             QualifyType,
                                             OfferType,
                                             BuyQualifier,
                                             GetQuantity,
                                             ProductType,
                                             Combine,
                                             BackOrder,
                                             Backout,
                                             OrMore,
                                             avDefault,
                                             avDistinct,
                                             AVZEROSTEP,
                                             OFFERFLAG,
                                             PROFILETYPE,
                                             AVLIMITBASE,
                                             AVLIMITQTY,
                                             AVSCHEDULETYPE,
                                             AVUSEBUYSETONCE,
                                             AVBYOFFERIDS,
                                             AVAPPLYONCE,
                                             AVINTERACTIVE,
                                             AVEXTDESC,
                                             AVUSETAGUPDATE,
                                             AVBYPASSMONITOREDITEMS,
                                             AVEXCEEDOPTION,
                                             AVdistinctDefault,
                                             AVUDT,
                                             AVUSEOP,
                                             AVOPDESC,
                                             AVOPCAPTION,
                                             AVOPPRIORITY,
                                             AVOPCATEGORY,
                                             AVOPMESSAGE,
                                             AVOPMESSAGEDUAL,
                                             AVOPMESSAGEGET,
                                             AVNODEFAULTITEM)
                       VALUES (Market_Code,
                               lv_offerid,
                               lv_group,
                               lv_description,
                               lv_enabled,
                               lv_priority,
                               lv_current_campaign,
                               TRUNC (SYSDATE),
                               TRUNC (SYSDATE),
                               lv_qualifytype,
                               lv_offertype,
                               lv_buyqualifier,
                               lv_getqty,
                               lv_producttype,
                               lv_combine,
                               lv_backorder,
                               lv_backout,
                               lv_ormore,
                               lv_default,
                               lv_distinct,
                               lv_zerostep,
                               lv_offerflag,
                               lv_profiletype,
                               lv_limitbase,
                               lv_limitqty,
                               lv_scheduletype,
                               lv_usebuysetone,
                               lv_byofferids,
                               lv_applyonce,
                               lv_interfactive,
                               lv_extdesc,
                               lv_usetagupdate,
                               lv_bypassmonitem,
                               lv_exceedoption,
                               lv_distedefault,
                               lv_udt,
                               lv_useop,
                               lv_opdesc,
                               lv_opcaption,
                               lv_oppriority,
                               lv_opcategory,
                               lv_opmessage,
                               lv_opmessagedual,
                               lv_opmesageget,
                               lv_nodefaultitem);

                  lv_my_own_variable := 0;

                  -- Call offer detail procedure
                  UPLOAD_DETAIL (Market_Code,
                                 lv_current_campaign,
                                 lv_odsofferid,
                                 lv_offertype,
                                 lv_offerid,
                                 lv_promoclaim,
                                 lv_offr_ntes_txt);

                  -- Insert informaiton in the Adapter Profile Table with value "Apply to All"
                  IF lv_my_own_variable = 0
                  THEN
                     UPLOAD_PROFILE (Market_Code, lv_offerid);

                     -- Insert information in the Link Table to control not to upload the file several times
                     INSERT INTO ODS_OFFR_LINK (MAPS_OFFER_ID,
                                                PROMO_CLAIM_ID,
                                                GOMAC_OFFER_ID,
                                                OFFER_TYPE,
                                                dataareaid,
                                                campaign,
                                                createddate)
                          VALUES (lv_odsofferid,
                                  lv_promoclaim,
                                  lv_offerid,
                                  lv_offertype,
                                  Market_Code,
                                  lv_current_campaign,
                                  SYSDATE);
                  END IF;

                  INSERT INTO AVUSERLOG (AVUSERID,
                                         AVADAPTER,
                                         AVACTION,
                                         AVADAPTERID,
                                         DATETIME,
                                         DATAAREAID)
                       VALUES (
                                 'ODS_GOMAC',
                                 'LOAD',
                                    'Offer_Type:'
                                 || r_offer.offer_type
                                 || ' - Prom_Claim_ID:'
                                 || r_offer.offer_promo_clm
                                 || ' - ODS Offer ID:'
                                 || r_offer.offer_id
                                 || ' - GOMAC Offer ID: '
                                 || lv_offerid,
                                 'UPK_ODS_GOMAC_LOAD',
                                 SYSDATE,
                                 Market_Code);

                  COMMIT;
               ELSIF (    TRIM (LV_FIELD16) IS NOT NULL
                      AND TRIM (LV_FIELD16) <> CHR (2)
                      AND r_offer.offer_type = 'GCBO')
               THEN
                  IF     TRIM (LV_FIELD5) IS NOT NULL
                     AND TRIM (LV_FIELD5) <> CHR (2)
                  THEN
                     IF UF_DOES_EXIST_SOME_STR (R_OFFER.OFFR_NTES_TXT,
                                                TRIM (LV_FIELD5)) = 0
                     THEN
                        IF TRIM (R_OFFER.Min_Get_Quantity) IS NULL
                        THEN
                           RAISE PARAMETERS_NULL;
                        ELSE
                           IF UF_IS_NUMERIC (R_OFFER.Min_Get_Quantity) = 0
                           THEN
                              LV_GETQTY := 0;
                           ELSE
                              LV_GETQTY := R_OFFER.Min_Get_Quantity;
                           END IF;
                        END IF;
                     ELSE
                        IF UF_IS_NUMERIC (
                              SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                        INSTR (R_OFFER.OFFR_NTES_TXT,
                                               TRIM (LV_FIELD5),
                                               1,
                                               1)
                                      + LENGTH (TRIM (LV_FIELD5)),
                                      1)) = 0
                        THEN
                           IF TRIM (R_OFFER.Min_Get_Quantity) IS NULL
                           THEN
                              RAISE PARAMETERS_NULL;
                           ELSE
                              IF UF_IS_NUMERIC (R_OFFER.Min_Get_Quantity) = 0
                              THEN
                                 LV_GETQTY := 0;
                              ELSE
                                 LV_GETQTY := R_OFFER.Min_Get_Quantity;
                              END IF;
                           END IF;
                        ELSE
                           LV_GETQTY :=
                              SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                        INSTR (R_OFFER.OFFR_NTES_TXT,
                                               TRIM (LV_FIELD5),
                                               1,
                                               1)
                                      + LENGTH (TRIM (LV_FIELD5)),
                                      1);
                        END IF;
                     END IF;
                  ELSE
                     IF TRIM (R_OFFER.Min_Get_Quantity) IS NULL
                     THEN
                        RAISE PARAMETERS_NULL;
                     ELSE
                        IF UF_IS_NUMERIC (R_OFFER.Min_Get_Quantity) = 0
                        THEN
                           LV_GETQTY := 0;
                        ELSE
                           LV_GETQTY := R_OFFER.Min_Get_Quantity;
                        END IF;
                     END IF;
                  END IF;

                  IF     TRIM (LV_FIELD4) IS NOT NULL
                     AND TRIM (LV_FIELD4) <> CHR (2)
                  THEN
                     IF UF_DOES_EXIST_SOME_STR (R_OFFER.OFFR_NTES_TXT,
                                                TRIM (LV_FIELD4)) = 0
                     THEN
                        IF TRIM (R_OFFER.MIN_QUALIFIER) IS NULL
                        THEN
                           RAISE PARAMETERS_NULL;
                        ELSE
                           IF UF_IS_NUMERIC (R_OFFER.MIN_QUALIFIER) = 0
                           THEN
                              LV_BUYQUALIFIER := 0;
                           ELSE
                              LV_BUYQUALIFIER := R_OFFER.MIN_QUALIFIER;
                           END IF;
                        END IF;
                     ELSE
                        IF UF_IS_NUMERIC (
                              SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                        INSTR (R_OFFER.OFFR_NTES_TXT,
                                               TRIM (LV_FIELD4),
                                               1,
                                               1)
                                      + LENGTH (TRIM (LV_FIELD4)),
                                      1)) = 0
                        THEN
                           IF TRIM (R_OFFER.MIN_QUALIFIER) IS NULL
                           THEN
                              RAISE PARAMETERS_NULL;
                           ELSE
                              IF UF_IS_NUMERIC (R_OFFER.MIN_QUALIFIER) = 0
                              THEN
                                 LV_BUYQUALIFIER := 0;
                              ELSE
                                 LV_BUYQUALIFIER := R_OFFER.MIN_QUALIFIER;
                              END IF;
                           END IF;
                        ELSE
                           LV_BUYQUALIFIER :=
                              SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                        INSTR (R_OFFER.OFFR_NTES_TXT,
                                               TRIM (LV_FIELD4),
                                               1,
                                               1)
                                      + LENGTH (TRIM (LV_FIELD4)),
                                      1);
                        END IF;
                     END IF;
                  ELSE
                     IF TRIM (R_OFFER.MIN_QUALIFIER) IS NULL
                     THEN
                        RAISE PARAMETERS_NULL;
                     ELSE
                        IF UF_IS_NUMERIC (R_OFFER.MIN_QUALIFIER) = 0
                        THEN
                           LV_BUYQUALIFIER := 0;
                        ELSE
                           LV_BUYQUALIFIER := R_OFFER.MIN_QUALIFIER;
                        END IF;
                     END IF;
                  END IF;

                  -- Get values into variables
                  --lv_odsofferid  := r_offer.offer_id;
                  lv_description :=
                        'C'
                     || SUBSTR (lv_current_campaign, 5, 2)
                     || 'OF'
                     || ' '
                     || r_offer.offr_desc_txt;

                  lv_description_pagenr := ' P.' || r_offer.page_nr || ' ';

                  IF r_offer.offr_BROCUHRE = '42'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'FH';
                  ELSIF r_offer.offr_BROCUHRE = '11'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'CF';
                  ELSIF r_offer.offr_BROCUHRE = '35'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'AC';
                  ELSIF r_offer.offr_BROCUHRE = '43'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'WE';
                  END IF;

                  lv_description :=
                     SUBSTR (lv_description,
                             1,
                             50 - LENGTH (lv_description_pagenr));
                  lv_description := lv_description || lv_description_pagenr;

                  --lv_description := SUBSTR (lv_description, 1, 50);

                  -- Process to insert the Offer Header information
                  INSERT INTO avOfferHeader (dataareaid,
                                             OfferId,
                                             GroupNumber,
                                             Description,
                                             Enabled,
                                             Priority,
                                             Campaign,
                                             DateFrom,
                                             DateTo,
                                             QualifyType,
                                             OfferType,
                                             BuyQualifier,
                                             GetQuantity,
                                             ProductType,
                                             Combine,
                                             BackOrder,
                                             Backout,
                                             OrMore,
                                             avDefault,
                                             avDistinct,
                                             AVZEROSTEP,
                                             OFFERFLAG,
                                             PROFILETYPE,
                                             AVLIMITBASE,
                                             AVLIMITQTY,
                                             AVSCHEDULETYPE,
                                             AVUSEBUYSETONCE,
                                             AVBYOFFERIDS,
                                             AVAPPLYONCE,
                                             AVINTERACTIVE,
                                             AVEXTDESC,
                                             AVUSETAGUPDATE,
                                             AVBYPASSMONITOREDITEMS,
                                             AVEXCEEDOPTION,
                                             AVdistinctDefault,
                                             AVUDT,
                                             AVUSEOP,
                                             AVOPDESC,
                                             AVOPCAPTION,
                                             AVOPPRIORITY,
                                             AVOPCATEGORY,
                                             AVOPMESSAGE,
                                             AVOPMESSAGEDUAL,
                                             AVOPMESSAGEGET,
                                             AVNODEFAULTITEM)
                       VALUES (Market_Code,
                               lv_offerid,
                               lv_group,
                               lv_description,
                               lv_enabled,
                               lv_priority,
                               lv_current_campaign,
                               TRUNC (SYSDATE),
                               TRUNC (SYSDATE),
                               lv_qualifytype,
                               lv_offertype,
                               lv_buyqualifier,
                               lv_getqty,
                               lv_producttype,
                               lv_combine,
                               lv_backorder,
                               lv_backout,
                               lv_ormore,
                               lv_default,
                               lv_distinct,
                               lv_zerostep,
                               lv_offerflag,
                               lv_profiletype,
                               lv_limitbase,
                               lv_limitqty,
                               lv_scheduletype,
                               lv_usebuysetone,
                               lv_byofferids,
                               lv_applyonce,
                               lv_interfactive,
                               lv_extdesc,
                               lv_usetagupdate,
                               lv_bypassmonitem,
                               lv_exceedoption,
                               lv_distedefault,
                               lv_udt,
                               lv_useop,
                               lv_opdesc,
                               lv_opcaption,
                               lv_oppriority,
                               lv_opcategory,
                               lv_opmessage,
                               lv_opmessagedual,
                               lv_opmesageget,
                               lv_nodefaultitem);

                  -- Call offer detail procedure
                  UPLOAD_DETAIL (Market_Code,
                                 lv_current_campaign,
                                 lv_odsofferid,
                                 lv_offertype,
                                 lv_offerid,
                                 lv_promoclaim,
                                 lv_offr_ntes_txt);

                  -- Insert informaiton in the Adapter Profile Table with value "Apply to All"
                  UPLOAD_PROFILE (Market_Code, lv_offerid);

                  -- Insert information in the Link Table to control not to upload the file several times
                  INSERT INTO ODS_OFFR_LINK (MAPS_OFFER_ID,
                                             PROMO_CLAIM_ID,
                                             GOMAC_OFFER_ID,
                                             OFFER_TYPE,
                                             dataareaid,
                                             campaign,
                                             createddate)
                       VALUES (lv_odsofferid,
                               lv_promoclaim,
                               lv_offerid,
                               lv_offertype,
                               Market_Code,
                               lv_current_campaign,
                               SYSDATE);

                  INSERT INTO AVUSERLOG (AVUSERID,
                                         AVADAPTER,
                                         AVACTION,
                                         AVADAPTERID,
                                         DATETIME,
                                         DATAAREAID)
                       VALUES (
                                 'ODS_GOMAC',
                                 'LOAD',
                                    'Offer_Type:'
                                 || r_offer.offer_type
                                 || ' - Prom_Claim_ID:'
                                 || r_offer.offer_promo_clm
                                 || ' - ODS Offer ID:'
                                 || r_offer.offer_id
                                 || ' - GOMAC Offer ID: '
                                 || lv_offerid,
                                 'UPK_ODS_GOMAC_LOAD',
                                 SYSDATE,
                                 Market_Code);

                  COMMIT;
               ELSIF r_offer.offer_type = 'SPRC'
               THEN
                  IF     TRIM (LV_FIELD5) IS NOT NULL
                     AND TRIM (LV_FIELD5) <> CHR (2)
                  THEN
                     IF UF_DOES_EXIST_SOME_STR (R_OFFER.OFFR_NTES_TXT,
                                                TRIM (LV_FIELD5)) = 0
                     THEN
                        IF TRIM (R_OFFER.Min_Get_Quantity) IS NULL
                        THEN
                           RAISE PARAMETERS_NULL;
                        ELSE
                           IF UF_IS_NUMERIC (R_OFFER.Min_Get_Quantity) = 0
                           THEN
                              LV_GETQTY := 0;
                           ELSE
                              LV_GETQTY := R_OFFER.Min_Get_Quantity;
                           END IF;
                        END IF;
                     ELSE
                        IF UF_IS_NUMERIC (
                              SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                        INSTR (R_OFFER.OFFR_NTES_TXT,
                                               TRIM (LV_FIELD5),
                                               1,
                                               1)
                                      + LENGTH (TRIM (LV_FIELD5)),
                                      1)) = 0
                        THEN
                           IF TRIM (R_OFFER.Min_Get_Quantity) IS NULL
                           THEN
                              RAISE PARAMETERS_NULL;
                           ELSE
                              IF UF_IS_NUMERIC (R_OFFER.Min_Get_Quantity) = 0
                              THEN
                                 LV_GETQTY := 0;
                              ELSE
                                 LV_GETQTY := R_OFFER.Min_Get_Quantity;
                              END IF;
                           END IF;
                        ELSE
                           LV_GETQTY :=
                              SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                        INSTR (R_OFFER.OFFR_NTES_TXT,
                                               TRIM (LV_FIELD5),
                                               1,
                                               1)
                                      + LENGTH (TRIM (LV_FIELD5)),
                                      1);
                        END IF;
                     END IF;
                  ELSE
                     IF TRIM (R_OFFER.Min_Get_Quantity) IS NULL
                     THEN
                        RAISE PARAMETERS_NULL;
                     ELSE
                        IF UF_IS_NUMERIC (R_OFFER.Min_Get_Quantity) = 0
                        THEN
                           LV_GETQTY := 0;
                        ELSE
                           LV_GETQTY := R_OFFER.Min_Get_Quantity;
                        END IF;
                     END IF;
                  END IF;

                  IF     TRIM (LV_FIELD4) IS NOT NULL
                     AND TRIM (LV_FIELD4) <> CHR (2)
                  THEN
                     IF UF_DOES_EXIST_SOME_STR (R_OFFER.OFFR_NTES_TXT,
                                                TRIM (LV_FIELD4)) = 0
                     THEN
                        IF TRIM (R_OFFER.MIN_QUALIFIER) IS NULL
                        THEN
                           RAISE PARAMETERS_NULL;
                        ELSE
                           IF UF_IS_NUMERIC (R_OFFER.MIN_QUALIFIER) = 0
                           THEN
                              LV_BUYQUALIFIER := 0;
                           ELSE
                              LV_BUYQUALIFIER := R_OFFER.MIN_QUALIFIER;
                           END IF;
                        END IF;
                     ELSE
                        IF UF_IS_NUMERIC (
                              SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                        INSTR (R_OFFER.OFFR_NTES_TXT,
                                               TRIM (LV_FIELD4),
                                               1,
                                               1)
                                      + LENGTH (TRIM (LV_FIELD4)),
                                      1)) = 0
                        THEN
                           IF TRIM (R_OFFER.MIN_QUALIFIER) IS NULL
                           THEN
                              RAISE PARAMETERS_NULL;
                           ELSE
                              IF UF_IS_NUMERIC (R_OFFER.MIN_QUALIFIER) = 0
                              THEN
                                 LV_BUYQUALIFIER := 0;
                              ELSE
                                 LV_BUYQUALIFIER := R_OFFER.MIN_QUALIFIER;
                              END IF;
                           END IF;
                        ELSE
                           LV_BUYQUALIFIER :=
                              SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                        INSTR (R_OFFER.OFFR_NTES_TXT,
                                               TRIM (LV_FIELD4),
                                               1,
                                               1)
                                      + LENGTH (TRIM (LV_FIELD4)),
                                      1);
                        END IF;
                     END IF;
                  ELSE
                     IF TRIM (R_OFFER.MIN_QUALIFIER) IS NULL
                     THEN
                        RAISE PARAMETERS_NULL;
                     ELSE
                        IF UF_IS_NUMERIC (R_OFFER.MIN_QUALIFIER) = 0
                        THEN
                           LV_BUYQUALIFIER := 0;
                        ELSE
                           LV_BUYQUALIFIER := R_OFFER.MIN_QUALIFIER;
                        END IF;
                     END IF;
                  END IF;

                  -- Get values into variables
                  --lv_odsofferid  := r_offer.offer_id;
                  lv_description :=
                        'C'
                     || SUBSTR (lv_current_campaign, 5, 2)
                     || 'OF'
                     || ' '
                     || r_offer.offr_desc_txt;

                  lv_description_pagenr := ' P.' || r_offer.page_nr || ' ';

                  IF r_offer.offr_BROCUHRE = '42'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'FH';
                  ELSIF r_offer.offr_BROCUHRE = '11'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'CF';
                  ELSIF r_offer.offr_BROCUHRE = '35'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'AC';
                  ELSIF r_offer.offr_BROCUHRE = '43'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'WE';
                  END IF;

                  lv_description :=
                     SUBSTR (lv_description,
                             1,
                             50 - LENGTH (lv_description_pagenr));
                  lv_description := lv_description || lv_description_pagenr;

                  --lv_description := SUBSTR (lv_description, 1, 50);

                  --lv_offertype   := r_offer.offer_type;
                  --lv_promoclaim  := r_offer.offer_promo_clm;

                  -- Process to insert the Offer Header information
                  INSERT INTO avOfferHeader (dataareaid,
                                             OfferId,
                                             GroupNumber,
                                             Description,
                                             Enabled,
                                             Priority,
                                             Campaign,
                                             DateFrom,
                                             DateTo,
                                             QualifyType,
                                             OfferType,
                                             BuyQualifier,
                                             GetQuantity,
                                             ProductType,
                                             Combine,
                                             BackOrder,
                                             Backout,
                                             OrMore,
                                             avDefault,
                                             avDistinct,
                                             AVZEROSTEP,
                                             OFFERFLAG,
                                             PROFILETYPE,
                                             AVLIMITBASE,
                                             AVLIMITQTY,
                                             AVSCHEDULETYPE,
                                             AVUSEBUYSETONCE,
                                             AVBYOFFERIDS,
                                             AVAPPLYONCE,
                                             AVINTERACTIVE,
                                             AVEXTDESC,
                                             AVUSETAGUPDATE,
                                             AVBYPASSMONITOREDITEMS,
                                             AVEXCEEDOPTION,
                                             AVdistinctDefault,
                                             AVUDT,
                                             AVUSEOP,
                                             AVOPDESC,
                                             AVOPCAPTION,
                                             AVOPPRIORITY,
                                             AVOPCATEGORY,
                                             AVOPMESSAGE,
                                             AVOPMESSAGEDUAL,
                                             AVOPMESSAGEGET,
                                             AVNODEFAULTITEM)
                       VALUES (Market_Code,
                               lv_offerid,
                               lv_group,
                               lv_description,
                               lv_enabled,
                               lv_priority,
                               lv_current_campaign,
                               TRUNC (SYSDATE),
                               TRUNC (SYSDATE),
                               lv_qualifytype,
                               lv_offertype,
                               lv_buyqualifier,
                               lv_getqty,
                               lv_producttype,
                               lv_combine,
                               lv_backorder,
                               lv_backout,
                               lv_ormore,
                               lv_default,
                               lv_distinct,
                               lv_zerostep,
                               lv_offerflag,
                               lv_profiletype,
                               lv_limitbase,
                               lv_limitqty,
                               lv_scheduletype,
                               lv_usebuysetone,
                               lv_byofferids,
                               lv_applyonce,
                               lv_interfactive,
                               lv_extdesc,
                               lv_usetagupdate,
                               lv_bypassmonitem,
                               lv_exceedoption,
                               lv_distedefault,
                               lv_udt,
                               lv_useop,
                               lv_opdesc,
                               lv_opcaption,
                               lv_oppriority,
                               lv_opcategory,
                               lv_opmessage,
                               lv_opmessagedual,
                               lv_opmesageget,
                               lv_nodefaultitem);

                  -- Call offer detail procedure
                  UPLOAD_DETAIL (Market_Code,
                                 lv_current_campaign,
                                 lv_odsofferid,
                                 lv_offertype,
                                 lv_offerid,
                                 lv_promoclaim,
                                 lv_offr_ntes_txt);

                  -- Insert informaiton in the Adapter Profile Table with value "Apply to All"
                  UPLOAD_PROFILE (Market_Code, lv_offerid);

                  -- Insert information in the Link Table to control not to upload the file several times
                  INSERT INTO ODS_OFFR_LINK (MAPS_OFFER_ID,
                                             PROMO_CLAIM_ID,
                                             GOMAC_OFFER_ID,
                                             OFFER_TYPE,
                                             dataareaid,
                                             campaign,
                                             createddate)
                       VALUES (lv_odsofferid,
                               lv_promoclaim,
                               lv_offerid,
                               lv_offertype,
                               Market_Code,
                               lv_current_campaign,
                               SYSDATE);

                  INSERT INTO AVUSERLOG (AVUSERID,
                                         AVADAPTER,
                                         AVACTION,
                                         AVADAPTERID,
                                         DATETIME,
                                         DATAAREAID)
                       VALUES (
                                 'ODS_GOMAC',
                                 'LOAD',
                                    'Offer_Type:'
                                 || r_offer.offer_type
                                 || ' - Prom_Claim_ID:'
                                 || r_offer.offer_promo_clm
                                 || ' - ODS Offer ID:'
                                 || r_offer.offer_id
                                 || ' - GOMAC Offer ID: '
                                 || lv_offerid,
                                 'UPK_ODS_GOMAC_LOAD',
                                 SYSDATE,
                                 Market_Code);

                  COMMIT;
               ELSIF r_offer.offer_type = 'STEP'
               THEN
                  -- Get values into variables

                  lv_description := r_offer.offr_desc_txt;

                  lv_description_pagenr := ' P.' || r_offer.page_nr || ' ';

                  IF r_offer.offr_BROCUHRE = '42'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'FH';
                  ELSIF r_offer.offr_BROCUHRE = '11'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'CF';
                  ELSIF r_offer.offr_BROCUHRE = '35'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'AC';
                  ELSIF r_offer.offr_BROCUHRE = '43'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'WE';
                  END IF;

                  lv_description :=
                     SUBSTR (
                        lv_description,
                        1,
                          50
                        - LENGTH (lv_description_pagenr)
                        - LENGTH ('C18OF '));
                  lv_description := lv_description || lv_description_pagenr;
                  --lv_offertype   := r_offer.offer_type;

                  --lv_promoclaim  := r_offer.offer_promo_clm;

                  -- Update Values in Step Price Header
                  UPLOAD_STEPPRICEHDR (Market_Code,
                                       lv_current_campaign,
                                       lv_odsofferid,
                                       lv_offertype,
                                       lv_offerid,
                                       lv_promoclaim,
                                       lv_description,
                                       lv_group,
                                       lv_qualifytype,
                                       lv_producttype,
                                       lv_priority,
                                       lv_ormore,
                                       lv_enabled,
                                       lv_combine,
                                       lv_backout,
                                       lv_backorder,
                                       lv_zerostep,
                                       lv_useop,
                                       lv_limitbase);

                  -- Insert informaiton in the Adapter Profile Table with value "Apply to All"
                  UPLOAD_PROFILE (Market_Code, lv_offerid);

                  -- Insert information in the Link Table to control not to upload the file several times
                  INSERT INTO ODS_OFFR_LINK (MAPS_OFFER_ID,
                                             PROMO_CLAIM_ID,
                                             GOMAC_OFFER_ID,
                                             OFFER_TYPE,
                                             dataareaid,
                                             campaign,
                                             createddate)
                       VALUES (lv_odsofferid,
                               lv_promoclaim,
                               lv_offerid,
                               lv_offertype,
                               Market_Code,
                               lv_current_campaign,
                               SYSDATE);

                  INSERT INTO AVUSERLOG (AVUSERID,
                                         AVADAPTER,
                                         AVACTION,
                                         AVADAPTERID,
                                         DATETIME,
                                         DATAAREAID)
                       VALUES (
                                 'ODS_GOMAC',
                                 'LOAD',
                                    'Offer_Type:'
                                 || r_offer.offer_type
                                 || ' - Prom_Claim_ID:'
                                 || r_offer.offer_promo_clm
                                 || ' - ODS Offer ID:'
                                 || r_offer.offer_id
                                 || ' - GOMAC Offer ID: '
                                 || lv_offerid,
                                 'UPK_ODS_GOMAC_LOAD',
                                 SYSDATE,
                                 Market_Code);

                  COMMIT;
               -- If (r_offer.offer_type = 'SPRC') or (r_offer.offer_type = 'GFRE')  then

               ELSIF     r_offer.offer_type = 'DEMO'
                     AND r_offer.offer_promo_clm = '40'
                     AND r_offer.sls_cls_cd = '4'
               THEN
                  IF     TRIM (LV_FIELD5) IS NOT NULL
                     AND TRIM (LV_FIELD5) <> CHR (2)
                  THEN
                     IF UF_DOES_EXIST_SOME_STR (R_OFFER.OFFR_NTES_TXT,
                                                TRIM (LV_FIELD5)) = 0
                     THEN
                        IF TRIM (R_OFFER.Min_Get_Quantity) IS NULL
                        THEN
                           RAISE PARAMETERS_NULL;
                        ELSE
                           IF UF_IS_NUMERIC (R_OFFER.Min_Get_Quantity) = 0
                           THEN
                              LV_GETQTY := 0;
                           ELSE
                              LV_GETQTY := R_OFFER.Min_Get_Quantity;
                           END IF;
                        END IF;
                     ELSE
                        IF UF_IS_NUMERIC (
                              SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                        INSTR (R_OFFER.OFFR_NTES_TXT,
                                               TRIM (LV_FIELD5),
                                               1,
                                               1)
                                      + LENGTH (TRIM (LV_FIELD5)),
                                      2)) = 0
                        THEN
                           IF TRIM (R_OFFER.Min_Get_Quantity) IS NULL
                           THEN
                              RAISE PARAMETERS_NULL;
                           ELSE
                              IF UF_IS_NUMERIC (R_OFFER.Min_Get_Quantity) = 0
                              THEN
                                 LV_GETQTY := 0;
                              ELSE
                                 LV_GETQTY := R_OFFER.Min_Get_Quantity;
                              END IF;
                           END IF;
                        ELSE
                           LV_GETQTY :=
                              SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                        INSTR (R_OFFER.OFFR_NTES_TXT,
                                               TRIM (LV_FIELD5),
                                               1,
                                               1)
                                      + LENGTH (TRIM (LV_FIELD5)),
                                      2);
                        END IF;
                     END IF;
                  ELSE
                     IF TRIM (R_OFFER.Min_Get_Quantity) IS NULL
                     THEN
                        RAISE PARAMETERS_NULL;
                     ELSE
                        IF UF_IS_NUMERIC (R_OFFER.Min_Get_Quantity) = 0
                        THEN
                           LV_GETQTY := 0;
                        ELSE
                           LV_GETQTY := R_OFFER.Min_Get_Quantity;
                        END IF;
                     END IF;
                  END IF;

                  IF     TRIM (LV_FIELD4) IS NOT NULL
                     AND TRIM (LV_FIELD4) <> CHR (2)
                  THEN
                     IF UF_DOES_EXIST_SOME_STR (R_OFFER.OFFR_NTES_TXT,
                                                TRIM (LV_FIELD4)) = 0
                     THEN
                        IF TRIM (R_OFFER.MIN_QUALIFIER) IS NULL
                        THEN
                           RAISE PARAMETERS_NULL;
                        ELSE
                           IF UF_IS_NUMERIC (R_OFFER.MIN_QUALIFIER) = 0
                           THEN
                              LV_BUYQUALIFIER := 0;
                           ELSE
                              LV_BUYQUALIFIER := R_OFFER.MIN_QUALIFIER;
                           END IF;
                        END IF;
                     ELSE
                        IF UF_IS_NUMERIC (
                              SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                        INSTR (R_OFFER.OFFR_NTES_TXT,
                                               TRIM (LV_FIELD4),
                                               1,
                                               1)
                                      + LENGTH (TRIM (LV_FIELD4)),
                                      2)) = 0
                        THEN
                           IF TRIM (R_OFFER.MIN_QUALIFIER) IS NULL
                           THEN
                              RAISE PARAMETERS_NULL;
                           ELSE
                              IF UF_IS_NUMERIC (R_OFFER.MIN_QUALIFIER) = 0
                              THEN
                                 LV_BUYQUALIFIER := 0;
                              ELSE
                                 LV_BUYQUALIFIER := R_OFFER.MIN_QUALIFIER;
                              END IF;
                           END IF;
                        ELSE
                           LV_BUYQUALIFIER :=
                              SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                        INSTR (R_OFFER.OFFR_NTES_TXT,
                                               TRIM (LV_FIELD4),
                                               1,
                                               1)
                                      + LENGTH (TRIM (LV_FIELD4)),
                                      2);
                        END IF;
                     END IF;
                  ELSE
                     IF TRIM (R_OFFER.MIN_QUALIFIER) IS NULL
                     THEN
                        RAISE PARAMETERS_NULL;
                     ELSE
                        IF UF_IS_NUMERIC (R_OFFER.MIN_QUALIFIER) = 0
                        THEN
                           LV_BUYQUALIFIER := 0;
                        ELSE
                           LV_BUYQUALIFIER := R_OFFER.MIN_QUALIFIER;
                        END IF;
                     END IF;
                  END IF;

                  -- Get values into variables
                  --lv_odsofferid  := r_offer.offer_id;
                  lv_description :=
                        'C'
                     || SUBSTR (lv_current_campaign, 5, 2)
                     || 'OF'
                     || ' '
                     || r_offer.offr_desc_txt;

                  lv_description_pagenr := ' P.' || r_offer.page_nr || ' ';

                  IF r_offer.offr_BROCUHRE = '42'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'FH';
                  ELSIF r_offer.offr_BROCUHRE = '11'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'CF';
                  ELSIF r_offer.offr_BROCUHRE = '35'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'AC';
                  ELSIF r_offer.offr_BROCUHRE = '43'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'WE';
                  END IF;

                  lv_description :=
                     SUBSTR (lv_description,
                             1,
                             50 - LENGTH (lv_description_pagenr));
                  lv_description := lv_description || lv_description_pagenr;

                  --lv_description := SUBSTR (lv_description, 1, 50);

                  -- Process to insert the Offer Header information
                  INSERT INTO avOfferHeader (dataareaid,
                                             OfferId,
                                             GroupNumber,
                                             Description,
                                             Enabled,
                                             Priority,
                                             Campaign,
                                             DateFrom,
                                             DateTo,
                                             QualifyType,
                                             OfferType,
                                             BuyQualifier,
                                             GetQuantity,
                                             ProductType,
                                             Combine,
                                             BackOrder,
                                             Backout,
                                             OrMore,
                                             avDefault,
                                             avDistinct,
                                             AVZEROSTEP,
                                             OFFERFLAG,
                                             PROFILETYPE,
                                             AVLIMITBASE,
                                             AVLIMITQTY,
                                             AVSCHEDULETYPE,
                                             AVUSEBUYSETONCE,
                                             AVBYOFFERIDS,
                                             AVAPPLYONCE,
                                             AVINTERACTIVE,
                                             AVEXTDESC,
                                             AVUSETAGUPDATE,
                                             AVBYPASSMONITOREDITEMS,
                                             AVEXCEEDOPTION,
                                             AVdistinctDefault,
                                             AVUDT,
                                             AVUSEOP,
                                             AVOPDESC,
                                             AVOPCAPTION,
                                             AVOPPRIORITY,
                                             AVOPCATEGORY,
                                             AVOPMESSAGE,
                                             AVOPMESSAGEDUAL,
                                             AVOPMESSAGEGET,
                                             AVNODEFAULTITEM)
                       VALUES (Market_Code,
                               lv_offerid,
                               lv_group,
                               lv_description,
                               lv_enabled,
                               lv_priority,
                               lv_current_campaign,
                               TRUNC (SYSDATE),
                               TRUNC (SYSDATE),
                               lv_qualifytype,
                               lv_offertype,
                               lv_buyqualifier,
                               lv_getqty,
                               lv_producttype,
                               lv_combine,
                               lv_backorder,
                               lv_backout,
                               lv_ormore,
                               lv_default,
                               lv_distinct,
                               lv_zerostep,
                               lv_offerflag,
                               lv_profiletype,
                               lv_limitbase,
                               lv_limitqty,
                               lv_scheduletype,
                               lv_usebuysetone,
                               lv_byofferids,
                               lv_applyonce,
                               lv_interfactive,
                               lv_extdesc,
                               lv_usetagupdate,
                               lv_bypassmonitem,
                               lv_exceedoption,
                               lv_distedefault,
                               lv_udt,
                               lv_useop,
                               lv_opdesc,
                               lv_opcaption,
                               lv_oppriority,
                               lv_opcategory,
                               lv_opmessage,
                               lv_opmessagedual,
                               lv_opmesageget,
                               lv_nodefaultitem);

                  -- Call offer detail procedure
                  UPLOAD_DETAIL (Market_Code,
                                 lv_current_campaign,
                                 lv_odsofferid,
                                 lv_offertype,
                                 lv_offerid,
                                 lv_promoclaim,
                                 lv_offr_ntes_txt);

                  -- Insert informaiton in the Adapter Profile Table with value "Apply to All"
                  UPLOAD_PROFILE (Market_Code, lv_offerid);

                  -- Insert information in the Link Table to control not to upload the file several times
                  INSERT INTO ODS_OFFR_LINK (MAPS_OFFER_ID,
                                             PROMO_CLAIM_ID,
                                             GOMAC_OFFER_ID,
                                             OFFER_TYPE,
                                             dataareaid,
                                             campaign,
                                             createddate)
                       VALUES (lv_odsofferid,
                               lv_promoclaim,
                               lv_offerid,
                               lv_offertype,
                               Market_Code,
                               lv_current_campaign,
                               SYSDATE);

                  INSERT INTO AVUSERLOG (AVUSERID,
                                         AVADAPTER,
                                         AVACTION,
                                         AVADAPTERID,
                                         DATETIME,
                                         DATAAREAID)
                       VALUES (
                                 'ODS_GOMAC',
                                 'LOAD',
                                    'Offer_Type:'
                                 || r_offer.offer_type
                                 || ' - Prom_Claim_ID:'
                                 || r_offer.offer_promo_clm
                                 || ' - ODS Offer ID:'
                                 || r_offer.offer_id
                                 || ' - GOMAC Offer ID: '
                                 || lv_offerid,
                                 'UPK_ODS_GOMAC_LOAD',
                                 SYSDATE,
                                 Market_Code);

                  COMMIT;
               ELSIF r_offer.offer_type = 'LIMIT'
               THEN
                  lv_description :=
                        'C'
                     || SUBSTR (lv_current_campaign, 5, 2)
                     || 'PL'
                     || ' '
                     || r_offer.offr_desc_txt;

                  lv_description_pagenr := ' P.' || r_offer.page_nr || ' ';

                  IF r_offer.offr_BROCUHRE = '42'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'FH';
                  ELSIF r_offer.offr_BROCUHRE = '11'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'CF';
                  ELSIF r_offer.offr_BROCUHRE = '35'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'AC';
                  ELSIF r_offer.offr_BROCUHRE = '43'
                  THEN
                     lv_description_pagenr := lv_description_pagenr || 'WE';
                  END IF;

                  lv_description :=
                     SUBSTR (lv_description,
                             1,
                             50 - LENGTH (lv_description_pagenr));
                  lv_description := lv_description || lv_description_pagenr;

                  --lv_description := SUBSTR (lv_description, 1, 50);

                  lv_productlimitid :=
                     GOMAC.UF_GET_GM_SEQNO (Market_Code, lv_seqno);

                  SELECT TO_NUMBER (
                              TO_CHAR (SYSDATE, 'HH24') * 60 * 60
                            + TO_CHAR (SYSDATE, 'MI') * 60
                            + TO_CHAR (SYSDATE, 'SS'))
                    INTO lv_modified_time
                    FROM DUAL;

                  IF     TRIM (LV_FIELD6) IS NOT NULL
                     AND TRIM (LV_FIELD6) <> CHR (2)
                  THEN
                     IF UF_DOES_EXIST_SOME_STR (R_OFFER.OFFR_NTES_TXT,
                                                TRIM (LV_FIELD6)) = 0
                     THEN
                        IF TRIM (R_OFFER.LIMIT_QTY) IS NULL
                        THEN
                           LV_LIMITQTY := 0;
                        ELSE
                           IF UF_IS_NUMERIC (R_OFFER.LIMIT_QTY) = 0
                           THEN
                              LV_LIMITQTY := 0;
                           ELSE
                              LV_LIMITQTY := R_OFFER.LIMIT_QTY;
                           END IF;
                        END IF;
                     ELSE
                        IF UF_IS_NUMERIC (
                              SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                        INSTR (R_OFFER.OFFR_NTES_TXT,
                                               TRIM (LV_FIELD6),
                                               1,
                                               1)
                                      + LENGTH (TRIM (LV_FIELD6)),
                                      1)) = 0
                        THEN
                           IF TRIM (R_OFFER.LIMIT_QTY) IS NULL
                           THEN
                              LV_LIMITQTY := 0;
                           ELSE
                              IF UF_IS_NUMERIC (R_OFFER.LIMIT_QTY) = 0
                              THEN
                                 LV_LIMITQTY := 0;
                              ELSE
                                 LV_LIMITQTY := R_OFFER.LIMIT_QTY;
                              END IF;
                           END IF;
                        ELSE
                           LV_LIMITQTY :=
                              SUBSTR (R_OFFER.OFFR_NTES_TXT,
                                        INSTR (R_OFFER.OFFR_NTES_TXT,
                                               TRIM (LV_FIELD6),
                                               1,
                                               1)
                                      + LENGTH (TRIM (LV_FIELD6)),
                                      1);
                        END IF;
                     END IF;
                  ELSE
                     IF TRIM (R_OFFER.LIMIT_QTY) IS NULL
                     THEN
                        LV_LIMITQTY := 0;
                     ELSE
                        IF UF_IS_NUMERIC (R_OFFER.LIMIT_QTY) = 0
                        THEN
                           LV_LIMITQTY := 0;
                        ELSE
                           LV_LIMITQTY := R_OFFER.LIMIT_QTY;
                        END IF;
                     END IF;
                  END IF;

                  IF LV_LIMITQTY > 0
                  THEN
                     INSERT INTO AvProductLimit (avproductlimitid,
                                                 description,
                                                 avdatefrom,
                                                 avdateto,
                                                 avcampaign,
                                                 avactive,
                                                 avaccumulate,
                                                 avcampaignto,
                                                 avusecamporder,
                                                 avrejectorder,
                                                 avlimitproduct,
                                                 dataareaid,
                                                 avprocessseq,
                                                 modifieddate,
                                                 modifiedtime,
                                                 modifiedby,
                                                 createddate,
                                                 createdtime,
                                                 createdby)
                          VALUES (lv_productlimitid,
                                  lv_description,
                                  TRUNC (SYSDATE),
                                  TRUNC (SYSDATE),
                                  lv_field7,
                                  1,
                                  0,
                                  lv_field8,
                                  0,
                                  0,
                                  0,
                                  Market_Code,
                                  ' ',
                                  TRUNC (SYSDATE),
                                  lv_modified_time,
                                  lv_createdby,
                                  TRUNC (SYSDATE),
                                  lv_modified_time,
                                  lv_createdby);

                     INSERT INTO AvProductLimitGroup (avproductlimitid,
                                                      avprofileid,
                                                      avlimitqty,
                                                      dataareaid)
                          VALUES (lv_productlimitid,
                                  --lv_global_profile_id,
                                  0,
                                  LV_LIMITQTY,
                                  Market_Code);

                     INSERT INTO AvAdapterProfile (avadapter,
                                                   avprofileid,
                                                   avadapterid,
                                                   avflag,
                                                   avgroup,
                                                   dataareaid)
                          VALUES (11,
                                  lv_global_profile_id,
                                  lv_productlimitid,
                                  0,
                                  0,
                                  Market_Code);

                     UPLOAD_DETAIL (Market_Code,
                                    lv_current_campaign,
                                    lv_odsofferid,
                                    lv_offertype,
                                    lv_productlimitid,
                                    lv_promoclaim,
                                    lv_offr_ntes_txt);

                     INSERT INTO ODS_OFFR_LINK (MAPS_OFFER_ID,
                                                PROMO_CLAIM_ID,
                                                GOMAC_OFFER_ID,
                                                OFFER_TYPE,
                                                dataareaid,
                                                campaign,
                                                createddate)
                          VALUES (r_offer.offer_id,
                                  r_offer.offer_promo_clm,
                                  lv_productlimitid,
                                  'Limit',
                                  Market_Code,
                                  lv_field7,
                                  SYSDATE);

                     INSERT INTO AVUSERLOG (AVUSERID,
                                            AVADAPTER,
                                            AVACTION,
                                            AVADAPTERID,
                                            DATETIME,
                                            DATAAREAID)
                          VALUES (
                                    'ODS_GOMAC',
                                    'LOAD',
                                       'Offer_Type: Limit'
                                    || ' - Prom_Claim_ID:'
                                    || r_offer.offer_promo_clm
                                    || ' - ODS Offer ID:'
                                    || r_offer.offer_id
                                    || ' - GOMAC Offer ID: '
                                    || lv_productlimitid,
                                    'UPK_ODS_GOMAC_LOAD',
                                    SYSDATE,
                                    Market_Code);
                  END IF;
               ELSIF LENGTH (TRIM (R_OFFER.offer_type)) = 0
               THEN
                  RAISE PARAMETERS_NULL;
               END IF;
            ELSE
               INSERT INTO AVUSERLOG (AVUSERID,
                                      AVADAPTER,
                                      AVACTION,
                                      AVADAPTERID,
                                      DATETIME,
                                      DATAAREAID)
                    VALUES (
                              'ODS_GOMAC',
                              'OFFER_EXIST',
                                 'Already Exist -  ODS Offer ID: '
                              || r_offer.offer_id,
                              'UPK_ODS_GOMAC_LOAD',
                              SYSDATE,
                              Market_Code);
            END IF;
         END IF;
      END LOOP;

      --As requested by Arthur Lu(Lu Jian),need to set finishing time after beginning time
      --so we delay the end_log by manual way like below:

      --Log the finish of job message
      -- Log Finish
      INSERT INTO AVUSERLOG (AVUSERID,
                             AVADAPTER,
                             AVACTION,
                             AVADAPTERID,
                             DATETIME,
                             DATAAREAID)
           VALUES (
                     'ODS_GOMAC',
                     'LOAD',
                     'ODS to GOMAC Interface Finished',
                     'UPK_ODS_GOMAC_LOAD',
                     --SYSDATE,
                     TO_DATE (
                           TO_CHAR (SYSDATE, 'yyyy/mm/dd hh24:mi')
                        || ':'
                        || LPAD (
                              TO_CHAR (
                                 DECODE (
                                    TO_NUMBER (TO_CHAR (SYSDATE, 'ss')) + 1,
                                    60, 0,
                                    TO_NUMBER (TO_CHAR (SYSDATE, 'ss')) + 1)),
                              2,
                              '0'),
                        'yyyy/mm/dd hh24:mi:ss'),
                     Market_Code);

      COMMIT;
   EXCEPTION
      WHEN PARAMETERS_NULL
      THEN
         INSERT INTO AVUSERLOG (AVUSERID,
                                AVADAPTER,
                                AVACTION,
                                AVADAPTERID,
                                DATETIME,
                                DATAAREAID)
              VALUES (
                        'ODS_GOMAC',
                        'LOAD',
                        'ODS to GOMAC Interface Finished',
                        'UPK_ODS_GOMAC_LOAD',
                        --SYSDATE,
                        TO_DATE (
                              TO_CHAR (SYSDATE, 'yyyy/mm/dd hh24:mi')
                           || ':'
                           || LPAD (
                                 TO_CHAR (
                                    DECODE (
                                         TO_NUMBER (TO_CHAR (SYSDATE, 'ss'))
                                       + 1,
                                       60, 0,
                                         TO_NUMBER (TO_CHAR (SYSDATE, 'ss'))
                                       + 1)),
                                 2,
                                 '0'),
                           'yyyy/mm/dd hh24:mi:ss'),
                        Market_Code);

         INSERT INTO AVUSERLOG (AVUSERID,
                                AVADAPTER,
                                AVACTION,
                                AVADAPTERID,
                                DATETIME,
                                DATAAREAID)
              VALUES (
                        'ODS_GOMAC',
                        'LOAD',
                           'ODS to GOMAC Issue:'
                        || DBMS_UTILITY.format_error_backtrace
                        || ' '
                        || 'Please check the parameter value(s) because of null value(s).',
                        'UPK_ODS_GOMAC_LOAD',
                        SYSDATE,
                        Market_Code);

         COMMIT;
         RAISE;
      WHEN OTHERS
      THEN
         INSERT INTO AVUSERLOG (AVUSERID,
                                AVADAPTER,
                                AVACTION,
                                AVADAPTERID,
                                DATETIME,
                                DATAAREAID)
              VALUES (
                        'ODS_GOMAC',
                        'LOAD',
                        'ODS to GOMAC Interface Finished',
                        'UPK_ODS_GOMAC_LOAD',
                        --SYSDATE,
                        TO_DATE (
                              TO_CHAR (SYSDATE, 'yyyy/mm/dd hh24:mi')
                           || ':'
                           || LPAD (
                                 TO_CHAR (
                                    DECODE (
                                         TO_NUMBER (TO_CHAR (SYSDATE, 'ss'))
                                       + 1,
                                       60, 0,
                                         TO_NUMBER (TO_CHAR (SYSDATE, 'ss'))
                                       + 1)),
                                 2,
                                 '0'),
                           'yyyy/mm/dd hh24:mi:ss'),
                        Market_Code);

         -- Log Error
         lv2_sqlmsg := SUBSTR (SQLERRM, 1, 255);

         INSERT INTO AVUSERLOG (AVUSERID,
                                AVADAPTER,
                                AVACTION,
                                AVADAPTERID,
                                DATETIME,
                                DATAAREAID)
              VALUES (
                        'ODS_GOMAC',
                        'LOAD',
                           'ODS to GOMAC Issue:'
                        || ' ODS_OFFER_ID - '
                        || lv_global_offer_id
                        || '-'
                        || DBMS_UTILITY.format_error_backtrace
                        || lv2_sqlmsg,
                        'UPK_ODS_GOMAC_LOAD',
                        SYSDATE,
                        Market_Code);

         COMMIT;
         RAISE;
   END USP_ODS_GOMAC_MAIN;
END UPK_ODS_GOMAC_LOAD;
/