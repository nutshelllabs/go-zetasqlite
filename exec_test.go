package zetasqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	zetasqlite "github.com/nutshelllabs/go-zetasqlite"
	"github.com/google/go-cmp/cmp"
)

func TestExec(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	ctx = zetasqlite.WithCurrentTime(ctx, now)
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, test := range []struct {
		name        string
		query       string
		args        []interface{}
		expectedErr bool
	}{
		{
			name: "create table with all types",
			query: `
CREATE TABLE _table_a (
 intValue        INT64,
 boolValue       BOOL,
 doubleValue     DOUBLE,
 floatValue      FLOAT,
 stringValue     STRING,
 bytesValue      BYTES,
 numericValue    NUMERIC,
 bignumericValue BIGNUMERIC,
 intervalValue   INTERVAL,
 dateValue       DATE,
 datetimeValue   DATETIME,
 timeValue       TIME,
 timestampValue  TIMESTAMP
)`,
		},
		{
			name: "create table as select",
			query: `
CREATE TABLE foo ( id STRING PRIMARY KEY NOT NULL, name STRING );
CREATE TABLE bar ( id STRING, name STRING, PRIMARY KEY (id, name) );
CREATE OR REPLACE TABLE new_table_as_select AS (
  SELECT t1.id, t2.name FROM foo t1 JOIN bar t2 ON t1.id = t2.id
);
`,
		},
		{
			name: "recreate table",
			query: `
CREATE OR REPLACE TABLE recreate_table ( a string );
DROP TABLE recreate_table;
CREATE TABLE recreate_table ( b string );
INSERT recreate_table (b) VALUES ('hello');
`,
		},
		{
			name: "insert select",
			query: `
CREATE OR REPLACE TABLE TableA(product string, quantity int64);
INSERT TableA (product, quantity) SELECT 'top load washer', 10;
INSERT INTO TableA (product, quantity) SELECT * FROM UNNEST([('microwave', 20), ('dishwasher', 30)]);
`,
		},
		{
			name: "transaction",
			query: `
CREATE OR REPLACE TABLE Inventory
(
 product string,
 quantity int64,
 supply_constrained bool
);

CREATE OR REPLACE TABLE NewArrivals
(
 product string,
 quantity int64,
 warehouse string
);

INSERT Inventory (product, quantity)
VALUES('top load washer', 10),
     ('front load washer', 20),
     ('dryer', 30),
     ('refrigerator', 10),
     ('microwave', 20),
     ('dishwasher', 30);

INSERT NewArrivals (product, quantity, warehouse)
VALUES('top load washer', 100, 'warehouse #1'),
     ('dryer', 200, 'warehouse #2'),
     ('oven', 300, 'warehouse #1');

BEGIN TRANSACTION;

CREATE TEMP TABLE tmp AS SELECT * FROM NewArrivals WHERE warehouse = 'warehouse #1';
DELETE NewArrivals WHERE warehouse = 'warehouse #1';
MERGE Inventory AS I
USING tmp AS T
ON I.product = T.product
WHEN NOT MATCHED THEN
 INSERT(product, quantity, supply_constrained)
 VALUES(product, quantity, false)
WHEN MATCHED THEN
 UPDATE SET quantity = I.quantity + T.quantity;

TRUNCATE TABLE tmp;

COMMIT TRANSACTION;
`,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if _, err := db.ExecContext(ctx, test.query); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNestedStructFieldAccess(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	ctx = zetasqlite.WithCurrentTime(ctx, now)
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `
CREATE TABLE table (
  id INT64,
  value STRUCT<fieldA STRING, fieldB STRUCT<fieldX STRING, fieldY STRING>>
)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(
		ctx,
		`INSERT table (id, value) VALUES (?, ?)`,
		123,
		map[string]interface{}{
			"fieldB": map[string]interface{}{
				"fieldY": "bar",
			},
		},
	); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, "SELECT value, value.fieldB, value.fieldB.fieldY FROM table")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type queryRow struct {
		Value  interface{}
		FieldB []map[string]interface{}
		FieldY string
	}
	var results []*queryRow
	for rows.Next() {
		var (
			value  interface{}
			fieldB []map[string]interface{}
			fieldY string
		)
		if err := rows.Scan(&value, &fieldB, &fieldY); err != nil {
			t.Fatal(err)
		}
		results = append(results, &queryRow{
			Value:  value,
			FieldB: fieldB,
			FieldY: fieldY,
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("failed to get results")
	}
	if results[0].FieldY != "bar" {
		t.Fatalf("failed to get fieldY")
	}
}

func TestCreateTempTable(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	ctx = zetasqlite.WithCurrentTime(ctx, now)
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "CREATE TEMP TABLE tmp_table (id INT64)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TEMP TABLE tmp_table (id INT64)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE tmp_table (id INT64)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE tmp_table (id INT64)"); err == nil {
		t.Fatal("expected error")
	}
}

func TestDeclare(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	ctx = zetasqlite.WithCurrentTime(ctx, now)
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
  if _, err := db.ExecContext(ctx, `
CREATE TABLE ` + "`prj.broadridge.ttrans_description`" + `
(
  line_desc_nbr INT64,
  language_cd STRING,
  account_cd STRING,
  branch_cd STRING,
  chck_brch_acct_nbr STRING,
  client_nbr STRING,
  currency_cd STRING,
  debit_credit_cd STRING,
  trans_hist_seq_nbr INT64,
  security_adp_nbr STRING,
  trans_acct_hist_cd STRING,
  transaction_dt DATETIME,
  type_account_cd STRING,
  processing_dt DATETIME,
  desc_trans_sec_txt STRING,
  action_cd STRING,
  rec_type_cd STRING,
  rr_cd STRING,
  desc_id_nbr STRING,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`prj.broadridge.tprchs_sale_trans`" + `
(
  account_cd STRING,
  branch_cd STRING,
  chck_brch_acct_nbr STRING,
  client_nbr STRING,
  currency_cd STRING,
  debit_credit_cd STRING,
  trans_hist_seq_nbr INT64,
  security_adp_nbr STRING,
  trans_acct_hist_cd STRING,
  transaction_dt DATETIME,
  type_account_cd STRING,
  prc_sec_amt NUMERIC,
  desc_line_qty INT64,
  blttr_mrkt_cd STRING,
  blttr_cpcty_cd STRING,
  int_accrd_bond_amt NUMERIC,
  broker_contra_cd STRING,
  date_trade_sec_cd STRING,
  cncl_cmmsn_trd_cd STRING,
  rr_trans_cd STRING,
  wthld_tax_cd STRING,
  spcl_trade_cd STRING,
  int_bond_frqnc_cd STRING,
  do_not_use STRING,
  tax_state_amt NUMERIC,
  fee_sec_amt NUMERIC,
  pstg_chrg_amt NUMERIC,
  desc_add_prnt_cd STRING,
  instr_buy_cd STRING,
  gnrl_prps_fee_amt NUMERIC,
  broker_exec_cd STRING,
  frst_mny_amt NUMERIC,
  account_clsfn_cd STRING,
  ggrph_lctn_cd STRING,
  branch_prft_ctr_cd STRING,
  language_cd STRING,
  psr_olts_cd STRING,
  grnte_grntr_cd STRING,
  cmmsn_amt NUMERIC,
  coupon_date_cd STRING,
  cusip_nbr STRING,
  extnt_whi_dt DATETIME,
  ordr_trade_nbr STRING,
  prc_basis_amt NUMERIC,
  comm_cd STRING,
  prfg_comm_amt NUMERIC,
  branch_sttl_cd STRING,
  systm_gnrt_ind STRING,
  wthld_account_amt NUMERIC,
  type_optn_cd STRING,
  bond_dated_cd STRING,
  type_dtd_bond_cd STRING,
  type_sec_pos6_cd STRING,
  type_sec_pos7_cd STRING,
  type_sec_pos5_cd STRING,
  type_sec_pos4_cd STRING,
  type_sec_pos3_cd STRING,
  type_sec_pos2_cd STRING,
  type_security_cd STRING,
  exchange_cd STRING,
  tag_tran_cd STRING,
  int_bond_rt NUMERIC,
  mtrty_bond_dt DATETIME,
  instr_spcl_cd STRING,
  tran_total_amt NUMERIC,
  share_trans_qty NUMERIC,
  chrg_cmmsn_trd_cd STRING,
  status_tax_cd STRING,
  trade_as_of_cd STRING,
  spcl_prcs_cd STRING,
  instr_sell_cd STRING,
  prc_strike_amt NUMERIC,
  sec_adp_undrl_nbr STRING,
  dividend_rt NUMERIC,
  crdt_grss_amt NUMERIC,
  cnvrt_trd_crrn_rt NUMERIC,
  currency_trd_cd STRING,
  cncl_prcss_dt DATETIME,
  prcs_orgn_tran_dt DATETIME,
  cdg_cusip_cd STRING,
  rec_type_cd STRING,
  action_cd STRING,
  cusip_extend_nbr STRING,
  share_undln_qty NUMERIC,
  rr_cd STRING,
  processing_dt DATETIME,
  srce_tran_cd STRING,
  branch_offset_cd STRING,
  account_offset_cd STRING,
  typ_acct_offset_cd STRING,
  chk_acct_offset_cd STRING,
  trade_dt DATETIME,
  split_trade_ind STRING,
  issue_when_ps_cd STRING,
  issue_when_ind STRING,
  cmmsn_dscnt_pct NUMERIC,
  crspn_charge_amt NUMERIC,
  trade_charge_amt NUMERIC,
  yield_type_cd STRING,
  yield_pct NUMERIC,
  term_updt_last_id STRING,
  last_updt_user_cd STRING,
  online_change_dt STRING,
  client_use_txt STRING,
  trade_tmstp_txt STRING,
  trailer2_cd STRING,
  trailer3_cd STRING,
  dns_station_nbr STRING,
  entry_cd STRING,
  cmmsn_dscnt_cd STRING,
  cmmsn_ds_pnlty_amt NUMERIC,
  bony_dtc_clr_cd STRING,
  split_offset_id STRING,
  rgnzn_trans_cd STRING,
  trailer4_cd STRING,
  trailer5_cd STRING,
  direct_route_ind STRING,
  pymt_mthd_cd STRING,
  alph_exec_brkr_cd STRING,
  rsn_rbll_cncl_cd STRING,
  rr_scndy_cd STRING,
  trd_tmstp_ss_txt STRING,
  bsns_unit_txt STRING,
  trace_cd STRING,
  trailer6_cd STRING,
  trailer7_cd STRING,
  trailer8_cd STRING,
  trailer9_cd STRING,
  trailer10_cd STRING,
  dtl_cmprs_rcrd_cd STRING,
  yield_sign_ind STRING,
  mrkt_prc_ind STRING,
  tle_misc_txt STRING,
  exp_trd_ordr_nbr STRING,
  crncy_prcss_ind STRING,
  rttm_str_cntl_nbr STRING,
  msrb_control_nbr STRING,
  client_order_id STRING,
  execution_id STRING,
  option_open_code STRING,
  utc_trade_tmstp STRING,
  pmp_amt NUMERIC,
  non_ins_ind STRING,
  pmp_step_ind STRING,
  prc_trans_mumd_amt NUMERIC,
  prc_trans_mumd_pct NUMERIC,
  prc_trans_mumd_sign STRING,
  prc_trans_mumd_ind STRING,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`prj.broadridge.tsec_xref_key`" + `
(
  type_xref_cd STRING,
  cross_reference_cd STRING,
  security_adp_nbr STRING,
  record_type_cd STRING,
  action_cd STRING,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`prj.broadridge.trd_anls_sttlm_dt`" + `
(
  client_nbr STRING,
  branch_cd STRING,
  account_cd STRING,
  currency_cd STRING,
  type_account_cd STRING,
  security_adp_nbr STRING,
  issue_when_ind STRING,
  seq_nbr INT64,
  settlement_dt_qty NUMERIC,
  int_freq_cd STRING,
  sd_cost_amt NUMERIC,
  memo_int_amt NUMERIC,
  dvdnd_int_amt NUMERIC,
  gross_cr_amt NUMERIC,
  ernd_int_amt NUMERIC,
  sd_pl_rlzd_amt NUMERIC,
  sd_bal_amt NUMERIC,
  action_cd STRING,
  record_type_cd STRING,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`prj.broadridge.tact_l1_name_address`" + `
(
  raw_id STRING,
  name_and_address_segment_id_code STRING,
  name_and_address_segment_footprint STRING,
  name_and_address_segment_length STRING,
  filler_15 STRING,
  profit_center_branch_number STRING,
  net_payout_to_rr STRING,
  psr_olts_indicator STRING,
  guaranteed_guarantor_indicator STRING,
  cdcc_clearing_code STRING,
  related_account_number STRING,
  settlement_office STRING,
  invoice_confirm_code STRING,
  confirm_hold_code STRING,
  language_code STRING,
  geographic_location STRING,
  address_location_code_alc STRING,
  account_classification STRING,
  u_s_tax_withholding_tefra_code STRING,
  compression_indicator STRING,
  new_york_state_residence_code STRING,
  valid_account_types STRING,
  managed_account_indicator STRING,
  buy_instructions STRING,
  sell_instructions STRING,
  bond_commission_code STRING,
  stock_commission_code STRING,
  currency_of_account STRING,
  custodial_flip_indicator STRING,
  stock_commission_rate_per_share STRING,
  bond_commission_rate_per_share STRING,
  rio_indicator STRING,
  transient_number STRING,
  basp_number STRING,
  dtc_id_number STRING,
  fee_exempt STRING,
  telex_fax_indicator STRING,
  expanded_bond_commission_code STRING,
  expanded_stock_commission_code STRING,
  broker_clearing_number STRING,
  prime_broker_code STRING,
  cod_cor_indicator STRING,
  electronic_confirm_indicator STRING,
  money_manager_code STRING,
  order_counter_exempt_flag STRING,
  date_account_was_opened STRING,
  dda_code STRING,
  automatic_cash_managment STRING,
  acm_vendor_number STRING,
  msrb_reporting_indicator STRING,
  division STRING,
  product_type STRING,
  service_type STRING,
  taxlot_method STRING,
  taxlot_convert_indicator STRING,
  taxlot_activated_date STRING,
  alternate_branch STRING,
  managed_account_product_code STRING,
  confirm_count STRING,
  mifid_confirm_eligiblity_code STRING,
  mifid_statement_eligiblity_code STRING,
  no_name_and_address_flag STRING,
  restriction_indicator STRING,
  restriction_indicator_value STRING,
  expanded_account_classification STRING,
  n_a_ach_code STRING,
  ins_code STRING,
  no_more_b_cash STRING,
  no_more_b_mrgn STRING,
  rr_number STRING,
  c_90_day_restrict STRING,
  div_withhold_rate STRING,
  int_withhold_rate STRING,
  grs_withhold_rate STRING,
  min_exec STRING,
  cuid STRING,
  nad3_com STRING,
  wrap_code STRING,
  private_codes_1_10 STRING,
  margin_agreement_indicator STRING,
  non_member_affiliate_indicator STRING,
  filler_206 STRING,
  name_and_address_segment_delimiter STRING,
  business_date DATE,
  file_path STRING,
  file_date DATE,
  imported_at TIMESTAMP
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`prj.broadridge.tprchs_sale_fee`" + `
(
  client_nbr STRING,
  processing_dt DATETIME,
  branch_cd STRING,
  account_cd STRING,
  currency_cd STRING,
  type_account_cd STRING,
  chck_brch_acct_nbr STRING,
  security_adp_nbr STRING,
  trans_acct_hist_cd STRING,
  trans_hist_seq_nbr INT64,
  transaction_dt DATETIME,
  debit_credit_cd STRING,
  fee_type_cd STRING,
  action_cd STRING,
  rec_type_cd STRING,
  fee_amt NUMERIC,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`prj.broadridge.tsecurity_desc`" + `
(
  language_cd STRING,
  line_txt_nbr INT64,
  security_adp_nbr STRING,
  desc_sec_txt STRING,
  record_type_cd STRING,
  action_cd STRING,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`prj.broadridge.tacc_party_name`" + `
(
  client_nbr STRING,
  branch_cd STRING,
  account_cd STRING,
  ap_seq_nbr INT64,
  action STRING,
  record_type_cd STRING,
  rr_cd STRING,
  ap_type_cd STRING,
  person_cmpy_cd STRING,
  confirm_qty INT64,
  statement_qty INT64,
  transfers_ip_ind STRING,
  proxies_ip_ind STRING,
  income_chck_ip_ind STRING,
  online_chck_ip_ind STRING,
  id_system_ip_ind STRING,
  cod_ip_ind STRING,
  cor_ip_ind STRING,
  prcss_1099_ind STRING,
  ip_seg_added_dt DATETIME,
  ip_seg_add_rt_dt DATETIME,
  ip_seg_changed_dt DATETIME,
  ip_seg_chng_rt_dt DATETIME,
  first_nm STRING,
  mi_initial_txt STRING,
  last_nm STRING,
  title_sffx3_txt STRING,
  title_prfx4_txt STRING,
  title_prfx8_txt STRING,
  company_nm STRING,
  language_cd STRING,
  dtc_id STRING,
  soc_sec_tax_cd STRING,
  participant_cd STRING,
  branch_ip_rotng_cd STRING,
  soc_sec_nbr STRING,
  householding_cd STRING,
  print_trid_cd STRING,
  customer_nbr STRING,
  dtc_clearing_id STRING,
  eltro_stmt_cd STRING,
  eltro_cnfrm_cd STRING,
  soc_sec2_nbr STRING,
  soc_sec2_tax_cd STRING,
  soc_sec3_nbr STRING,
  soc_sec3_tax_cd STRING,
  soc_sec4_nbr STRING,
  soc_sec4_tax_cd STRING,
  ina_ip_tag_cd STRING,
  birth_joint_dt DATETIME,
  na_line_chng_dt DATETIME,
  updt_last_tmstp DATETIME,
  eltro_prspc_cd STRING,
  eltro_proxy_cd STRING,
  eltro_1099_cd STRING,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`%1$s.%4$s.tpp_info`" + `
(
  client_nbr STRING,
  branch_cd STRING,
  account_cd STRING,
  ap_seq_nbr INT64,
  action STRING,
  record_type_cd STRING,
  rr_cd STRING,
  birth_dt DATETIME,
  last_nm STRING,
  mailing_address_cd STRING,
  tax_address_cd STRING,
  residency_cd STRING,
  state_cd STRING,
  zip5_cd STRING,
  zip4_cd STRING,
  marital_status_cd STRING,
  mrtl_stts_chng_dt DATETIME,
  dependents_qty INT64,
  dpndt_chng_dt DATETIME,
  income_lvl_cd STRING,
  income_lvl_chng_dt DATETIME,
  net_worth_cd STRING,
  net_worth_chng_dt DATETIME,
  lqd_net_worth_cd STRING,
  short_nm STRING,
  lqd_net_chng_dt DATETIME,
  tax_sps_id STRING,
  tax_brkt_cd STRING,
  tax_brkt_chng_dt DATETIME,
  rent_own_cd STRING,
  rent_own_chng_dt DATETIME,
  profession_cd STRING,
  prfsn_chng_dt DATETIME,
  edctn_lvl_cd STRING,
  edctn_chng_dt DATETIME,
  cust_rspns_cd STRING,
  cust_rspns_chng_dt DATETIME,
  referal_cd STRING,
  cntry_rsdnc_cd STRING,
  home_net_worth_cd STRING,
  found_cust_cd STRING,
  addrs_unknw_dt DATETIME,
  pin_nbr STRING,
  pin_chng_dt DATETIME,
  mother_maiden_nm STRING,
  cr_bank_branch_nm STRING,
  cr_contact_nm STRING,
  profession_sps_cd STRING,
  employed_since_dt DATETIME,
  account_sps_cd STRING,
  branch2_sps_cd STRING,
  account2_sps_cd STRING,
  prfsn_chng_sps_dt DATETIME,
  first_sps_nm STRING,
  mi_initial_sps_txt STRING,
  last_sps_nm STRING,
  birth_sps_dt DATETIME,
  branch_sps_cd STRING,
  cr_check_ind STRING,
  cr_acct_open_yr_cd STRING,
  cr_bank_nm STRING,
  cr_rating_cd STRING,
  emp_rltd_nm STRING,
  exch_emp_ind STRING,
  rltd_emp_desc_txt STRING,
  exch_emp_dtl1_txt STRING,
  exch_emp_dtl2_txt STRING,
  cust_met_ind STRING,
  cust_known_dt DATETIME,
  cr_rgltr_cnstr_cd STRING,
  zip_foreign_cd STRING,
  postal6_canada_cd STRING,
  proxy_mail_cd STRING,
  cmmsn_stk_ovrrd_cd STRING,
  cmmsn_bnd_ovrrd_cd STRING,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`%1$s.%3$s.AccountMaster`" + `
(
  AccountMasterID INT64,
  AccountNumber STRING,
  ClientNumber STRING,
  CorrespondentID INT64,
  CorrespondentCode STRING,
  CorrespondentOfficeID INT64,
  OfficeCode STRING,
  Title STRING,
  FirstName STRING,
  MiddleName STRING,
  LastName STRING,
  CompanyName STRING,
  TaxIDType STRING,
  TaxIDNumber STRING,
  DateofBirth DATETIME,
  AddressLine1 STRING,
  AddressLine2 STRING,
  AddressLine3 STRING,
  AddressLine4 STRING,
  City STRING,
  State STRING,
  ZipCode STRING,
  ZipCodeExtension STRING,
  CountryCode STRING,
  MailingAddressCode STRING,
  MailingListCode STRING,
  TaxAddressCode STRING,
  ResidencyCode STRING,
  HomePhone STRING,
  HomePhoneExtension STRING,
  WorkPhone STRING,
  WorkPhoneExtension STRING,
  CellPhone STRING,
  CellPhoneExtension STRING,
  EmailAddress STRING,
  ClassificationCode STRING,
  CustomerCode STRING,
  RepCode STRING,
  RestrictionCode STRING,
  ForeignCode STRING,
  IRACode STRING,
  MMSweep STRING,
  OptionLevel STRING,
  OpenDate DATETIME,
  ClosedDate DATETIME,
  ClosedIndicator STRING,
  ActivityDate DATETIME,
  LastUpdated DATETIME,
  LastUpdatedBy STRING,
  AccountName1 STRING,
  AccountName2 STRING,
  AccountName3 STRING,
  AccountName4 STRING,
  ZipForeignCode STRING,
  PostalCanadaCode STRING,
  DTCNumber STRING,
  AgentID STRING,
  InstitutionalID STRING,
  InternalAccountNumber STRING,
  ForeignDividendRate NUMERIC,
  ForeignInterestRate NUMERIC,
  FDID STRING,
  CATAccountType STRING,
  OATSAccountType STRING,
  Margin STRING,
  W8CertificationDate DATETIME,
  IRSExempt STRING,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- CLUSTER BY AccountNumber;
;

CREATE TABLE ` + "`%1$s.%3$s.MarketHoliday`" + `
(
  MarketCode STRING,
  HolidayDate DATETIME,
  HolidayName STRING,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
);

CREATE TABLE ` + "`%1$s.%3$s.Correspondent`" + `
(
  CorrespondentID INT64,
  GLCode STRING,
  CorrespondentCode STRING,
  Firm STRING,
  CurrencyCode STRING,
  CorrespondentGroupID INT64,
  Closed INT64,
  CorrespondentName STRING,
  Address1 STRING,
  Address2 STRING,
  City STRING,
  State STRING,
  ZipCode STRING,
  Country STRING,
  LanguageID INT64,
  MaintenanceRequirementFirmLong NUMERIC,
  MaintenanceRequirementFirmShort NUMERIC,
  MaintenanceRequirementCorrespondentLong NUMERIC,
  MaintenanceRequirementCorrespondentShort NUMERIC,
  NetLiquidatingEquity INT64,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
);

CREATE TABLE ` + "`prj.broadridge.tmsd_base`" + `
(
  security_adp_nbr STRING,
  div_ann_est_amt NUMERIC,
  record_type_cd STRING,
  action_cd STRING,
  payment_freq_cd STRING,
  msd_class1_cd STRING,
  msd_class2_cd STRING,
  msd_class3_cd STRING,
  msd_class4_cd STRING,
  msd_class5_cd STRING,
  msd_class6_cd STRING,
  msd_class7_cd STRING,
  country_cd STRING,
  currency_cd STRING,
  class_industry_cd STRING,
  sic_cd STRING,
  marginability_cd STRING,
  source_pricing_cd STRING,
  desc_sec_line1_txt STRING,
  taxable_cd STRING,
  country_origin_cd STRING,
  expiration_dt DATETIME,
  royalty_ind STRING,
  remic_ind STRING,
  reit_ind STRING,
  sec_use_ind STRING,
  shr_otstd_thou_qty INT64,
  earning_share_amt NUMERIC,
  currency_iso_cd STRING,
  redenomination_dt DATETIME,
  currency_legacy_cd STRING,
  curr_legacy_iso_cd STRING,
  uts_canadian_cd STRING,
  annuity_cd STRING,
  swaps_ind STRING,
  oats_nasdaq_cd STRING,
  rgstr_bond_cd STRING,
  foreign_cd STRING,
  security_ida_cd STRING,
  depository_cd STRING,
  trnfr_dptry_cd STRING,
  ibm_cd STRING,
  mcgill_cd NUMERIC,
  issue_dt DATETIME,
  elig_tax_crdt_ind STRING,
  tax_credit_rt NUMERIC,
  tax_withold_rt NUMERIC,
  symbol_extended_cd STRING,
  dllr_us_trade_ind STRING,
  taxable_can_cd STRING,
  trnfr_chrg_amt NUMERIC,
  fctr_file_rt NUMERIC,
  qssp_crnt_pct INT64,
  qssp_prev_pct INT64,
  sector_cd STRING,
  etf_ind STRING,
  shrt_sl_elgbl_ind STRING,
  shrt_sl_elgbl_dt DATETIME,
  multiply_price_cd STRING,
  refer_to_sec_nbr STRING,
  dnu_reason_txt STRING,
  dvdnd_qlfy_ind STRING,
  dvdnd_qlfy_ovr_ind STRING,
  ranking_cd STRING,
  bkpg_drs_cd STRING,
  pprls_lgl_ind STRING,
  actual_360_ind STRING,
  drct_rgstn_ind STRING,
  exchg_prime_cd STRING,
  mrkt_tier_cd STRING,
  mrkt_ind_cd STRING,
  prmry_naics_cd STRING,
  issr_type_cd STRING,
  dly_trd_volume_qty NUMERIC,
  msd_added_dt DATETIME,
  mfdsc_ind STRING,
  llc_ind STRING,
  msd_dnu_dt DATETIME,
  tips_ind STRING,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date
-- CLUSTER BY security_adp_nbr;
;

CREATE TABLE ` + "`prj.broadridge.toption_data`" + `
(
  record_type_cd STRING,
  security_adp_nbr STRING,
  action_cd STRING,
  strike_price_amt NUMERIC,
  expiration_dt DATETIME,
  factor_pct NUMERIC,
  scrty_adp_base_nbr STRING,
  type_option_cd STRING,
  maturity_aclrtd_dt DATETIME,
  dlvrbl_factor_pct NUMERIC,
  occ_optn_sym_id STRING,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date
-- CLUSTER BY security_adp_nbr;
;

CREATE TABLE ` + "`prj.broadridge.taccount_new`" + `
(
  client_nbr STRING,
  branch_cd STRING,
  account_cd STRING,
  account_clsfn_cd STRING,
  sell_cd STRING,
  buy_cd STRING,
  dividend_cd STRING,
  credit_interest_cd STRING,
  credit_interest_rt NUMERIC,
  debit_interest_cd STRING,
  debit_interest_rt NUMERIC,
  rr_cd STRING,
  spreading_ind STRING,
  prospectus_ind STRING,
  optn_no_trdg_ind STRING,
  cash_no_bsns_ind STRING,
  margin_no_bsns_ind STRING,
  cmdty_no_bsns_ind STRING,
  restriction_90_ind STRING,
  tax_usa_wthld_s_cd STRING,
  cash_management_cd STRING,
  cash_sweep_cd STRING,
  ach_accounts_cd STRING,
  grnte_grntr_cd STRING,
  loan_agreement_cd STRING,
  joint_agreement_cd STRING,
  margin_agrmt_ind STRING,
  trust_agrmt_ind STRING,
  corp_resolution_cd STRING,
  know_your_cust_ind STRING,
  bnd_wthld_frgn_rt NUMERIC,
  stk_wthld_frgn_rt NUMERIC,
  cmmsn_stk_spcl_cd STRING,
  cmmsn_bnd_spcl_cd STRING,
  hold_confirm_cd STRING,
  currency_cd STRING,
  branch_prft_ctr_cd STRING,
  prvdt_fund_prev_cd STRING,
  provident_fund_cd STRING,
  year_end_record_cd STRING,
  broker_book_ind STRING,
  individual_cd STRING,
  proxy_dscls_cd STRING,
  tefra_change_dt DATETIME,
  rdmpt_check_cd STRING,
  reinvestment_cd STRING,
  funds_slot_nbr STRING,
  mrgn_acct_ins_cd STRING,
  sttlm_office_cd STRING,
  buy_dvdnd_frctn_cd STRING,
  prospectus_sent_dt DATETIME,
  optional_aprvl_dt DATETIME,
  income_optn_cr_ind STRING,
  put_sllng_nkd_ind STRING,
  writing_cvrd_ind STRING,
  writing_naked_ind STRING,
  purchase_ind STRING,
  active_last_dt DATETIME,
  due_dlgnc_prev_dt DATETIME,
  due_dlgnc_sent_dt DATETIME,
  due_diligence_cd STRING,
  transfer_cd STRING,
  postal_fee_cd STRING,
  acat_cd STRING,
  adt_dcmnt_spcl_cd STRING,
  adt_customer_cd STRING,
  account_option_cd STRING,
  mail_list_cd STRING,
  invoice_confirm_cd STRING,
  settlement_cd STRING,
  idntf_intrl_cd STRING,
  wrap_fee_exmpt_ind STRING,
  wrap_fee_amt NUMERIC,
  record_changed_dt DATETIME,
  record_added_dt DATETIME,
  adjmt_dtl_1099_ind STRING,
  federal_nbr STRING,
  irs_1042_rcpnt_cd STRING,
  attorney_power_dt DATETIME,
  attorney_power_cd STRING,
  record_type_cd STRING,
  action STRING,
  acat_rcv_brkr_nbr STRING,
  acat_dlvr_brkr_nbr STRING,
  atmtc_prd_pymnt_cd STRING,
  money_mgr_cd STRING,
  amex_transmit_cd STRING,
  acm_cancel_cd STRING,
  account_close_dt DATETIME,
  bsns_stopped_dt DATETIME,
  bsns_rnstt_dt DATETIME,
  user_ff_txt STRING,
  record_opened_dt DATETIME,
  due_diligence2_cd STRING,
  user_stts_acct_txt STRING,
  autx_access_cd STRING,
  autx_acronym_cd STRING,
  dvdnd_notice_ind STRING,
  erisa_ind STRING,
  hold_stmnt_ind STRING,
  stmnt_last_dt DATETIME,
  origin_broker_cd STRING,
  name_control_cd STRING,
  dscrt_acct_ind STRING,
  opt_trdg_agrmt_ind STRING,
  portfolio_svc_1_cd STRING,
  cash_agrmt_ind STRING,
  atty_paper_ind STRING,
  last_day_stmt_ind STRING,
  mult_dlvry_cd STRING,
  olts_ind STRING,
  fee_cd STRING,
  fee_exmpt_ind STRING,
  fee_freq_cd STRING,
  fee_override_pct NUMERIC,
  del_after_tax_ind STRING,
  ira_mtr_plan_cd STRING,
  rtrmt_pln_sbjct_cd STRING,
  rtrmt_plan_cd STRING,
  rtrmt_pln_clnt_cd STRING,
  invst_advsr_ind STRING,
  psr_ind STRING,
  rule_351_cd STRING,
  rule_80a_cd STRING,
  portfolio_svc_2_cd STRING,
  cmdty_aprvl_cd STRING,
  rr_prev_cd STRING,
  cmdty_acct_ind STRING,
  custody_acct_cd STRING,
  acct_internal_cd STRING,
  activity_last_dt DATETIME,
  custodial_flip_ind STRING,
  cstd_fee_exempt_cd STRING,
  related_cd STRING,
  brkr_exec_prim_cd STRING,
  cstd_fee_status_cd STRING,
  entitlement_ach_cd STRING,
  term_updt_last_id STRING,
  private_txt STRING,
  cmmsn_shr_stock_rt NUMERIC,
  cmmsn_shr_bond_rt NUMERIC,
  rio_ind STRING,
  invst_advsr_cd STRING,
  acct_aa_adv_cd STRING,
  acct_aa_adv_cdg_cd STRING,
  direct_dep_acct_cd STRING,
  tax_lot_ind STRING,
  acct_aa_adv_nm STRING,
  brkr_clearing_id STRING,
  clnt_hhld_key_cd STRING,
  clnt_hhld_updt_dt DATETIME,
  cr_chck_stts_cd STRING,
  cr_chck_rvw_dt DATETIME,
  funds_advn_ind STRING,
  ps_trd_cmprs_cd STRING,
  tax_usa_wthld_l_cd STRING,
  w8_s_cd STRING,
  w8_s_cert_dt DATETIME,
  w8_chg_dt DATETIME,
  w8_l_cd STRING,
  eqty_cmmsn_ovrd_cd STRING,
  ina_ind STRING,
  tefra_sub_cd STRING,
  srce_of_add_cd STRING,
  brkr_prim_nbr STRING,
  acm_vndr_nbr STRING,
  cncl_acm_vndr_nbr STRING,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`prj.broadridge.tdividend_trans`" + `
(
  account_cd STRING,
  branch_cd STRING,
  chck_brch_acct_nbr STRING,
  client_nbr STRING,
  currency_cd STRING,
  debit_credit_cd STRING,
  trans_hist_seq_nbr INT64,
  security_adp_nbr STRING,
  trans_acct_hist_cd STRING,
  transaction_dt DATETIME,
  type_account_cd STRING,
  share_trans_qty NUMERIC,
  entry_cd STRING,
  net_trans_amt NUMERIC,
  instr_spcl_cd STRING,
  mtrty_bond_dt DATETIME,
  int_bond_rt NUMERIC,
  cusip_issr_cd STRING,
  line_desc_msd_qty INT64,
  exchange_cd STRING,
  tax_sec_stts_cd STRING,
  tax_wthld_amt NUMERIC,
  dividend_paid_rt NUMERIC,
  tax_wthld_rt NUMERIC,
  instr_spcl_orgn_cd STRING,
  shr_pay_out_amt NUMERIC,
  type_wthld_cd STRING,
  cpn_pay_date_cd STRING,
  sec_adp_undrl_nbr STRING,
  type_security_cd STRING,
  cusip_extend_cd STRING,
  currency_div_cd STRING,
  crrn_cvrsn_rt NUMERIC,
  rcrd_dvdnd_cd STRING,
  type_sec_pos6_cd STRING,
  type_sec_pos7_cd STRING,
  type_sec_pos5_cd STRING,
  type_sec_pos4_cd STRING,
  type_sec_pos3_cd STRING,
  type_sec_pos2_cd STRING,
  dividend_rt NUMERIC,
  share_undln_qty NUMERIC,
  action_cd STRING,
  rec_type_cd STRING,
  cusip_cdg_cd STRING,
  rr_cd STRING,
  batch_cd STRING,
  processing_dt DATETIME,
  srce_tax_info_cd STRING,
  instr_spcl_cams_cd STRING,
  srce_trans_cd STRING,
  collctn_chrg_amt NUMERIC,
  rcrd_dvdnd_sub_cd STRING,
  us_wthld_cd STRING,
  fatca_wthld_ind STRING,
  fatca_elgblty_cd STRING,
  fatca_wthld_amt NUMERIC,
  fatca_wthld_rt NUMERIC,
  nra_wthld_amt NUMERIC,
  nra_wthld_rt NUMERIC,
  nra_drp_thr_wh_amt NUMERIC,
  nra_drp_thr_wh_rt NUMERIC,
  fatca_tax_cr_amt NUMERIC,
  fatca_tax_cr_cd STRING,
  na_geo_cd STRING,
  state_wthld_amt NUMERIC,
  state_wthld_rt NUMERIC,
  div_pay_ca_id STRING,
  rcrd_dt DATETIME,
  cntrt_pybl_dt DATETIME,
  ex_dt DATETIME,
  client_use_txt STRING,
  non_cash_pymt_ind STRING,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`prj.broadridge.tbookkeeping_trans`" + `
(
  account_cd STRING,
  branch_cd STRING,
  chck_brch_acct_nbr STRING,
  client_nbr STRING,
  currency_cd STRING,
  debit_credit_cd STRING,
  trans_hist_seq_nbr INT64,
  security_adp_nbr STRING,
  trans_acct_hist_cd STRING,
  transaction_dt DATETIME,
  type_account_cd STRING,
  share_undln_qty NUMERIC,
  batch_cd STRING,
  type_optn_cd STRING,
  type_dvdnd_rcrd_cd STRING,
  trans_value_dt DATETIME,
  cusip_nbr STRING,
  prcs_trans_cd STRING,
  srce_trans_cd STRING,
  dividend_rt NUMERIC,
  type_sec_pos3_cd STRING,
  type_security_cd STRING,
  sec_adp_undrl_nbr STRING,
  prc_strike_amt NUMERIC,
  shr_trns_dvdnd_qty NUMERIC,
  instr_spcl_misc_cd STRING,
  instr_spcl_cams_cd STRING,
  instr_spcl_orgn_cd STRING,
  price_close_amt NUMERIC,
  srce_tax_info_cd STRING,
  status_tax_cd STRING,
  cncl_prnt_ind STRING,
  exchange_cd STRING,
  line_desc_msd_qty INT64,
  cusip_issr_cd STRING,
  int_bond_rt NUMERIC,
  mtrty_bond_dt DATETIME,
  instr_spcl_cd STRING,
  tran_total_amt NUMERIC,
  tran_bkpg_amt NUMERIC,
  entry_cd STRING,
  share_trans_qty NUMERIC,
  type_sec_pos4_cd STRING,
  type_sec_pos5_cd STRING,
  type_sec_pos6_cd STRING,
  type_sec_pos2_cd STRING,
  type_sec_pos7_cd STRING,
  action_cd STRING,
  rec_type_cd STRING,
  cusip_extend_nbr STRING,
  cdg_cusip_cd STRING,
  rr_cd STRING,
  processing_dt DATETIME,
  term_updt_last_id STRING,
  last_updt_user_cd STRING,
  online_change_dt STRING,
  tax_wthld_rt NUMERIC,
  tax_wthld_amt NUMERIC,
  collctn_chrg_amt NUMERIC,
  asset_nbr_id STRING,
  rcrd_dvdnd_sub_cd STRING,
  bkkpg_cancel_cd STRING,
  rgnzn_trans_cd STRING,
  trailer2_cd STRING,
  trailer3_cd STRING,
  bkg_clnt_use_txt STRING,
  tle_misc_txt STRING,
  tle_trnfr_typ_cd STRING,
  gift_cd STRING,
  cbt_control_id STRING,
  cbt_cntra_pty_cd STRING,
  us_wthld_cd STRING,
  fatca_wthld_ind STRING,
  fatca_elgblty_cd STRING,
  fatca_wthld_amt NUMERIC,
  fatca_wthld_rt NUMERIC,
  nra_wthld_amt NUMERIC,
  nra_wthld_rt NUMERIC,
  nra_drp_thr_wh_amt NUMERIC,
  nra_drp_thr_wh_rt NUMERIC,
  fatca_tax_cr_amt NUMERIC,
  fatca_tax_cr_cd STRING,
  na_geo_cd STRING,
  state_wthld_amt NUMERIC,
  state_wthld_rt NUMERIC,
  dvdnd_eqvlnt_amt NUMERIC,
  option_open_code STRING,
  div_pay_ca_id STRING,
  non_cash_pymt_ind STRING,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`prj.broadridge.tincome_data`" + `
(
  record_dt DATETIME,
  type_income_cd STRING,
  security_adp_nbr STRING,
  exdividend_dt DATETIME,
  payment_dt DATETIME,
  record_type_cd STRING,
  action_cd STRING,
  dividend_rt NUMERIC,
  div_csh1_paytyp_cd STRING,
  div_csh2_paytyp_cd STRING,
  div_stk_paytyp_cd STRING,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`prj.broadridge.taccount_history`" + `
(
  tag_dcltn_nbr STRING,
  online_add_ind STRING,
  processing_dt DATETIME,
  item_dcltn_nbr STRING,
  batch_dcltn_nbr STRING,
  type_dcltn_cd STRING,
  location_cd STRING,
  rr_trans_cd STRING,
  credit_gross_amt NUMERIC,
  comm_amt NUMERIC,
  trade_settle_dt DATETIME,
  activity_ch_cd STRING,
  batch_cd STRING,
  entry_cd STRING,
  transaction_amt NUMERIC,
  transaction_qty NUMERIC,
  trade_dt DATETIME,
  seq_nbr INT64,
  type_tran_ch_cd STRING,
  transaction_dt DATETIME,
  currency_cd STRING,
  price_trd_amt_txt STRING,
  entry_income_cd STRING,
  account_cd STRING,
  branch_cd STRING,
  client_nbr STRING,
  type_account_cd STRING,
  security_adp_nbr STRING,
  record_type_cd STRING,
  action STRING,
  rr_cd STRING,
  chk_brch_acct_nbr STRING,
  tax_wthld_dvd_amt NUMERIC,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`prj.broadridge.taccount_hist_txt`" + `
(
  desc_history_txt STRING,
  seq_nbr INT64,
  type_account_cd STRING,
  account_cd STRING,
  branch_cd STRING,
  client_nbr STRING,
  currency_cd STRING,
  type_tran_ch_cd STRING,
  security_adp_nbr STRING,
  transaction_dt DATETIME,
  txt_line_nbr INT64,
  record_type_cd STRING,
  action STRING,
  rr_cd STRING,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY DATE(transaction_dt)
;

CREATE TABLE ` + "`prj.broadridge.tsec_trd_exchange`" + `
(
  exchange_msd_cd STRING,
  security_adp_nbr STRING,
  country_cd STRING,
  exch_primary_ind STRING,
  record_type_cd STRING,
  action_cd STRING,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`prj.broadridge.taccount_sec_hldr`" + `
(
  currency_cd STRING,
  issue_when_ind STRING,
  seq_nbr INT64,
  type_account_cd STRING,
  account_cd STRING,
  branch_cd STRING,
  client_nbr STRING,
  security_adp_nbr STRING,
  mrgn_hous_rqmt_amt NUMERIC,
  source_price_cd STRING,
  online_add_ind STRING,
  free_lock_cd STRING,
  holder_price_txt STRING,
  market_value_amt NUMERIC,
  trade_dt_qty NUMERIC,
  settlement_dt_qty NUMERIC,
  rr_cd STRING,
  cusip_intrl_nbr STRING,
  symbol_12 STRING,
  exchange_msd_cd STRING,
  activity_last_dt DATETIME,
  record_type_cd STRING,
  action STRING,
  change_last_dt DATETIME,
  today_total_qty NUMERIC,
  chk_brch_acct_nbr STRING,
  gi_today_total_qty NUMERIC,
  mrgn_hous_rqmt_pct INT64,
  sttlm_dt_mrkt_amt NUMERIC,
  nyse_req NUMERIC,
  regt_req NUMERIC,
  dly_accrd_intrst NUMERIC,
  pb_sd_mrkt_val_amt NUMERIC,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`%1$s.%3$s.BPSAMoneyMarketCodeXRef`" + `
(
  security_adp_nbr STRING,
  PensonMMCode STRING,
  FundName STRING,
  MoneyMarketDescription STRING,
  funds_slot_nbr STRING,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
);

CREATE TABLE ` + "`prj.broadridge.taccount_balance`" + `
(
  currency_cd STRING,
  account_cd STRING,
  branch_cd STRING,
  client_nbr STRING,
  last_bsns_actv_dt DATETIME,
  last_bkpg_actv_dt DATETIME,
  last_mrgn_actv_dt DATETIME,
  cash_bal_call_amt NUMERIC,
  house_bal_call_amt NUMERIC,
  maxcash_type12_amt NUMERIC,
  mmf_avlbl_tdy_amt NUMERIC,
  mmf_avlbl_ytdy_amt NUMERIC,
  ytd_mrgn_int_amt NUMERIC,
  sma_balance_amt NUMERIC,
  sma_bal_chg130_amt NUMERIC,
  same_day_sbst_amt NUMERIC,
  net_lmv_smv_amt NUMERIC,
  mrgn_regt_rqmt_amt NUMERIC,
  market_val_tot_amt NUMERIC,
  mrgn_csh_avlbl_amt NUMERIC,
  buying_pwr_tot_amt NUMERIC,
  tbill_byng_pwr_amt NUMERIC,
  noncv_bnd_bpwr_amt NUMERIC,
  muni_byng_pwr_amt NUMERIC,
  mmf_accmt_txt STRING,
  new_house_call_amt NUMERIC,
  house_equity_amt NUMERIC,
  equity_total_amt NUMERIC,
  spad_note_ind STRING,
  problem_note_cd STRING,
  ulstd_stk_optn_amt NUMERIC,
  unscr_db_bal_amt NUMERIC,
  super_rstrd_amt NUMERIC,
  online_add_ind STRING,
  reorg_postdt_ind STRING,
  optn_mrkt_val_amt NUMERIC,
  lqdtg_eqty_tot_amt NUMERIC,
  funds_nbr_cd STRING,
  record_type_cd STRING,
  action STRING,
  rr_cd STRING,
  opt_maint_rqrm_amt NUMERIC,
  call_nyse_amt NUMERIC,
  pending_amt NUMERIC,
  fund_na_cd STRING,
  fund_held_cd STRING,
  dytrd_buy_pwr_amt NUMERIC,
  pttrn_dytrd_ind STRING,
  dtrd_eqty_vltn_ind STRING,
  dytrd_rstrtn_ind STRING,
  dytrd_call_amt NUMERIC,
  pmv_amt NUMERIC,
  stk_cvbd_bypwr_amt NUMERIC,
  total_fed_call_amt NUMERIC,
  funds_exp_nbr_cd STRING,
  csh_put_frzn_fnds NUMERIC,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`prj.broadridge.tacc_type_balance`" + `
(
  type_account_cd STRING,
  account_cd STRING,
  branch_cd STRING,
  client_nbr STRING,
  currency_cd STRING,
  ydys_hse_exces_amt NUMERIC,
  ydys_equity_pct NUMERIC,
  ydys_equity_amt NUMERIC,
  ydys_mrkt_val_amt NUMERIC,
  ydys_trade_dt_amt NUMERIC,
  tdys_trade_dt_amt NUMERIC,
  ydys_settlm_dt_amt NUMERIC,
  tdys_settlm_dt_amt NUMERIC,
  record_type_cd STRING,
  action STRING,
  rr_cd STRING,
  chk_br_acct_ty_nbr STRING,
  class_acct_mrgn_cd STRING,
  gi_tdys_trd_dt_amt NUMERIC,
  gi_tdys_settlm_amt NUMERIC,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;

CREATE TABLE ` + "`prj.broadridge.tbond_data`" + `
(
  security_adp_nbr STRING,
  maturity_dt DATETIME,
  interest_rt NUMERIC,
  coupon_first_dt DATETIME,
  action_cd STRING,
  record_type_cd STRING,
  accrue_start_dt DATETIME,
  accrue_end_dt DATETIME,
  dated_dt DATETIME,
  dated_dt_cd STRING,
  date_coupon_cd STRING,
  evaluation_bond_cd STRING,
  day_delay_qty INT64,
  cmo_ind STRING,
  pool_nbr STRING,
  day_wt_avg_mat_qty NUMERIC,
  serial_bond_nbr STRING,
  state_cd STRING,
  avg_wght_cpn_amt NUMERIC,
  ins_jjk_cd STRING,
  date_pay_actual_cd STRING,
  prerefunded_dt DATETIME,
  prerefunded_rt NUMERIC,
  int_calcn_cd STRING,
  rate_bond_dt DATETIME,
  bond_ext_rt NUMERIC,
  pay_interest_dt DATETIME,
  defease_ind STRING,
  int_pay_cd STRING,
  updt_last_tmstp DATETIME,
  business_date DATE,
  gcptimestamp DATETIME,
  hvrintegseq STRING,
  hvrop INT64
)
-- PARTITION BY business_date;
;
`); err != nil {
    t.Fatal("unexpected error")
  }
	defer db.Close()
	rows, err := db.QueryContext(ctx, `
DECLARE last_business_date
DEFAULT (SELECT MAX(business_date)
           FROM ` + "`prj.broadridge.taccount_new`" + `
          WHERE 1=1
            AND business_date >= @business_date_start
            AND business_date <= @business_date_end);

DECLARE last_business_date_plus_1_day
DEFAULT (SELECT DATE_ADD(last_business_date, INTERVAL 1 DAY));

DECLARE business_date_start_minus_2_weeks
DEFAULT (SELECT DATE_ADD(@business_date_start, INTERVAL -2 WEEK));

DECLARE latest_opening_business_date
DEFAULT (SELECT MAX(business_date)
           FROM ` + "`prj.broadridge.taccount_new`" + `
          WHERE 1=1
            AND business_date >= business_date_start_minus_2_weeks
            AND business_date < @business_date_start);

DECLARE min_possible_opening_business_date
DEFAULT (SELECT coalesce(
         (SELECT DATE(MIN(t.stmnt_last_dt))
           FROM ` + "`prj.broadridge.taccount_new`" + ` t
          WHERE 1=1
            AND t.business_date = last_business_date
            AND t.stmnt_last_dt IS NOT NULL
            AND (
              CAST(@account_allowlist AS ARRAY<STRING>) IS NULL
              OR array_length(CAST(@account_allowlist AS ARRAY<STRING>)) = 0
              OR concat(t.branch_cd, '-', t.account_cd) IN UNNEST(CAST(@account_allowlist AS ARRAY<STRING>))
            )
            AND (
              CAST(@branch_allowlist AS ARRAY<STRING>) IS NULL
              OR array_length(CAST(@branch_allowlist AS ARRAY<STRING>)) = 0
              OR t.branch_cd IN UNNEST(CAST(@branch_allowlist AS ARRAY<STRING>))
            )
         ), latest_opening_business_date)
        );
DECLARE min_possible_opening_date
DEFAULT (SELECT DATE_TRUNC(DATE_ADD(DATE(min_possible_opening_business_date), INTERVAL 1 MONTH), MONTH));


WITH
account_code_mapping AS (
  SELECT '1' as type_account_cd, 'Cash' as name UNION ALL
  SELECT '2' as type_account_cd, 'Margin' as name UNION ALL
  SELECT '5' as type_account_cd, 'Short' as name UNION ALL
  SELECT '9' as type_account_cd, 'RVP/DVP' as name
),
accounts_with_trades AS (
  SELECT t.branch_cd, t.account_cd, count(*) as trade_count
  FROM ` + "`prj.broadridge.tprchs_sale_trans`" + ` t
  WHERE t.trans_acct_hist_cd <> 'G' -- ignore settlement records
    AND t.cncl_cmmsn_trd_cd IN ('1', '2') -- ignore cancels
    AND t.business_date >= @business_date_start
    AND t.business_date <= @business_date_end
    AND (
      CAST(@account_allowlist AS ARRAY<STRING>) IS NULL
      OR array_length(CAST(@account_allowlist AS ARRAY<STRING>)) = 0
      OR concat(t.branch_cd, '-', t.account_cd) IN UNNEST(CAST(@account_allowlist AS ARRAY<STRING>))
    )
    AND (
      CAST(@branch_allowlist AS ARRAY<STRING>) IS NULL
      OR array_length(CAST(@branch_allowlist AS ARRAY<STRING>)) = 0
      OR t.branch_cd IN UNNEST(CAST(@branch_allowlist AS ARRAY<STRING>))
    )
  GROUP BY t.branch_cd, t.account_cd
),
accounts_with_dividends AS (
  SELECT t.branch_cd, t.account_cd, count(*) as dividend_count
  FROM ` + "`prj.broadridge.tdividend_trans`" + ` t
  WHERE t.trans_acct_hist_cd <> 'G' -- ignore settlement records
    AND t.entry_cd = 'DIV' -- shouldn't be anything but 'DIV' but let's make sure it's true
    AND t.business_date >= @business_date_start
    AND t.business_date <= @business_date_end
    AND (
      CAST(@account_allowlist AS ARRAY<STRING>) IS NULL
      OR array_length(CAST(@account_allowlist AS ARRAY<STRING>)) = 0
      OR concat(t.branch_cd, '-', t.account_cd) IN UNNEST(CAST(@account_allowlist AS ARRAY<STRING>))
    )
    AND (
      CAST(@branch_allowlist AS ARRAY<STRING>) IS NULL
      OR array_length(CAST(@branch_allowlist AS ARRAY<STRING>)) = 0
      OR t.branch_cd IN UNNEST(CAST(@branch_allowlist AS ARRAY<STRING>))
    )
  GROUP BY t.branch_cd, t.account_cd
),
accounts_with_bookkeeping_stuff AS (
  SELECT t.branch_cd, t.account_cd, count(*) as bookkeeping_count
  FROM ` + "`prj.broadridge.tbookkeeping_trans`" + ` t
  WHERE t.trans_acct_hist_cd <> 'G' -- ignore settlement records
    AND t.entry_cd = 'DIV' -- only dividends for now, but add more stuff later
    AND t.business_date >= @business_date_start
    AND t.business_date <= @business_date_end
    AND (
      CAST(@account_allowlist AS ARRAY<STRING>) IS NULL
      OR array_length(CAST(@account_allowlist AS ARRAY<STRING>)) = 0
      OR concat(t.branch_cd, '-', t.account_cd) IN UNNEST(CAST(@account_allowlist AS ARRAY<STRING>))
    )
    AND (
      CAST(@branch_allowlist AS ARRAY<STRING>) IS NULL
      OR array_length(CAST(@branch_allowlist AS ARRAY<STRING>)) = 0
      OR t.branch_cd IN UNNEST(CAST(@branch_allowlist AS ARRAY<STRING>))
    )
  GROUP BY t.branch_cd, t.account_cd
),
eligible_accounts AS (
  SELECT
    a.branch_cd,
    a.account_cd,
    concat(a.branch_cd, a.account_cd) as AccountNumber,
    a.client_nbr,
    SUBSTR(a.client_nbr, -2) as Firm,
    case
      when coalesce(t.trade_count, 0) + coalesce(d.dividend_count, 0) + coalesce(bk.bookkeeping_count, 0) > 0 then (SELECT MAX(business_date)
           FROM ` + "`prj.broadridge.taccount_new`" + `
          WHERE 1=1
            AND business_date >= (SELECT DATE_ADD(@business_date_start, INTERVAL -2 WEEK))
            AND business_date < @business_date_start)
      else coalesce(DATE(a.stmnt_last_dt), (SELECT MAX(business_date)
           FROM ` + "`prj.broadridge.taccount_new`" + `
          WHERE 1=1
            AND business_date >= (SELECT DATE_ADD(@business_date_start, INTERVAL -2 WEEK))
            AND business_date < @business_date_start))
    end as opening_business_date,
    case
      when coalesce(t.trade_count, 0) + coalesce(d.dividend_count, 0) + coalesce(bk.bookkeeping_count, 0) > 0 then @business_date_start
      else DATE_TRUNC(DATE_ADD(DATE(a.stmnt_last_dt), INTERVAL 1 MONTH), MONTH)
    end as opening_date,
    -- is always last business date, see WHERE clause below
    a.business_date as last_business_date,
  FROM
    ` + "`prj.broadridge.taccount_new`" + ` a
    LEFT JOIN accounts_with_trades t
      ON a.branch_cd = t.branch_cd AND a.account_cd = t.account_cd
    LEFT JOIN accounts_with_dividends d
      ON a.branch_cd = d.branch_cd AND a.account_cd = d.account_cd
    LEFT JOIN accounts_with_bookkeeping_stuff bk
      ON a.branch_cd = bk.branch_cd AND a.account_cd = bk.account_cd
  WHERE a.business_date = (SELECT MAX(business_date)
           FROM ` + "`prj.broadridge.taccount_new`" + `
          WHERE 1=1
            AND business_date >= @business_date_start
            AND business_date <= @business_date_end)
    AND (
     -- is trade confirm...
     (DATE_DIFF(@business_date_end, @business_date_start, DAY) = 0 AND t.trade_count > 0)
     OR
     -- brokerage statement
     (DATE_DIFF(@business_date_end, @business_date_start, DAY) > 0
      AND
      (
        coalesce(t.trade_count, 0) + coalesce(d.dividend_count, 0) + coalesce(bk.bookkeeping_count, 0) > 0
        OR
        DATE_DIFF(@business_date_end, DATE(a.stmnt_last_dt), MONTH) = 3)
      )
    )
    AND (
      CAST(@account_allowlist AS ARRAY<STRING>) IS NULL
      OR array_length(CAST(@account_allowlist AS ARRAY<STRING>)) = 0
      OR concat(a.branch_cd, '-', a.account_cd) IN UNNEST(CAST(@account_allowlist AS ARRAY<STRING>))
    )
    AND (
      CAST(@branch_allowlist AS ARRAY<STRING>) IS NULL
      OR array_length(CAST(@branch_allowlist AS ARRAY<STRING>)) = 0
      OR a.branch_cd IN UNNEST(CAST(@branch_allowlist AS ARRAY<STRING>))
    )
)
-- prepend this file contents to any data that needs to go into statements--
-- <% bq_eligible_accounts_query.sql %>
--
,
p as (
  SELECT
    t.seq_nbr,
    t.branch_cd,
    t.account_cd,
    t.type_account_cd,
    t.security_adp_nbr,
    t.type_tran_ch_cd,
    t.entry_cd,
    t.transaction_dt,
    MAX(t.transaction_amt) as amount,
    MAX(t.transaction_qty) as quantity,
    MIN(t.business_date) as business_date,
  -- this one is 'broadridge' not 'broadridge_batch'!!!
  FROM ` + "`prj.broadridge.taccount_history`" + ` t
  WHERE t.entry_cd IN ('FND', 'RDM', 'JNL', 'ACH', 'TIN', 'INT', 'MMR')
    AND t.transaction_dt >= (SELECT DATE_TRUNC(DATE_ADD(DATE((SELECT coalesce(
         (SELECT DATE(MIN(t.stmnt_last_dt))
           FROM ` + "`prj.broadridge.taccount_new`" + ` t
          WHERE 1=1
            AND t.business_date = (SELECT MAX(business_date)
           FROM ` + "`prj.broadridge.taccount_new`" + `
          WHERE 1=1
            AND business_date >= @business_date_start
            AND business_date <= @business_date_end)
            AND t.stmnt_last_dt IS NOT NULL
            AND (
              CAST(@account_allowlist AS ARRAY<STRING>) IS NULL
              OR array_length(CAST(@account_allowlist AS ARRAY<STRING>)) = 0
              OR concat(t.branch_cd, '-', t.account_cd) IN UNNEST(CAST(@account_allowlist AS ARRAY<STRING>))
            )
            AND (
              CAST(@branch_allowlist AS ARRAY<STRING>) IS NULL
              OR array_length(CAST(@branch_allowlist AS ARRAY<STRING>)) = 0
              OR t.branch_cd IN UNNEST(CAST(@branch_allowlist AS ARRAY<STRING>))
            )
         ), (SELECT MAX(business_date)
           FROM ` + "`prj.broadridge.taccount_new`" + `
          WHERE 1=1
            AND business_date >= (SELECT DATE_ADD(@business_date_start, INTERVAL -2 WEEK))
            AND business_date < @business_date_start))
        )), INTERVAL 1 MONTH), MONTH))
    AND t.transaction_dt < (SELECT DATE_ADD((SELECT MAX(business_date)
           FROM ` + "`prj.broadridge.taccount_new`" + `
          WHERE 1=1
            AND business_date >= @business_date_start
            AND business_date <= @business_date_end), INTERVAL 1 DAY))
    AND (
      CAST(@account_allowlist AS ARRAY<STRING>) IS NULL
      OR array_length(CAST(@account_allowlist AS ARRAY<STRING>)) = 0
      OR concat(t.branch_cd, '-', t.account_cd) IN UNNEST(CAST(@account_allowlist AS ARRAY<STRING>))
    )
    AND (
      CAST(@branch_allowlist AS ARRAY<STRING>) IS NULL
      OR array_length(CAST(@branch_allowlist AS ARRAY<STRING>)) = 0
      OR t.branch_cd IN UNNEST(CAST(@branch_allowlist AS ARRAY<STRING>))
    )
  GROUP BY t.branch_cd, t.account_cd, t.type_account_cd, t.transaction_dt, t.type_tran_ch_cd, t.entry_cd, t.security_adp_nbr, t.seq_nbr
),
td_agg as (
  SELECT
    seq_nbr, branch_cd, account_cd, type_account_cd, security_adp_nbr, transaction_dt, type_tran_ch_cd,
    STRING_AGG(desc_history_txt, " " ORDER BY txt_line_nbr ASC) description,
  -- this one is 'broadridge' not 'broadridge_batch'!!!
  FROM (
    SELECT DISTINCT
      t.seq_nbr, t.branch_cd, t.account_cd, t.type_account_cd, t.security_adp_nbr, t.transaction_dt, t.type_tran_ch_cd, t.desc_history_txt, t.txt_line_nbr
    -- this one is 'broadridge' not 'broadridge_batch'!!!
    FROM ` + "`prj.broadridge.taccount_hist_txt`" + ` t
    WHERE t.action = 'A'
      AND t.record_type_cd = 'HIA'
      AND t.transaction_dt >= (SELECT DATE_TRUNC(DATE_ADD(DATE((SELECT coalesce(
         (SELECT DATE(MIN(t.stmnt_last_dt))
           FROM ` + "`prj.broadridge.taccount_new`" + ` t
          WHERE 1=1
            AND t.business_date = (SELECT MAX(business_date)
           FROM ` + "`prj.broadridge.taccount_new`" + `
          WHERE 1=1
            AND business_date >= @business_date_start
            AND business_date <= @business_date_end)
            AND t.stmnt_last_dt IS NOT NULL
            AND (
              CAST(@account_allowlist AS ARRAY<STRING>) IS NULL
              OR array_length(CAST(@account_allowlist AS ARRAY<STRING>)) = 0
              OR concat(t.branch_cd, '-', t.account_cd) IN UNNEST(CAST(@account_allowlist AS ARRAY<STRING>))
            )
            AND (
              CAST(@branch_allowlist AS ARRAY<STRING>) IS NULL
              OR array_length(CAST(@branch_allowlist AS ARRAY<STRING>)) = 0
              OR t.branch_cd IN UNNEST(CAST(@branch_allowlist AS ARRAY<STRING>))
            )
         ), (SELECT MAX(business_date)
           FROM ` + "`prj.broadridge.taccount_new`" + `
          WHERE 1=1
            AND business_date >= (SELECT DATE_ADD(@business_date_start, INTERVAL -2 WEEK))
            AND business_date < @business_date_start))
        )), INTERVAL 1 MONTH), MONTH))
      AND t.transaction_dt < (SELECT DATE_ADD((SELECT MAX(business_date)
           FROM ` + "`prj.broadridge.taccount_new`" + `
          WHERE 1=1
            AND business_date >= @business_date_start
            AND business_date <= @business_date_end), INTERVAL 1 DAY))
      AND (
        CAST(@account_allowlist AS ARRAY<STRING>) IS NULL
        OR array_length(CAST(@account_allowlist AS ARRAY<STRING>)) = 0
        OR concat(t.branch_cd, '-', t.account_cd) IN UNNEST(CAST(@account_allowlist AS ARRAY<STRING>))
      )
      AND (
        CAST(@branch_allowlist AS ARRAY<STRING>) IS NULL
        OR array_length(CAST(@branch_allowlist AS ARRAY<STRING>)) = 0
        OR t.branch_cd IN UNNEST(CAST(@branch_allowlist AS ARRAY<STRING>))
      )
    ORDER BY t.transaction_dt
  ) o
  GROUP BY branch_cd, account_cd, type_account_cd, transaction_dt, type_tran_ch_cd, security_adp_nbr, seq_nbr
  ORDER BY transaction_dt
)
SELECT
  case
    when entry_cd IN ('ACH', 'TIN') then 'C' -- cash movement
    when entry_cd IN ('RDM', 'FND', 'MMR') then 'FA' -- FDIC activity
    when entry_cd = 'JNL' then 'J' -- journal
    when entry_cd = 'INT' then 'I'
    else 'Unknown [' || entry_cd || ']'
  end as record_type,
  p.branch_cd || '-' || p.account_cd as account_code,
  p.security_adp_nbr as security_id,
  coalesce(act.name, 'Other') as account_type,

  -- cash movement specific stuff
  case
    when entry_cd = 'ACH' then 'MECHANISM_ACH'
    when entry_cd = 'TIN' then 'MECHANISM_TIN'
    else 'MECHANISM_UNDEFINED'
  end as mechanism,

  -- FDIC activity specific stuff
  case
    when entry_cd = 'FND' then 'TYPE_SWEEP'
    when entry_cd = 'RDM' then 'TYPE_REDEEM'
    when entry_cd = 'MMR' then 'TYPE_DIVIDEND_REINVESTMENT'
    else 'TYPE_UNDEFINED'
  end as fdic_activity_type,

  -- Journal specific stuff: nothing?

  -- Interest specific stuff
  case
    -- this one is weird because it's security_adp_nbr (2015086)
    -- doesn't match the one we have balance for (A000DN7)
    -- so let's just check the description
    when td_agg.description = 'THE INSURED DEPOSIT PROGRAM INTEREST' then 'INTEREST_TYPE_FDIC'
    else 'INTEREST_TYPE_UNSPECIFIED'
  end as interest_type,

  amount,
  quantity,
  td_agg.description as transaction_description,
  DATE(p.transaction_dt) as activity_date
FROM eligible_accounts a
  JOIN p ON p.branch_cd = a.branch_cd AND p.account_cd = a.account_cd
  LEFT JOIN account_code_mapping act ON p.type_account_cd = act.type_account_cd
  LEFT JOIN td_agg
    ON p.branch_cd = td_agg.branch_cd AND p.account_cd = td_agg.account_cd AND p.transaction_dt = td_agg.transaction_dt
      AND p.type_account_cd = td_agg.type_account_cd AND p.type_tran_ch_cd = td_agg.type_tran_ch_cd
      AND p.security_adp_nbr = td_agg.security_adp_nbr AND p.seq_nbr = td_agg.seq_nbr
WHERE p.transaction_dt >= a.opening_date
  AND p.transaction_dt < (SELECT DATE_ADD(last_business_date, INTERVAL 1 DAY))
ORDER BY record_type, p.transaction_dt
`,
  sql.Named("business_date_start", time.Now()),
  sql.Named("business_date_end", time.Now()),
  sql.Named("account_allowlist", nil),
  sql.Named("branch_allowlist", nil))
  if err != nil {
		t.Fatal(err)
	}
  defer rows.Close()
  if rows.Err() != nil {
    t.Fatal(rows.Err())
  }
}

func TestWildcardTable(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(
		ctx,
		"CREATE TABLE `project.dataset.table_a` AS SELECT specialName FROM UNNEST (['alice_a', 'bob_a']) as specialName",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(
		ctx,
		"CREATE TABLE `project.dataset.table_b` AS SELECT name FROM UNNEST(['alice_b', 'bob_b']) as name",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(
		ctx,
		"CREATE TABLE `project.dataset.table_c` AS SELECT name FROM UNNEST(['alice_c', 'bob_c']) as name",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(
		ctx,
		"CREATE TABLE `project.dataset.other_d` AS SELECT name FROM UNNEST(['alice_d', 'bob_d']) as name",
	); err != nil {
		t.Fatal(err)
	}
	t.Run("with first identifier", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, "SELECT name, _TABLE_SUFFIX FROM `project.dataset.table_*` WHERE name LIKE 'alice%' OR name IS NULL")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		type queryRow struct {
			Name   *string
			Suffix string
		}
		var results []*queryRow
		for rows.Next() {
			var (
				name   *string
				suffix string
			)
			if err := rows.Scan(&name, &suffix); err != nil {
				t.Fatal(err)
			}
			results = append(results, &queryRow{
				Name:   name,
				Suffix: suffix,
			})
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		stringPtr := func(v string) *string { return &v }
		if diff := cmp.Diff(results, []*queryRow{
			{Name: stringPtr("alice_c"), Suffix: "c"},
			{Name: stringPtr("alice_b"), Suffix: "b"},
			{Name: nil, Suffix: "a"},
			{Name: nil, Suffix: "a"},
		}); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
	})
	t.Run("without first identifier", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, "SELECT name, _TABLE_SUFFIX FROM `dataset.table_*` WHERE name LIKE 'alice%' OR name IS NULL")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		type queryRow struct {
			Name   *string
			Suffix string
		}
		var results []*queryRow
		for rows.Next() {
			var (
				name   *string
				suffix string
			)
			if err := rows.Scan(&name, &suffix); err != nil {
				t.Fatal(err)
			}
			results = append(results, &queryRow{
				Name:   name,
				Suffix: suffix,
			})
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		stringPtr := func(v string) *string { return &v }
		if diff := cmp.Diff(results, []*queryRow{
			{Name: stringPtr("alice_c"), Suffix: "c"},
			{Name: stringPtr("alice_b"), Suffix: "b"},
			{Name: nil, Suffix: "a"},
			{Name: nil, Suffix: "a"},
		}); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
	})
}

func TestTemplatedArgFunc(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	t.Run("simple any arguments", func(t *testing.T) {
		if _, err := db.ExecContext(
			ctx,
			`CREATE FUNCTION ANY_ADD(x ANY TYPE, y ANY TYPE) AS ((x + 4) / y)`,
		); err != nil {
			t.Fatal(err)
		}
		t.Run("int64", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, "SELECT ANY_ADD(3, 4)")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			rows.Next()
			var num float64
			if err := rows.Scan(&num); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(num) != "1.75" {
				t.Fatalf("failed to get max number. got %f", num)
			}
			if rows.Err() != nil {
				t.Fatal(rows.Err())
			}
		})
		t.Run("float64", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, "SELECT ANY_ADD(18.22, 11.11)")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			rows.Next()
			var num float64
			if err := rows.Scan(&num); err != nil {
				t.Fatal(err)
			}
			if num != 2.0 {
				t.Fatalf("failed to get max number. got %f", num)
			}
			if rows.Err() != nil {
				t.Fatal(rows.Err())
			}
		})
	})
	t.Run("array any arguments", func(t *testing.T) {
		if _, err := db.ExecContext(
			ctx,
			`CREATE FUNCTION MAX_FROM_ARRAY(arr ANY TYPE) as (( SELECT MAX(x) FROM UNNEST(arr) as x ))`,
		); err != nil {
			t.Fatal(err)
		}
		t.Run("int64", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, "SELECT MAX_FROM_ARRAY([1, 4, 2, 3])")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			rows.Next()
			var num int64
			if err := rows.Scan(&num); err != nil {
				t.Fatal(err)
			}
			if num != 4 {
				t.Fatalf("failed to get max number. got %d", num)
			}
			if rows.Err() != nil {
				t.Fatal(rows.Err())
			}
		})
		t.Run("float64", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, "SELECT MAX_FROM_ARRAY([1.234, 3.456, 4.567, 2.345])")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			rows.Next()
			var num float64
			if err := rows.Scan(&num); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(num) != "4.567" {
				t.Fatalf("failed to get max number. got %f", num)
			}
			if rows.Err() != nil {
				t.Fatal(rows.Err())
			}
		})
	})
}

func TestJavaScriptUDF(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	t.Run("operation", func(t *testing.T) {
		if _, err := db.ExecContext(
			ctx,
			`
CREATE FUNCTION multiplyInputs(x FLOAT64, y FLOAT64)
RETURNS FLOAT64
LANGUAGE js
AS r"""
  return x*y;
"""`,
		); err != nil {
			t.Fatal(err)
		}
		rows, err := db.QueryContext(ctx, `
WITH numbers AS
  (SELECT 1 AS x, 5 as y UNION ALL SELECT 2 AS x, 10 as y UNION ALL SELECT 3 as x, 15 as y)
  SELECT x, y, multiplyInputs(x, y) AS product FROM numbers`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		results := [][]float64{}
		for rows.Next() {
			var (
				x, y, retVal float64
			)
			if err := rows.Scan(&x, &y, &retVal); err != nil {
				t.Fatal(err)
			}
			results = append(results, []float64{x, y, retVal})
		}
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		if diff := cmp.Diff(results, [][]float64{
			{1, 5, 5},
			{2, 10, 20},
			{3, 15, 45},
		}); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
	})
	t.Run("function", func(t *testing.T) {
		if _, err := db.ExecContext(
			ctx,
			`
CREATE FUNCTION SumFieldsNamedFoo(json_row STRING)
RETURNS FLOAT64
LANGUAGE js
AS r"""
  function SumFoo(obj) {
    var sum = 0;
    for (var field in obj) {
      if (obj.hasOwnProperty(field) && obj[field] != null) {
        if (typeof obj[field] == "object") {
          sum += SumFoo(obj[field]);
        } else if (field == "foo") {
          sum += obj[field];
        }
      }
    }
    return sum;
  }
  var row = JSON.parse(json_row);
  return SumFoo(row);
"""`,
		); err != nil {
			t.Fatal(err)
		}
		rows, err := db.QueryContext(ctx, `
WITH Input AS (
  SELECT
    STRUCT(1 AS foo, 2 AS bar, STRUCT('foo' AS x, 3.14 AS foo) AS baz) AS s,
    10 AS foo
  UNION ALL
  SELECT
    NULL,
    4 AS foo
  UNION ALL
  SELECT
    STRUCT(NULL, 2 AS bar, STRUCT('fizz' AS x, 1.59 AS foo) AS baz) AS s,
    NULL AS foo
) SELECT TO_JSON_STRING(t), SumFieldsNamedFoo(TO_JSON_STRING(t)) FROM Input AS t`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		type queryRow struct {
			JsonRow string
			Sum     float64
		}
		results := []*queryRow{}
		for rows.Next() {
			var (
				jsonRow string
				sum     float64
			)
			if err := rows.Scan(&jsonRow, &sum); err != nil {
				t.Fatal(err)
			}
			results = append(results, &queryRow{JsonRow: jsonRow, Sum: sum})
		}
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		if diff := cmp.Diff(results, []*queryRow{
			{JsonRow: `{"s":{"foo":1,"bar":2,"baz":{"x":"foo","foo":3.14}},"foo":10}`, Sum: 14.14},
			{JsonRow: `{"s":null,"foo":4}`, Sum: 4},
			{JsonRow: `{"s":{"foo":null,"bar":2,"baz":{"x":"fizz","foo":1.59}},"foo":null}`, Sum: 1.59},
		}); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
	})
	t.Run("multibytes", func(t *testing.T) {
		if _, err := db.ExecContext(
			ctx,
			`
CREATE FUNCTION JS_JOIN(v ARRAY<STRING>)
RETURNS STRING
LANGUAGE js
AS r"""
  return v.join(' ');
"""`,
		); err != nil {
			t.Fatal(err)
		}
		rows, err := db.QueryContext(ctx, `SELECT JS_JOIN(['あいうえお', 'かきくけこ'])`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("failed to get result")
		}
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		if v != "あいうえお かきくけこ" {
			t.Fatalf("got %s", v)
		}
	})
	t.Run("struct", func(t *testing.T) {
		if _, err := db.ExecContext(
			ctx,
			`
CREATE FUNCTION structToArray(obj STRUCT<idx INT64, name STRING>)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
  let result = []

  result.push(obj["idx"])
  result.push(obj["name"])
  return result;
""";
`,
		); err != nil {
			t.Fatal(err)
		}
		rows, err := db.QueryContext(ctx, `SELECT * FROM UNNEST(structToArray(STRUCT(1,"A")))`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		var results []string
		for i := 0; i < 2; i++ {
			if !rows.Next() {
				t.Fatal("failed to get result")
			}
			var v string
			if err := rows.Scan(&v); err != nil {
				t.Fatal(err)
			}
			results = append(results, v)
		}
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		if !reflect.DeepEqual(results, []string{"1", "A"}) {
			t.Fatalf("failed to get results")
		}
	})
}
