// Copyright (c) 2023 Zion Dials <me@ziondials.com>
// Modifications Copyright (c) 2025 eds-ch
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package database

import (
	"fmt"
	"time"

	"github.com/eds-ch/Go-CDR-V/config"
	"github.com/eds-ch/Go-CDR-V/logger"
	"github.com/eds-ch/Go-CDR-V/models"
	"gorm.io/driver/clickhouse"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
)

// DataService represents a database service instance
type DataService struct {
	Session *gorm.DB
	Config  config.DatabaseConfig
}

func InitDB(dbConfig config.DatabaseConfig) *DataService {
	switch dbConfig.Driver {
	case "mysql":
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			dbConfig.Username, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Database)
		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
			Logger: glogger.Default.LogMode(glogger.Silent),
		})
		if err != nil {
			logger.Fatal("Database Connection Error: %s\n", err)
		}
		logger.Info("Connected to MySQL database.\n")
		if dbConfig.AutoMigrate {
			db.AutoMigrate(&models.CucmCdr{}, &models.CubeCDR{}, &models.CucmCmr{})
		}
		return &DataService{Session: db, Config: dbConfig}

	case "postgres":
		dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable TimeZone=UTC",
			dbConfig.Host, dbConfig.Username, dbConfig.Password, dbConfig.Database, dbConfig.Port)
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
			Logger: glogger.Default.LogMode(glogger.Silent),
		})
		if err != nil {
			logger.Fatal("Database Connection Error: %s\n", err)
		}
		logger.Info("Connected to PostgreSQL database.\n")
		if dbConfig.AutoMigrate {
			db.AutoMigrate(&models.CucmCdr{}, &models.CubeCDR{}, &models.CucmCmr{})
		}
		return &DataService{Session: db, Config: dbConfig}

	case "sqlserver":
		dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s",
			dbConfig.Username, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Database)
		db, err := gorm.Open(sqlserver.Open(dsn), &gorm.Config{
			Logger: glogger.Default.LogMode(glogger.Silent),
		})
		if err != nil {
			logger.Fatal("Database Connection Error: %s\n", err)
		}
		logger.Info("Connected to SQL Server database.\n")
		if dbConfig.AutoMigrate {
			db.AutoMigrate(&models.CucmCdr{}, &models.CubeCDR{}, &models.CucmCmr{})
		}
		return &DataService{Session: db, Config: dbConfig}

	case "clickhouse":
		var dsn string
		if dbConfig.SSL == "true" || dbConfig.SSL == "secure" {
			dsn = fmt.Sprintf("clickhouse://%s:%s@%s:%d/default?secure=true&dial_timeout=10s&read_timeout=30s&compress=1&max_execution_time=300",
				dbConfig.Username, dbConfig.Password, dbConfig.Host, dbConfig.Port)
		} else {
			dsn = fmt.Sprintf("clickhouse://%s:%s@%s:%d/default?dial_timeout=10s&read_timeout=30s&compress=1&max_execution_time=300",
				dbConfig.Username, dbConfig.Password, dbConfig.Host, dbConfig.Port)
		}

		db, err := gorm.Open(clickhouse.New(clickhouse.Config{
			DSN:                          dsn,
			DisableDatetimePrecision:     true,
			DontSupportRenameColumn:      true,
			DontSupportEmptyDefaultValue: true,
			SkipInitializeWithVersion:    true,
			DefaultTableEngineOpts:       "ENGINE=MergeTree() ORDER BY tuple()",
		}), &gorm.Config{
			Logger:                                   glogger.Default.LogMode(glogger.Silent),
			DisableForeignKeyConstraintWhenMigrating: true,
			CreateBatchSize:                          100,
		})
		if err != nil {
			logger.Fatal("Database Connection Error: %s\n", err)
		}

		sqlDB, err := db.DB()
		if err != nil {
			logger.Fatal("Failed to configure connection pool: %s\n", err)
		}

		sqlDB.SetMaxOpenConns(10)
		sqlDB.SetMaxIdleConns(5)
		sqlDB.SetConnMaxLifetime(30 * time.Minute)
		sqlDB.SetConnMaxIdleTime(5 * time.Minute)

		logger.Info("Connected to ClickHouse database.\n")
		if dbConfig.AutoMigrate {
			migrateClickHouse(db, dbConfig.Database)
		}

		return &DataService{Session: db, Config: dbConfig}

	case "sqlite":
		db, err := gorm.Open(sqlite.Open(dbConfig.Path), &gorm.Config{
			Logger: glogger.Default.LogMode(glogger.Silent),
		})
		if err != nil {
			logger.Fatal("Database Connection Error: %s\n", err)
		}
		logger.Info("Connected to SQLite database.\n")
		if dbConfig.AutoMigrate {
			db.AutoMigrate(&models.CucmCdr{}, &models.CubeCDR{}, &models.CucmCmr{})
		}
		return &DataService{Session: db, Config: dbConfig}

	default:
		logger.Fatal("Unsupported database driver: %s\n", dbConfig.Driver)
		return nil
	}
}

// migrateClickHouse handles ClickHouse-specific migration with error recovery
func migrateClickHouse(db *gorm.DB, databaseName string) {
	logger.Info("Starting ClickHouse migration...\n")

	logger.Info("Creating database %s...\n", databaseName)
	createDBQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", databaseName)
	if err := db.Exec(createDBQuery).Error; err != nil {
		logger.Error("Failed to create database: %s\n", err)
		return
	}
	logger.Info("Database %s created successfully\n", databaseName)

	logger.Info("Creating table cucm_cdrs...\n")
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.cucm_cdrs (
			id String,
			origin_pkid Nullable(String),
			file_cluster_id Nullable(String),
			file_node_id Nullable(String),
			file_date_time Nullable(Int64),
			file_sequence_number Nullable(Int64),
			cdrrecordtype Nullable(Int64),
			globalcallid_callmanagerid Nullable(Int64),
			globalcallid_callid Nullable(Int64),
			origlegcallidentifier Nullable(Int64),
			datetimeorigination Nullable(Int64),
			orignodeid Nullable(Int64),
			origspan Nullable(Int64),
			origipaddr Nullable(String),
			callingpartynumber Nullable(String),
			callingpartyunicodeloginuserid Nullable(String),
			origcause_location Nullable(Int64),
			origcause_value Nullable(Int64),
			origprecedencelevel Nullable(Int64),
			origmediatransportaddress_ip Nullable(String),
			origmediatransportaddress_port Nullable(Int64),
			origmediacap_payloadcapability Nullable(Int64),
			origmediacap_maxframesperpacket Nullable(Int64),
			origmediacap_g723bitrate Nullable(Int64),
			origvideocap_codec Nullable(Int64),
			origvideocap_bandwidth Nullable(Int64),
			origvideocap_resolution Nullable(Int64),
			origvideotransportaddress_ip Nullable(String),
			origvideotransportaddress_port Nullable(Int64),
			origrsvpaudiostat Nullable(Int64),
			origrsvpvideostat Nullable(Int64),
			destlegcallidentifier Nullable(Int64),
			destnodeid Nullable(Int64),
			destspan Nullable(Int64),
			destipaddr Nullable(String),
			originalcalledpartynumber Nullable(String),
			finalcalledpartynumber Nullable(String),
			finalcalledpartyunicodeloginuserid Nullable(String),
			destcause_location Nullable(Int64),
			destcause_value Nullable(Int64),
			destprecedencelevel Nullable(Int64),
			destmediatransportaddress_ip Nullable(String),
			destmediatransportaddress_port Nullable(Int64),
			destmediacap_payloadcapability Nullable(Int64),
			destmediacap_maxframesperpacket Nullable(Int64),
			destmediacap_g723bitrate Nullable(Int64),
			destvideocap_codec Nullable(Int64),
			destvideocap_bandwidth Nullable(Int64),
			destvideocap_resolution Nullable(Int64),
			destvideotransportaddress_ip Nullable(String),
			destvideotransportaddress_port Nullable(Int64),
			destrsvpaudiostat Nullable(Int64),
			destrsvpvideostat Nullable(Int64),
			datetimeconnect Nullable(Int64),
			datetimedisconnect Nullable(Int64),
			lastredirectdn Nullable(String),
			originalcalledpartynumberpartition Nullable(String),
			callingpartynumberpartition Nullable(String),
			finalcalledpartynumberpartition Nullable(String),
			lastredirectdnpartition Nullable(String),
			duration Nullable(Int64),
			origdevicename Nullable(String),
			destdevicename Nullable(String),
			origcallterminationonbehalfof Nullable(Int64),
			destcallterminationonbehalfof Nullable(Int64),
			origcalledpartyredirectonbehalfof Nullable(Int64),
			lastredirectredirectonbehalfof Nullable(Int64),
			origcalledpartyredirectreason Nullable(Int64),
			lastredirectredirectreason Nullable(Int64),
			destconversationid Nullable(Int64),
			globalcallid_clusterid Nullable(String),
			joinonbehalfof Nullable(Int64),
			comment Nullable(String),
			authcodedescription Nullable(String),
			authorizationlevel Nullable(Int64),
			clientmattercode Nullable(String),
			origdtmfmethod Nullable(Int64),
			destdtmfmethod Nullable(Int64),
			callsecuredstatus Nullable(Int64),
			origconversationid Nullable(Int64),
			origmediacap_bandwidth Nullable(Int64),
			destmediacap_bandwidth Nullable(Int64),
			authorizationcodevalue Nullable(String),
			outpulsedcallingpartynumber Nullable(String),
			outpulsedcalledpartynumber Nullable(String),
			origipv4v6addr Nullable(String),
			destipv4v6addr Nullable(String),
			origvideocap_codec_channel2 Nullable(Int64),
			origvideocap_bandwidth_channel2 Nullable(Int64),
			origvideocap_resolution_channel2 Nullable(Int64),
			origvideotransportaddress_ip_channel2 Nullable(String),
			origvideotransportaddress_port_channel2 Nullable(Int64),
			origvideochannel_role_channel2 Nullable(Int64),
			destvideocap_codec_channel2 Nullable(Int64),
			destvideocap_bandwidth_channel2 Nullable(Int64),
			destvideocap_resolution_channel2 Nullable(Int64),
			destvideotransportaddress_ip_channel2 Nullable(String),
			destvideotransportaddress_port_channel2 Nullable(Int64),
			destvideochannel_role_channel2 Nullable(Int64),
			incomingprotocolid Nullable(Int64),
			incomingprotocolcallref Nullable(String),
			outgoingprotocolid Nullable(Int64),
			outgoingprotocolcallref Nullable(String),
			currentroutingreason Nullable(Int64),
			origroutingreason Nullable(Int64),
			lastredirectingroutingreason Nullable(Int64),
			huntpilotpartition Nullable(String),
			huntpilotdn Nullable(String),
			calledpartypatternusage Nullable(Int64),
			incomingicid Nullable(String),
			incomingorigioi Nullable(String),
			incomingtermioi Nullable(String),
			outgoingicid Nullable(String),
			outgoingorigioi Nullable(String),
			outgoingtermioi Nullable(String),
			outpulsedoriginalcalledpartynumber Nullable(String),
			outpulsedlastredirectingnumber Nullable(String),
			wascallqueued Nullable(Int64),
			totalwaittimeinqueue Nullable(Int64),
			callingpartynumber_uri Nullable(String),
			originalcalledpartynumber_uri Nullable(String),
			finalcalledpartynumber_uri Nullable(String),
			lastredirectdn_uri Nullable(String),
			mobilecallingpartynumber Nullable(String),
			finalmobilecalledpartynumber Nullable(String),
			origmobiledevicename Nullable(String),
			destmobiledevicename Nullable(String),
			origmobilecallduration Nullable(Int64),
			destmobilecallduration Nullable(Int64),
			mobilecalltype Nullable(Int64),
			originalcalledpartypattern Nullable(String),
			finalcalledpartypattern Nullable(String),
			lastredirectingpartypattern Nullable(String),
			huntpilotpattern Nullable(String),
			origdevicetype Nullable(String),
			destdevicetype Nullable(String),
			origdevicesessionid Nullable(String),
			destdevicesessionid Nullable(String)
		) ENGINE = MergeTree()
		ORDER BY (id)
		PARTITION BY tuple()
		SETTINGS index_granularity = 8192
	`, databaseName)

	if err := db.Exec(createTableQuery).Error; err != nil {
		logger.Error("Failed to create cucm_cdrs table: %s\n", err)
		return
	}
	logger.Info("Table cucm_cdrs created successfully\n")

	logger.Info("Creating table cube_cdrs...\n")
	createCubeTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.cube_cdrs (
			id String,
			invalid_ntp_reference Bool,
			hostname Nullable(String),
			filename Nullable(String),
			file_timestamp Nullable(Int64),
			record_timestamp Nullable(Int64),
			call_id Nullable(Int64),
			cdr_type Nullable(Int64),
			account_code Nullable(String),
			acom_level Nullable(Int64),
			alert_time Nullable(Int64),
			backward_call_id Nullable(String),
			bytes_in Nullable(Int64),
			bytes_out Nullable(Int64),
			call_forward_count Nullable(String),
			call_forward_feature_correlation_id Nullable(String),
			call_forward_feature_id Nullable(String),
			call_forward_feature_status Nullable(String),
			call_forward_leg_id Nullable(Int64),
			call_forward_reason Nullable(String),
			call_forwarded_from_number Nullable(String),
			call_forwarded_number Nullable(String),
			call_forwarded_to_number Nullable(String),
			call_forwarding_from_number Nullable(String),
			calling_party_category Nullable(String),
			carrier_id Nullable(String),
			charge_number Nullable(String),
			charged_units Nullable(Int64),
			clid Nullable(String),
			codec_bytes Nullable(Int64),
			codec_type_rate Nullable(String),
			cust_biz_grp_id Nullable(String),
			disconnect_text Nullable(String),
			dnis Nullable(String),
			dsp_id Nullable(String),
			early_packets Nullable(Int64),
			fac_digit Nullable(String),
			fac_status Nullable(String),
			faxrelay_direction Nullable(String),
			faxrelay_ecm_status Nullable(String),
			faxrelay_encap_protocol Nullable(String),
			faxrelay_fax_success Nullable(String),
			faxrelay_init_hs_mod Nullable(Int64),
			faxrelay_jit_buf_ovflow Nullable(Int64),
			faxrelay_max_jit_buf_depth Nullable(Int64),
			faxrelay_mr_hs_mod Nullable(Int64),
			faxrelay_nsf_country_code Nullable(String),
			faxrelay_nsf_manuf_code Nullable(String),
			faxrelay_num_pages Nullable(Int64),
			faxrelay_pkt_conceal Nullable(Int64),
			faxrelay_rx_packets Nullable(Int64),
			faxrelay_start_time Nullable(String),
			faxrelay_stop_time Nullable(String),
			faxrelay_tx_packets Nullable(Int64),
			feature_id Nullable(String),
			feature_id_field1 Nullable(String),
			feature_id_field2 Nullable(Int64),
			feature_op_status Nullable(String),
			feature_op_time Nullable(String),
			feature_operation Nullable(String),
			gapfill_with_interpolation Nullable(Int64),
			gapfill_with_prediction Nullable(Int64),
			gapfill_with_redundancy Nullable(Int64),
			gapfill_with_silence Nullable(Int64),
			gk_xlated_cdn Nullable(String),
			gk_xlated_cgn Nullable(String),
			gtd_gw_rxd_cnn Nullable(String),
			gtd_gw_rxd_ocn Nullable(String),
			gtd_orig_cic Nullable(String),
			gtd_term_cic Nullable(String),
			gw_collected_cdn Nullable(String),
			gw_final_xlated_cdn Nullable(String),
			gw_final_xlated_cgn Nullable(String),
			gw_final_xlated_rdn Nullable(String),
			gw_rxd_cdn Nullable(String),
			gw_rxd_cgn Nullable(String),
			gw_rxd_rdn Nullable(String),
			h323_call_origin Nullable(String),
			h323_conf_id Nullable(String),
			h323_connect_time Nullable(Int64),
			h323_disconnect_cause Nullable(String),
			h323_disconnect_time Nullable(Int64),
			h323_ivr_out Nullable(String),
			h323_setup_time Nullable(Int64),
			h323_voice_quality Nullable(Int64),
			held_dn Nullable(Int64),
			hiwater_playout_delay Nullable(Int64),
			hold_feature_correlation_id Nullable(String),
			hold_feature_id Nullable(String),
			hold_leg_id Nullable(String),
			hold_phone_tag Nullable(String),
			hold_reason Nullable(String),
			hold_shared_line Nullable(Int64),
			hold_status Nullable(String),
			hold_username Nullable(String),
			holding_dn Nullable(Int64),
			in_carrier_id Nullable(String),
			in_intrfc_desc Nullable(String),
			in_lpcor_group Nullable(String),
			in_trunkgroup_label Nullable(String),
			incoming_area Nullable(String),
			info_type Nullable(String),
			internal_error_code Nullable(String),
			ip_hop Nullable(Int64),
			ip_pbx_mode Nullable(String),
			ip_phone_info Nullable(String),
			late_packets Nullable(Int64),
			leg_type Nullable(Int64),
			local_hostname Nullable(String),
			logical_if_index Nullable(Int64),
			lost_packets Nullable(Int64),
			lowater_playout_delay Nullable(Int64),
			max_bitrate Nullable(String),
			noise_level Nullable(Int64),
			ontime_rv_playout Nullable(Int64),
			originating_line_info Nullable(String),
			out_carrier_id Nullable(String),
			out_intrfc_desc Nullable(String),
			out_lpcor_group Nullable(String),
			out_trunkgroup_label Nullable(String),
			outgoing_area Nullable(String),
			override_session_time Nullable(Int64),
			paks_in Nullable(Int64),
			paks_out Nullable(Int64),
			peer_address Nullable(String),
			peer_id Nullable(Int64),
			peer_if_index Nullable(Int64),
			peer_sub_address Nullable(String),
			receive_delay Nullable(Int64),
			redirected_station_address Nullable(String),
			redirected_station_noa Nullable(String),
			redirected_station_npi Nullable(String),
			redirected_station_pi Nullable(String),
			remote_media_address Nullable(String),
			remote_media_id Nullable(String),
			remote_media_udp_port Nullable(Int64),
			remote_udp_port Nullable(Int64),
			round_trip_delay Nullable(Int64),
			service_descriptor Nullable(String),
			session_protocol Nullable(String),
			subscriber Nullable(String),
			supp_svc_xfer_by Nullable(String),
			twc_called_number Nullable(String),
			twc_calling_number Nullable(String),
			twc_feature_correlation_id Nullable(String),
			twc_feature_id Nullable(String),
			twc_feature_status Nullable(Int64),
			twc_leg_id Nullable(Int64),
			transfer_consultation_id Nullable(Int64),
			transfer_feature_correlation_id Nullable(String),
			transfer_feature_id Nullable(String),
			transfer_feature_status Nullable(String),
			transfer_forwarding_reason Nullable(String),
			transfer_leg_id Nullable(String),
			transfer_status Nullable(Int64),
			transferred_from_part Nullable(String),
			transferred_number Nullable(String),
			transferred_to_party Nullable(String),
			transmission_medium_req Nullable(String),
			tx_duration Nullable(Int64),
			username Nullable(String),
			vad_enable Nullable(Bool),
			voice_feature Nullable(String),
			voice_tx_duration Nullable(Int64)
		) ENGINE = MergeTree()
		ORDER BY (id)
		PARTITION BY tuple()
		SETTINGS index_granularity = 8192
	`, databaseName)

	if err := db.Exec(createCubeTableQuery).Error; err != nil {
		logger.Error("Failed to create cube_cdrs table: %s\n", err)
		return
	}
	logger.Info("Table cube_cdrs created successfully\n")

	logger.Info("Creating table cucm_cmrs...\n")
	createCMRTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.cucm_cmrs (
			id String,
			originpkid Nullable(String),
			file_cluster_id Nullable(String),
			file_node_id Nullable(String),
			file_date_time Nullable(Int64),
			file_sequence_number Nullable(Int64),
			cdrrecordtype Nullable(Int64),
			globalcallid_callmanagerid Nullable(Int64),
			globalcallid_callid Nullable(Int64),
			nodeid Nullable(Int64),
			directorynum Nullable(String),
			callidentifier Nullable(Int64),
			datetimestamp Nullable(Int64),
			numberpacketssent Nullable(Int64),
			numberoctetssent Nullable(Int64),
			numberpacketsreceived Nullable(Int64),
			numberoctetsreceived Nullable(Int64),
			numberpacketslost Nullable(Int64),
			jitter Nullable(Int64),
			latency Nullable(Int64),
			directorynumpartition Nullable(String),
			globalcallid_clusterid Nullable(String),
			devicename Nullable(String),
			duration Nullable(Int64),
			videocontenttype Nullable(String),
			videoduration Nullable(Int64),
			numbervideopacketssent Nullable(Int64),
			numbervideooctetssent Nullable(Int64),
			numbervideopacketsreceived Nullable(Int64),
			numbervideooctetsreceived Nullable(Int64),
			numbervideopacketslost Nullable(Int64),
			videoaveragejitter Nullable(Int64),
			videoroundtriptime Nullable(Int64),
			videoonewaydelay Nullable(Int64),
			videoreceptionmetrics Nullable(String),
			videotransmissionmetrics Nullable(String),
			videocontenttype_channel2 Nullable(String),
			videoduration_channel2 Nullable(Int64),
			numbervideopacketssent_channel2 Nullable(Int64),
			numbervideooctetssent_channel2 Nullable(Int64),
			numbervideopacketsreceived_channel2 Nullable(Int64),
			numbervideooctetsreceived_channel2 Nullable(Int64),
			numbervideopacketslost_channel2 Nullable(Int64),
			videoaveragejitter_channel2 Nullable(Int64),
			videoroundtriptime_channel2 Nullable(Int64),
			videoonewaydelay_channel2 Nullable(Int64),
			videoreceptionmetrics_channel2 Nullable(String),
			videotransmissionmetrics_channel2 Nullable(String),
			localsessionid Nullable(String),
			remotesessionid Nullable(String),
			headsetsn Nullable(String),
			headsetmetrics Nullable(String),
			vqccr Nullable(Float64),
			vqicr Nullable(Float64),
			vqicrmx Nullable(Float64),
			vqcs Nullable(Int64),
			vqscs Nullable(Int64),
			vqver Nullable(Float64),
			vqvorxcodec Nullable(String),
			vqc_id Nullable(Int64),
			vqvopktsizems Nullable(Int64),
			vqvopktlost Nullable(Int64),
			vqvopktdis Nullable(Int64),
			vqvoonewaydelayms Nullable(Int64),
			vqmaxjitter Nullable(Int64),
			vqmlqk Nullable(Float64),
			vqmlqkav Nullable(Float64),
			vqmlqkmn Nullable(Float64),
			vqmlqkmx Nullable(Float64),
			vqmlqkvr Nullable(Float64)
		) ENGINE = MergeTree()
		ORDER BY (id)
		PARTITION BY tuple()
		SETTINGS index_granularity = 8192
	`, databaseName)

	if err := db.Exec(createCMRTableQuery).Error; err != nil {
		logger.Error("Failed to create cucm_cmrs table: %s\n", err)
		return
	}
	logger.Info("Table cucm_cmrs created successfully\n")

	logger.Info("ClickHouse migration completed successfully.\n")
}

func (ds *DataService) WriteCDRs(cdrs []models.CucmCdr) error {
	if len(cdrs) == 0 {
		return nil
	}

	if ds.Config.Driver == "clickhouse" {
		return ds.writeClickHouseCDRs(cdrs)
	}

	limit := int(ds.Config.Limit)
	if limit <= 0 {
		limit = 100
	}
	if err := ds.Session.CreateInBatches(cdrs, limit).Error; err != nil {
		return fmt.Errorf("failed to write CDRs: %w", err)
	}

	return nil
}

func (ds *DataService) writeClickHouseCDRs(cdrs []models.CucmCdr) error {
	batchSize := int(ds.Config.Limit)
	if batchSize <= 0 {
		batchSize = 5000
	}

	db := ds.Session
	tableName := fmt.Sprintf("%s.cucm_cdrs", ds.Config.Database)

	for i := 0; i < len(cdrs); i += batchSize {
		end := i + batchSize
		if end > len(cdrs) {
			end = len(cdrs)
		}

		batch := cdrs[i:end]

		if err := db.Table(tableName).CreateInBatches(batch, len(batch)).Error; err != nil {
			return fmt.Errorf("failed to write ClickHouse CDR batch: %w", err)
		}
	}

	return nil
}
