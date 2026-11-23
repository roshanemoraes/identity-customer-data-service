/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package provider

import (
	"strings"
	"testing"
	"time"

	"github.com/wso2/identity-customer-data-service/internal/system/config"
	"github.com/wso2/identity-customer-data-service/internal/system/database"
)

func oracleDataSource() config.DataSourceConfig {
	return config.DataSourceConfig{
		Type:     "oracle",
		Hostname: "db.example.com",
		Port:     1522,
		Name:     "FREEPDB1",
		Username: "cdsuser",
		Password: "p@ss/word",
		SSLMode:  "require",
	}
}

func Test_getDBConfig_oracle(t *testing.T) {
	dbConfig, err := getDBConfig(config.Config{DataSource: oracleDataSource()})
	if err != nil {
		t.Fatal(err)
	}
	if dbConfig.driverName != database.DriverOracle || dbConfig.connector == nil {
		t.Errorf("expected an oracle connector, got %+v", dbConfig)
	}
}

func Test_OracleDriverConfig(t *testing.T) {
	driverConfig, err := OracleDriverConfig(oracleDataSource(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if driverConfig.ConnectDescriptor != "tcps://db.example.com:1522/FREEPDB1" {
		t.Errorf("unexpected connect descriptor %q", driverConfig.ConnectDescriptor)
	}
	// The password is never parsed out of a string, so it keeps the @ and the /.
	if driverConfig.Credentials.User != "cdsuser" || driverConfig.Credentials.Password != "p@ss/word" {
		t.Errorf("unexpected credentials %q / %q", driverConfig.Credentials.User, driverConfig.Credentials.Password)
	}
	if driverConfig.ConnectionProperties.ConnectTimeout != 10 {
		t.Errorf("expected a 10 second connect timeout, got %d", driverConfig.ConnectionProperties.ConnectTimeout)
	}
}

func Test_OracleDriverConfig_defaults(t *testing.T) {
	ds := oracleDataSource()
	ds.Port = 0
	ds.SSLMode = ""

	driverConfig, err := OracleDriverConfig(ds, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if driverConfig.ConnectDescriptor != "tcp://db.example.com:1521/FREEPDB1" {
		t.Errorf("unexpected connect descriptor %q", driverConfig.ConnectDescriptor)
	}
}

func Test_oracleTLS(t *testing.T) {
	type tls struct {
		protocol        string
		matchServerName bool
	}
	cases := map[string]tls{
		"": {"tcp", false}, "disable": {"tcp", false}, "require": {"tcps", false},
		"verify-ca": {"tcps", false}, "VERIFY-FULL": {"tcps", true},
	}
	for sslMode, want := range cases {
		protocol, matchServerName, err := oracleTLS(sslMode)
		if err != nil || protocol != want.protocol || matchServerName != want.matchServerName {
			t.Errorf("oracleTLS(%q) = %q, %v, %v; expected %+v", sslMode, protocol, matchServerName, err, want)
		}
	}
	if _, _, err := oracleTLS(unsupportedSSLMode); err == nil {
		t.Error("expected an error for an sslmode Oracle cannot honor")
	}
}

func Test_resolveOraclePoolSettings(t *testing.T) {
	settings, err := resolveOraclePoolSettings(config.OracleConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if settings.maxOpenConns != database.DefaultOracleMaxOpenConns {
		t.Errorf("the defaults were not applied: %+v", settings)
	}
	_, err = resolveOraclePoolSettings(config.OracleConfig{MaxOpenConns: 5, MaxIdleConns: 9})
	if err == nil || !strings.Contains(err.Error(), "datasource.oracle.max_idle_conns") {
		t.Errorf("expected an error naming the oracle setting, got %v", err)
	}
}

func Test_ValidateDataSource_oracle(t *testing.T) {
	if err := ValidateDataSource(oracleDataSource()); err != nil {
		t.Fatalf("a complete oracle configuration was refused: %v", err)
	}

	ds := oracleDataSource()
	ds.Name = ""
	if err := ValidateDataSource(ds); err == nil || !strings.Contains(err.Error(), "datasource.name") {
		t.Errorf("expected the missing name to be named, got %v", err)
	}

	ds = oracleDataSource()
	ds.SSLMode = unsupportedSSLMode
	if err := ValidateDataSource(ds); err == nil {
		t.Error("expected an unsupported sslmode to be refused")
	}

	ds = oracleDataSource()
	ds.Oracle.ConnectTimeoutSeconds = -1
	if err := ValidateDataSource(ds); err == nil {
		t.Error("expected a negative pool setting to be refused")
	}
}
