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
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/oracle/go-oracledb/v26/oracle"
	oracleconfig "github.com/oracle/go-oracledb/v26/oracle/config"

	"github.com/wso2/identity-customer-data-service/internal/system/config"
)

// defaultOraclePort
const defaultOraclePort = 1521

// oracleTLS maps datasource.sslmode to the connect protocol and to whether the
// server certificate's name must match the host. Over TCPS the driver always
// verifies the certificate chain against the system trust store, so require,
// which asks for encryption without verification, verifies the chain too.
func oracleTLS(sslMode string) (protocol string, matchServerName bool, err error) {
	switch strings.ToLower(strings.TrimSpace(sslMode)) {
	case "", sslModeDisable:
		return "tcp", false, nil
	case sslModeRequire, sslModeVerifyCA:
		return "tcps", false, nil
	case sslModeVerifyFull:
		return "tcps", true, nil
	default:
		return "", false, fmt.Errorf("datasource.sslmode %q is not supported for oracle: use disable, require, "+
			"verify-ca or verify-full", sslMode)
	}
}

// OracleDriverConfig builds the driver configuration of the Oracle datasource.
// The credentials go in the configuration rather than in a connection string,
// so a password may hold any character.
func OracleDriverConfig(ds config.DataSourceConfig, connectTimeout time.Duration) (
	*oracleconfig.OracleDriverConfig, error) {
	protocol, matchServerName, err := oracleTLS(ds.SSLMode)
	if err != nil {
		return nil, err
	}

	port := ds.Port
	if port == 0 {
		port = defaultOraclePort
	}

	driverConfig := oracle.NewOracleDriverConfig()
	driverConfig.ConnectDescriptor = fmt.Sprintf("%s://%s/%s", protocol,
		net.JoinHostPort(ds.Hostname, strconv.Itoa(port)), ds.Name)
	driverConfig.Credentials.User = ds.Username
	driverConfig.Credentials.Password = ds.Password
	driverConfig.ConnectionProperties.ConnectTimeout = int(connectTimeout.Seconds())
	driverConfig.ConnectionProperties.SSLServerDNMatch = matchServerName
	return driverConfig, nil
}

// OracleConnector builds the connector the Oracle pool opens connections with.
func OracleConnector(ds config.DataSourceConfig, connectTimeout time.Duration) (driver.Connector, error) {
	driverConfig, err := OracleDriverConfig(ds, connectTimeout)
	if err != nil {
		return nil, err
	}
	return oracle.NewOracleConnector(driverConfig)
}

// OpenOracle opens a pool on the Oracle datasource.
func OpenOracle(ds config.DataSourceConfig, connectTimeout time.Duration) (*sql.DB, error) {
	connector, err := OracleConnector(ds, connectTimeout)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(connector), nil
}

// getOracleDB opens the Oracle pool once and returns it on every later call.
func getOracleDB() (*sql.DB, error) {
	dbMu.Lock()
	defer dbMu.Unlock()

	if closed {
		return nil, ErrDatabaseClosed
	}
	if oracleHandle != nil {
		return oracleHandle, nil
	}

	runtimeConfig := config.GetCDSRuntime().Config

	dbConfig, err := getDBConfig(runtimeConfig)
	if err != nil {
		return nil, err
	}

	settings, err := resolveOraclePoolSettings(runtimeConfig.DataSource.Oracle)
	if err != nil {
		return nil, err
	}

	db, err := openServerPool(dbConfig, settings)
	if err != nil {
		return nil, err
	}

	oracleHandle = db
	return oracleHandle, nil
}
