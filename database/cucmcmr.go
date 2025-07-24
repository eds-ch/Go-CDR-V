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

	"github.com/eds-ch/Go-CDR-V/models"
)

func (ds DataService) CreateCucmCMRs(cdrs []*models.CucmCmr) error {
	cdrValues := make([]models.CucmCmr, len(cdrs))
	for i, cdr := range cdrs {
		if cdr != nil {
			cdrValues[i] = *cdr
		}
	}

	return ds.WriteCMRs(cdrValues)
}

func (ds *DataService) WriteCMRs(cdrs []models.CucmCmr) error {
	if len(cdrs) == 0 {
		return nil
	}

	if ds.Session.Dialector.Name() == "clickhouse" {
		return ds.writeClickHouseCMRs(cdrs)
	}

	limit := int(ds.Config.Limit)
	if limit <= 0 {
		limit = 100
	}
	if err := ds.Session.CreateInBatches(cdrs, limit).Error; err != nil {
		return fmt.Errorf("failed to write CMRs: %w", err)
	}

	return nil
}

func (ds *DataService) writeClickHouseCMRs(cdrs []models.CucmCmr) error {
	batchSize := int(ds.Config.Limit)
	if batchSize <= 0 {
		batchSize = 5000
	}

	db := ds.Session
	tableName := fmt.Sprintf("%s.cucm_cmrs", ds.Config.Database)

	for i := 0; i < len(cdrs); i += batchSize {
		end := i + batchSize
		if end > len(cdrs) {
			end = len(cdrs)
		}

		batch := cdrs[i:end]

		if err := db.Table(tableName).CreateInBatches(batch, len(batch)).Error; err != nil {
			return fmt.Errorf("failed to write ClickHouse CMR batch: %w", err)
		}
	}

	return nil
}
