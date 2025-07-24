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
	"gorm.io/gorm"
)

func (ds DataService) CreateCubeCDRs(cdrs []*models.CubeCDR) error {
	cdrValues := make([]models.CubeCDR, len(cdrs))
	for i, cdr := range cdrs {
		if cdr != nil {
			cdrValues[i] = *cdr
		}
	}

	return ds.WriteCubeCDRs(cdrValues)
}

func (ds *DataService) WriteCubeCDRs(cdrs []models.CubeCDR) error {
	if len(cdrs) == 0 {
		return nil
	}

	if ds.Session.Dialector.Name() == "clickhouse" {
		return SaveCubeCDRsToClickHouse(cdrs, ds.Session, ds.Config.Database)
	}

	limit := int(ds.Config.Limit)
	if limit <= 0 {
		limit = 100
	}
	if err := ds.Session.CreateInBatches(cdrs, limit).Error; err != nil {
		return fmt.Errorf("failed to write CUBE CDRs: %w", err)
	}

	return nil
}

func SaveCubeCDRsToClickHouse(cdrs []models.CubeCDR, db *gorm.DB, databaseName string) error {
	if len(cdrs) == 0 {
		return nil
	}

	tableName := fmt.Sprintf("%s.cube_cdrs", databaseName)

	result := db.Table(tableName).CreateInBatches(cdrs, 1000)
	if result.Error != nil {
		return fmt.Errorf("failed to save CubeCDRs to ClickHouse: %w", result.Error)
	}

	return nil
}
