// Copyright (c) 2023 Zion Dials <me@ziondials.com>
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

package cron

import (
	"time"

	"github.com/eds-ch/Go-CDR-V/config"
	"github.com/eds-ch/Go-CDR-V/database"
	"github.com/eds-ch/Go-CDR-V/parser"
	"github.com/go-co-op/gocron"
)

func RunCronJobs() {

	dbConfig := config.GetDatabaseFromGlobalConfig()
	db := database.InitDB(*dbConfig)
	s := gocron.NewScheduler(time.UTC)

	parseInterval := config.GetParserFromGlobalConfig().ParseInterval

	parseDirectories := config.GetDirectoriesFromGlobalConfig()

	s.Every(parseInterval).Minutes().Do(func() {
		for _, directory := range parseDirectories {
			parser.ParseFiles(directory.Input, directory.Output, directory.Type, directory.DeleteOriginal, db)
		}
	})

	s.StartBlocking()
}
