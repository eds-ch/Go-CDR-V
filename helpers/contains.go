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

package helpers

func ContainsString(slice *[]string, s *string) bool {
	for _, v := range *slice {
		if v == *s {
			return true
		}
	}
	return false
}

func ContainsInt(slice *[]int, i *int) bool {
	for _, v := range *slice {
		if &v == i {
			return true
		}
	}
	return false
}

func ContainsInt64(slice *[]int64, i *int64) bool {
	for _, v := range *slice {
		if &v == i {
			return true
		}
	}
	return false
}
