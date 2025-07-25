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

import (
	"encoding/binary"
	"fmt"
	"strings"
)

func ConvertStringToIPCisco(ip *string) (*string, error) {
	if ip == nil {
		return nil, nil
	}
	stringInt, err := ConvertStringToInt(ip)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(*stringInt))
	arrayIP := fmt.Sprintf("%d", b)
	newIPStringArray := strings.ReplaceAll(strings.ReplaceAll(arrayIP, "[", ""), "]", "")
	splitIPStringArray := strings.Split(newIPStringArray, " ")
	newIPString := fmt.Sprintf("%s.%s.%s.%s", splitIPStringArray[0], splitIPStringArray[1], splitIPStringArray[2], splitIPStringArray[3])
	if newIPString == "0.0.0.0" {
		return nil, nil
	}
	return &newIPString, nil
}
