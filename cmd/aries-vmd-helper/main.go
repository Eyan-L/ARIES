// Command aries-vmd-helper runs inside each aries-vmd sandbox as its init,
// exec wrapper, and archive tool. Build it with CGO_ENABLED=0.
package main

import (
	"os"

	"github.com/hyscale-lab/aries/pkg/vmd/helper"
)

func main() {
	os.Exit(helper.Default().Main(os.Args[1:]))
}
