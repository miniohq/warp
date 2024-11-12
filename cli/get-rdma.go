/*
 * Warp (C) 2024 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cli

import (
	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/v2/console"
	"github.com/minio/warp/pkg/bench"
)

var getRDMAFlags = []cli.Flag{
	cli.IntFlag{
		Name:  "objects",
		Value: 2500,
		Usage: "Number of objects to upload.",
	},
	cli.StringFlag{
		Name:  "obj.size",
		Value: "10MiB",
		Usage: "Size of each generated object. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
	},
	cli.BoolFlag{
		Name:  "list-existing",
		Usage: "Instead of preparing the bench by PUTing some objects, only use objects already in the bucket",
	},
	cli.BoolFlag{
		Name:  "list-flat",
		Usage: "When using --list-existing, do not use recursive listing",
	},
	cli.IntFlag{
		Name:  "versions",
		Value: 1,
		Usage: "Number of versions to upload. If more than 1, versioned listing will be benchmarked",
	},
	cli.BoolFlag{
		Name:  "gpu",
		Usage: "allocate buffers on GPU memory for RDMA",
	},
}

// GetRDMA command.
var getRDMACmd = cli.Command{
	Name:   "getRDMA",
	Usage:  "benchmark get objects over RDMA",
	Action: mainGetRDMA,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, getRDMAFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#getRDMA

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainGetRDMA is the entry point for cp command.
func mainGetRDMA(ctx *cli.Context) error {
	checkGetRDMASyntax(ctx)
	b := bench.GetRDMA{
		Common:        getCommon(ctx, newGenSource(ctx, "obj.size")),
		Versions:      ctx.Int("versions"),
		ListExisting:  ctx.Bool("list-existing"),
		ListFlat:      ctx.Bool("list-flat"),
		ListPrefix:    ctx.String("prefix"),
		CreateObjects: ctx.Int("objects"),
	}
	b.Common.GPU = ctx.Bool("gpu")
	b.CliCtx = ctx
	return runBench(ctx, &b)
}

// getRDMAOpts retrieves getRDMA options from the context.
func getRDMAOpts(ctx *cli.Context) minio.GetObjectOptions {
	return minio.GetObjectOptions{}
}

func checkGetRDMASyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}

	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
