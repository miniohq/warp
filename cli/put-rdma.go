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

var putRDMAFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "obj.size",
		Value: "10MiB",
		Usage: "Size of each generated object. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
	},
	cli.BoolFlag{
		Name:  "gpu",
		Usage: "allocate buffers on GPU memory for RDMA",
	},
}

// PutRDMA command.
var putRDMACmd = cli.Command{
	Name:   "putRDMA",
	Usage:  "benchmark put objects over RDMA",
	Action: mainPutRDMA,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, putRDMAFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#putRDMA

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainPutRDMA is the entry point for cp command.
func mainPutRDMA(ctx *cli.Context) error {
	checkPutRDMASyntax(ctx)
	b := bench.PutRDMA{
		Common: getCommon(ctx, newGenSource(ctx, "obj.size")),
	}
	b.Common.GPU = ctx.Bool("gpu")
	b.CliCtx = ctx
	return runBench(ctx, &b)
}

// putRDMAOpts retrieves putRDMA options from the context.
func putRDMAOpts(ctx *cli.Context) minio.PutObjectOptions {
	return minio.PutObjectOptions{}
}

func checkPutRDMASyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}

	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
