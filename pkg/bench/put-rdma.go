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

package bench

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/warp/pkg"
)

// PutRDMA benchmarks upload speed.
type PutRDMA struct {
	Common
	prefixes map[string]struct{}
	cl       *http.Client
}

// Prepare will create an empty bucket ot delete any content already there.
func (u *PutRDMA) Prepare(ctx context.Context) error {
	return u.createEmptyBucket(ctx)
}

func getClient(ctx *cli.Context, host string) (*minio.MetaClient, error) {
	var creds *credentials.Credentials
	switch strings.ToUpper(ctx.String("signature")) {
	case "S3V4":
		// if Signature version '4' use NewV4 directly.
		creds = credentials.NewStaticV4(ctx.String("access-key"), ctx.String("secret-key"), "")
	case "S3V2":
		// if Signature version '2' use NewV2 directly.
		creds = credentials.NewStaticV2(ctx.String("access-key"), ctx.String("secret-key"), "")
	default:
		log.Fatalln(probe.NewError(errors.New("unknown signature method. S3V2 and S3V4 is available")), strings.ToUpper(ctx.String("signature")))
	}

	cl, err := minio.NewC(host, &minio.Options{
		Creds:        creds,
		Secure:       ctx.Bool("tls"),
		Region:       ctx.String("region"),
		BucketLookup: minio.BucketLookupAuto,
		Transport:    clientTransport(ctx),
	})
	if err != nil {
		return nil, err
	}
	cl.GoClient.SetAppInfo("warp", pkg.Version)

	if ctx.Bool("debug") {
		cl.GoClient.TraceOn(os.Stderr)
	}

	return cl, nil
}

func clientTransport(ctx *cli.Context) http.RoundTripper {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 10 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   ctx.Int("concurrent"),
		WriteBufferSize:       ctx.Int("sndbuf"), // Configure beyond 4KiB default buffer size.
		ReadBufferSize:        ctx.Int("rcvbuf"), // Configure beyond 4KiB default buffer size.
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 2 * time.Minute,
		// Set this value so that the underlying transport round-tripper
		// doesn't try to auto decode the body of objects with
		// content-encoding set to `gzip`.
		//
		// Refer:
		//    https://golang.org/src/net/http/transport.go?h=roundTrip#L1843
		DisableCompression: true,
		DisableKeepAlives:  ctx.Bool("disable-http-keepalive"),
	}
	if ctx.Bool("tls") {
		// Keep TLS config.
		tr.TLSClientConfig = &tls.Config{
			// Can't use SSLv3 because of POODLE and BEAST
			// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
			// Can't use TLSv1.1 because of RC4 cipher usage
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: ctx.Bool("insecure"),
		}
	}
	return tr
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (u *PutRDMA) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
	var wg sync.WaitGroup
	wg.Add(u.Concurrency)
	u.addCollector()
	c := u.Collector
	if u.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, http.MethodPut, u.AutoTermScale, autoTermCheck, autoTermSamples, u.AutoTermDur)
	}

	size, err := humanize.ParseBytes(u.CliCtx.String("obj.size"))
	if err != nil {
		return Operations{}, err
	}

	// Non-terminating context.
	nonTerm := context.Background()
	var groupErr error

	for i := 0; i < u.Concurrency; i++ {
		go func(i int) {
			rcv := c.Receiver()
			defer wg.Done()

			opts := u.PutOpts
			done := ctx.Done()

			<-wait

			rbuf := make([]byte, 1)
			rand.Read(rbuf)

			var buf unsafe.Pointer
			if u.GPU {
				buf = minio.AlignedGPU(int(size), rbuf[0])
			} else {
				buf = minio.Aligned(int(size), rbuf[0])
			}

			client, cldone := u.Client()
			defer cldone()

			free := func(buf unsafe.Pointer) {
				if u.GPU {
					minio.FreeGPU(buf)
				} else {
					minio.Free(buf)
				}
			}

			defer free(buf)

			j := 1
			for {
				select {
				case <-done:
					return
				default:
				}

				if u.rpsLimit(ctx) != nil {
					return
				}

				objName := fmt.Sprintf("%d-xx-%d/testobject-obj%d-worker%d.txt", j, i, j, i)
				op := Operation{
					OpType:   http.MethodPut,
					Thread:   uint16(i),
					Size:     int64(size),
					ObjPerOp: 1,
					File:     objName,
					Endpoint: client.GoClient.EndpointURL().String(),
				}

				op.Start = time.Now()
				res, err := client.PutObjectRDMA(nonTerm, u.Bucket, objName, buf, int(size), opts)
				op.End = time.Now()
				if err != nil {
					u.Error("upload error: ", err)
					op.Err = err.Error()
				}

				if res.Size != int64(size) && op.Err == "" {
					err := fmt.Sprint("short upload. want:", size, ", got:", res.Size)
					if op.Err == "" {
						op.Err = err
					}
					u.Error(err)
				}

				op.Size = res.Size
				rcv <- op
				j++
			}
		}(i)
	}
	wg.Wait()
	return c.Close(), groupErr
}

// Cleanup deletes everything uploaded to the bucket.
func (u *PutRDMA) Cleanup(ctx context.Context) {
	pf := make([]string, 0, len(u.prefixes))
	for p := range u.prefixes {
		pf = append(pf, p)
	}
	u.deleteAllInBucket(ctx, pf...)
}
