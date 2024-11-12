/*
 * Warp (C) 2019-2020 MinIO, Inc.
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
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
	"unsafe"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/v2/console"
	"github.com/minio/warp/pkg/generator"
)

// GetRDMA benchmarks download speed.
type GetRDMA struct {
	Common

	// Default Get options.
	GetOpts    minio.GetObjectOptions
	ListPrefix string

	objects       generator.Objects
	CreateObjects int
	Versions      int
	RandomRanges  bool
	RangeSize     int64
	ListExisting  bool
	ListFlat      bool
}

// Prepare will create an empty bucket or delete any content already there
// and upload a number of objects.
func (g *GetRDMA) Prepare(ctx context.Context) error {
	// prepare the bench by listing object from the bucket
	g.addCollector()
	if g.ListExisting {
		cl, done := g.Client()

		// ensure the bucket exist
		found, err := cl.GoClient.BucketExists(ctx, g.Bucket)
		if err != nil {
			return err
		}
		if !found {
			return (fmt.Errorf("bucket %s does not exist and --list-existing has been set", g.Bucket))
		}

		// list all objects
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		objectCh := cl.GoClient.ListObjects(ctx, g.Bucket, minio.ListObjectsOptions{
			WithVersions: g.Versions > 1,
			Prefix:       g.ListPrefix,
			Recursive:    !g.ListFlat,
		})

		versions := map[string]int{}

		for object := range objectCh {
			if object.Err != nil {
				return object.Err
			}
			if object.Size == 0 {
				continue
			}
			obj := generator.Object{
				Name: object.Key,
				Size: object.Size,
			}

			if g.Versions > 1 {
				if object.VersionID == "" {
					continue
				}

				if version, found := versions[object.Key]; found {
					if version >= g.Versions {
						continue
					}
				}
				versions[object.Key]++
				obj.VersionID = object.VersionID
			}

			g.objects = append(g.objects, obj)

			// limit to ListingMaxObjects
			if g.CreateObjects > 0 && len(g.objects) >= g.CreateObjects {
				break
			}
		}
		if len(g.objects) == 0 {
			return (fmt.Errorf("no objects found for bucket %s", g.Bucket))
		}
		done()
		return nil
	}

	// prepare the bench by creating the bucket and pushing some objects
	if err := g.createEmptyBucket(ctx); err != nil {
		return err
	}
	if g.Versions > 1 {
		cl, done := g.Client()
		if !g.Versioned {
			err := cl.GoClient.EnableVersioning(ctx, g.Bucket)
			if err != nil {
				return err
			}
			g.Versioned = true
		}
		done()
	}
	console.Eraseline()
	x := ""
	if g.Versions > 1 {
		x = fmt.Sprintf(" with %d versions each", g.Versions)
	}
	console.Info("\rUploading ", g.CreateObjects, " objects", x)

	var wg sync.WaitGroup
	wg.Add(g.Concurrency)

	objs := splitObjs(g.CreateObjects, g.Concurrency)
	rcv := g.Collector.rcv
	var groupErr error
	var mu sync.Mutex

	size, err := humanize.ParseBytes(g.CliCtx.String("obj.size"))
	if err != nil {
		return err
	}

	for i, obj := range objs {
		go func(i int, obj []struct{}) {
			defer wg.Done()
			src := g.Source()
			opts := g.PutOpts

			rbuf := make([]byte, 1)
			rand.Read(rbuf)

			var buf unsafe.Pointer
			if g.GPU {
				buf = minio.AlignedGPU(int(size), rbuf[0])
			} else {
				buf = minio.Aligned(int(size), rbuf[0])
			}

			client, cldone := g.Client()
			defer cldone()

			free := func(buf unsafe.Pointer) {
				if g.GPU {
					minio.FreeGPU(buf)
				} else {
					minio.Free(buf)
				}
			}

			defer free(buf)

			for range obj {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if g.rpsLimit(ctx) != nil {
					return
				}

				obj := src.Object()
				name := obj.Name
				for ver := 0; ver < g.Versions; ver++ {
					// New input for each version
					obj := src.Object()
					obj.Name = name
					op := Operation{
						OpType:   http.MethodPut,
						Thread:   uint16(i),
						Size:     obj.Size,
						File:     obj.Name,
						ObjPerOp: 1,
						Endpoint: client.GoClient.EndpointURL().String(),
					}

					opts.ContentType = obj.ContentType
					op.Start = time.Now()
					res, err := client.PutObjectRDMA(ctx, g.Bucket, obj.Name, buf, int(size), opts)
					op.End = time.Now()
					if err != nil {
						err := fmt.Errorf("upload error: %w", err)
						g.Error(err)
						mu.Lock()
						if groupErr == nil {
							groupErr = err
						}
						mu.Unlock()
						return
					}
					obj.VersionID = res.VersionID
					if res.Size != int64(size) {
						err := fmt.Errorf("short upload. want: %d, got %d", obj.Size, res.Size)
						g.Error(err)
						mu.Lock()
						if groupErr == nil {
							groupErr = err
						}
						mu.Unlock()
						return
					}
					mu.Lock()
					obj.Reader = nil
					g.objects = append(g.objects, *obj)
					g.prepareProgress(float64(len(g.objects)) / float64(g.CreateObjects*g.Versions))
					mu.Unlock()
					rcv <- op
				}
			}
		}(i, obj)
	}
	wg.Wait()
	return groupErr
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (g *GetRDMA) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
	var wg sync.WaitGroup
	wg.Add(g.Concurrency)
	c := g.Collector
	if g.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, http.MethodGet, g.AutoTermScale, autoTermCheck, autoTermSamples, g.AutoTermDur)
	}

	size, err := humanize.ParseBytes(g.CliCtx.String("obj.size"))
	if err != nil {
		return Operations{}, err
	}

	// Non-terminating context.
	nonTerm := context.Background()
	var groupErr error

	for i := 0; i < g.Concurrency; i++ {
		go func(i int) {
			rng := rand.New(rand.NewSource(int64(i)))
			rcv := c.Receiver()
			defer wg.Done()
			opts := g.GetOpts
			done := ctx.Done()

			<-wait

			var buf unsafe.Pointer
			if g.GPU {
				buf = minio.AlignedGPU(int(size), ' ')
			} else {
				buf = minio.Aligned(int(size), ' ')
			}

			client, cldone := g.Client()
			defer cldone()

			free := func(buf unsafe.Pointer) {
				if g.GPU {
					minio.FreeGPU(buf)
				} else {
					minio.Free(buf)
				}
			}

			defer free(buf)

			for {
				select {
				case <-done:
					return
				default:
				}

				if g.rpsLimit(ctx) != nil {
					return
				}

				obj := g.objects[rng.Intn(len(g.objects))]
				op := Operation{
					OpType:   http.MethodGet,
					Thread:   uint16(i),
					Size:     obj.Size,
					File:     obj.Name,
					ObjPerOp: 1,
					Endpoint: client.GoClient.EndpointURL().String(),
				}
				if g.DiscardOutput {
					op.File = ""
				}

				if g.RandomRanges && op.Size > 2 {
					var start, end int64
					if g.RangeSize <= 0 {
						// Randomize length similar to --obj.randsize
						size := generator.GetExpRandSize(rng, 0, op.Size-2)
						start = rng.Int63n(op.Size - size)
						end = start + size
					} else {
						start = rng.Int63n(op.Size - g.RangeSize)
						end = start + g.RangeSize - 1
					}
					op.Size = end - start + 1
					opts.SetRange(start, end)
				}
				op.Start = time.Now()
				var err error
				if g.Versions > 1 {
					opts.VersionID = obj.VersionID
				}

				err = client.GetObjectRDMA(nonTerm, g.Bucket, obj.Name, buf, int(size), opts)
				if err != nil {
					g.Error("download error:", err)
					op.Err = err.Error()
					op.End = time.Now()
					rcv <- op
					continue
				}
				op.FirstByte = &op.Start
				op.End = time.Now()
				if int64(size) != op.Size && op.Err == "" {
					op.Err = fmt.Sprint("unexpected download size. want:", op.Size, ", got:", size)
					g.Error(op.Err)
				}
				rcv <- op
			}
		}(i)
	}
	wg.Wait()
	return c.Close(), groupErr
}

// Cleanup deletes everything uploaded to the bucket.
func (g *GetRDMA) Cleanup(ctx context.Context) {
	if !g.ListExisting {
		g.deleteAllInBucket(ctx, g.objects.Prefixes()...)
	}
}
