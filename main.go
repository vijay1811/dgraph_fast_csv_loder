package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"github.com/dgraph-io/dgraph/xidmap"
	"google.golang.org/grpc"
)

// fast data loading

func main() {
	server, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	zero, err := grpc.Dial("localhost:5080", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	dir := "badger"
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	xidMap := xidmap.New(zero, db)
	client := dgo.NewDgraphClient(api.NewDgraphClient(server))
	//cleint.Alter(nil, nil)

	file, err := os.Open("1500000_sr.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := xidMap.Flush(); err != nil {
			log.Fatal(err)
		}
	}()
	r := csv.NewReader(file)
	coloumns, err := r.Read()
	if err == io.EOF {
		return
	}
	if err != nil {
		log.Fatal(err)
	}
	for i, col := range coloumns {
		coloumns[i] = strings.ReplaceAll(col, " ", "_")
	}

	//	muChan := make(chan api.Mutation)

	id := 0
	mu := &api.Mutation{
		CommitNow: true,
	}
	t := time.Now()
	for {
		id++
		row, err := r.Read()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatal(err)
		}
		nqs := NquadsForRow(id, coloumns, row)
		for _, nq := range nqs {
			nq.Subject = fmt.Sprintf("%#x", uint64(xidMap.AssignUid(nq.Subject)))
			if nq.ObjectId != "" {
				nq.ObjectId = fmt.Sprintf("%#x", uint64(xidMap.AssignUid(nq.ObjectId)))
			}
			mu.Set = append(mu.Set, nq)
		}
		if len(mu.Set) < 7500 {
			continue
		}

		_, err = client.NewTxn().Mutate(context.Background(), mu)
		if err != nil {
			log.Fatal(err)
		}
		mu = &api.Mutation{
			CommitNow: true,
		}
		fmt.Println(id)

	}
	fmt.Println(time.Now().Sub(t))
	// mu := &api.Mutation{
	// 	CommitNow: true,
	// }
	// for i := 1; i <= 1500000; i++ {

	// 	for _, nq := range Nquads(i) {
	// 		nq.Subject = fmt.Sprintf("%#x", uint64(xidMap.AssignUid(nq.Subject)))
	// 		if nq.ObjectId != "" {
	// 			nq.ObjectId = fmt.Sprintf("%#x", uint64(xidMap.AssignUid(nq.ObjectId)))
	// 		}
	// 		mu.Set = append(mu.Set, nq)
	// 	}
	// 	if len(mu.Set) < 7500 {
	// 		continue
	// 	}
	// 	_, err := client.NewTxn().Mutate(context.Background(), mu)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}

	// 	mu = &api.Mutation{
	// 		CommitNow: true,
	// 	}
	// 	fmt.Println()
	// 	fmt.Println(i)
	// }

}

func NquadsForRow(id int, coloumns, row []string) []*api.NQuad {
	nquads := make([]*api.NQuad, len(row)+1)
	sub := fmt.Sprintf("id_%v", id)
	for i := 0; i <= len(row); i++ {
		if i == len(row) {
			nquads[i] = &api.NQuad{
				Subject:   sub,
				Predicate: "xid",
				ObjectValue: &api.Value{
					Val: &api.Value_StrVal{
						StrVal: sub,
					},
				},
			}
			continue
		}
		nquads[i] = &api.NQuad{
			Subject:   sub,
			Predicate: coloumns[i],
			ObjectValue: &api.Value{
				Val: &api.Value_StrVal{
					StrVal: row[i],
				},
			},
		}
	}
	return nquads
}

func Nquads(i int) []*api.NQuad {
	return []*api.NQuad{
		&api.NQuad{
			Subject:   fmt.Sprintf("id_%v", i),
			Predicate: "xid",
			ObjectValue: &api.Value{
				Val: &api.Value_StrVal{
					StrVal: fmt.Sprintf("id_%v", i),
				},
			},
		},
		&api.NQuad{
			Subject:   fmt.Sprintf("id_%v", i),
			Predicate: "name",
			ObjectValue: &api.Value{
				Val: &api.Value_StrVal{
					StrVal: fmt.Sprintf("person_%v", i),
				},
			},
		},
		&api.NQuad{
			Subject:   fmt.Sprintf("id_%v", i),
			Predicate: "age",
			ObjectValue: &api.Value{
				Val: &api.Value_IntVal{
					IntVal: int64(i % 30),
				},
			},
		},
		&api.NQuad{
			Subject:   fmt.Sprintf("id_%v", i),
			Predicate: "friend",
			ObjectId:  fmt.Sprintf("id_%v", i+1),
		},
	}
}
