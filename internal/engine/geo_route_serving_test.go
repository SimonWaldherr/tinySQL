package engine

import (
	"context"
	"fmt"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
	"sync"
	"testing"
)

func servingRouteGrid(tb testing.TB, side int) (*routeGraph, *routeGraphCoordinates) {
	tb.Helper()
	table := storage.NewTable("route_serving", []storage.Column{{Name: "source", Type: storage.IntType}, {Name: "target", Type: storage.IntType}, {Name: "cost", Type: storage.FloatType}}, false)
	for y := 0; y < side; y++ {
		for x := 0; x < side; x++ {
			from := y*side + x
			for _, delta := range [][2]int{{1, 0}, {-1, 0}, {0, 1}, {0, -1}} {
				nx, ny := x+delta[0], y+delta[1]
				if nx >= 0 && nx < side && ny >= 0 && ny < side {
					table.Rows = append(table.Rows, []any{from, ny*side + nx, float64(100)})
				}
			}
		}
	}
	graph, err := buildRouteGraph(context.Background(), table, "source", "target", "cost", "directed")
	if err != nil {
		tb.Fatal(err)
	}
	coords := &routeGraphCoordinates{lat: make([]float64, len(graph.nodeValues)), lon: make([]float64, len(graph.nodeValues)), complete: true}
	for i, value := range graph.nodeValues {
		id := value.(int)
		coords.lat[i] = 48 + float64(id/side)*0.0005
		coords.lon[i] = 11 + float64(id%side)*0.0005
	}
	return graph, coords
}

func BenchmarkRouteServingAStar(b *testing.B) {
	graph, coords := servingRouteGrid(b, 100)
	for _, path := range []bool{false, true} {
		b.Run(fmt.Sprintf("path=%t", path), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, distance, found := aStarSearch(graph, coords, 0, len(graph.nodeValues)-1, 1, path)
				if !found || distance <= 0 {
					b.Fatal("route missing")
				}
			}
		})
	}
}

func TestRouteServingAStarScratchReuse(t *testing.T) {
	for _, side := range []int{12, 5, 20, 8} {
		graph, coords := servingRouteGrid(t, side)
		for _, pair := range [][2]int{{0, len(graph.nodeValues) - 1}, {len(graph.nodeValues) - 1, 0}, {3, side + 1}, {1, 1}} {
			_, want, wantFound := dijkstraSearch(graph, pair[0], pair[1], false)
			for _, scale := range []float64{1, 0, 0.5} {
				path, got, found := aStarSearch(graph, coords, pair[0], pair[1], scale, true)
				if found != wantFound || got != want {
					t.Fatalf("side=%d pair=%v scale=%v got=(%v,%t) want=(%v,%t)", side, pair, scale, got, found, want, wantFound)
				}
				if len(path) == 0 || path[0].node != pair[0] || path[len(path)-1].node != pair[1] || path[len(path)-1].cumulative != got {
					t.Fatalf("invalid path: %v", path)
				}
			}
		}
	}
}

func TestRouteServingAStarConcurrent(t *testing.T) {
	graph, coords := servingRouteGrid(t, 20)
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				start, end := (worker*17+i)%len(graph.nodeValues), (worker*31+i*7)%len(graph.nodeValues)
				a, b := graph.nodeValues[start].(int), graph.nodeValues[end].(int)
				dx, dy := a%20-b%20, a/20-b/20
				if dx < 0 {
					dx = -dx
				}
				if dy < 0 {
					dy = -dy
				}
				_, got, found := aStarSearch(graph, coords, start, end, float64(i%2), true)
				if !found || got != float64((dx+dy)*100) {
					t.Errorf("pair=%d/%d got=(%v,%t)", start, end, got, found)
				}
			}
		}(worker)
	}
	wg.Wait()
}
