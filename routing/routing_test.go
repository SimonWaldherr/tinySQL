package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/importer"
)

func road(id int64, nodes ...int64) Way {
	return Way{ID: id, Nodes: nodes, Tags: map[string]string{"highway": "residential"}}
}
func fixture() Data {
	return Data{Nodes: []Node{{1, Point{-.001, 0}, nil}, {2, Point{0, 0}, nil}, {3, Point{.001, 0}, nil}, {4, Point{0, .001}, nil}, {5, Point{.001, .001}, nil}}, Ways: []Way{road(10, 1, 2), road(20, 2, 3), road(30, 2, 4), road(40, 4, 5, 3)}}
}
func build(t testing.TB, d Data, p Profile) *Router {
	t.Helper()
	r, err := Build(context.Background(), d, p, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func restriction(kind string, from, to int64, via ...Member) Relation {
	members := []Member{{"way", from, "from"}, {"way", to, "to"}}
	members = append(members, via...)
	return Relation{ID: 100, Members: members, Tags: map[string]string{"type": "restriction", "restriction": kind}}
}
func query(t *testing.T, r *Router, from, to Point) Result {
	t.Helper()
	res, err := r.Route(t.Context(), Request{From: from, To: to, MaxSnapMeters: 10})
	if err != nil {
		t.Fatal(err)
	}
	return res
}
func TestTurnRestrictionsAndPartialEdges(t *testing.T) {
	for _, kind := range []string{"no_straight_on", "only_right_turn"} {
		t.Run(kind, func(t *testing.T) {
			d := fixture()
			target := int64(20)
			if strings.HasPrefix(kind, "only_") {
				target = 30
			}
			d.Relations = []Relation{restriction(kind, 10, target, Member{"node", 2, "via"})}
			r := build(t, d, Car)
			full := query(t, r, Point{-.001, 0}, Point{.001, 0})
			if full.DistanceMeters < 400 {
				t.Fatalf("forbidden shortcut: %+v", full)
			}
			partial := query(t, r, Point{-.0005, 0}, Point{.0005, 0})
			if partial.DistanceMeters < 300 {
				t.Fatalf("partial goal bypassed restriction: %+v", partial)
			}
			reverse := query(t, r, Point{.001, 0}, Point{-.001, 0})
			if reverse.DistanceMeters > 230 {
				t.Fatal("restriction wrongly applied backwards")
			}
			atVia := query(t, r, Point{0, 0}, Point{.001, 0})
			if atVia.DistanceMeters > 120 {
				t.Fatal("start node inherited incoming turn history")
			}
		})
	}
}
func TestViaWayRestrictions(t *testing.T) {
	d := Data{Nodes: []Node{{1, Point{0, 0}, nil}, {2, Point{.001, 0}, nil}, {3, Point{.002, 0}, nil}, {4, Point{.003, 0}, nil}, {5, Point{.004, 0}, nil}, {6, Point{.002, .002}, nil}}, Ways: []Way{road(10, 1, 2), road(20, 2, 3, 4), road(30, 4, 5), road(40, 3, 6, 5)}}
	d.Relations = []Relation{restriction("no_straight_on", 10, 30, Member{"way", 20, "via"})}
	r := build(t, d, Car)
	res := query(t, r, Point{0, 0}, Point{.004, 0})
	if res.DistanceMeters < 600 {
		t.Fatal("via-way restriction ignored")
	}
	middle := query(t, r, Point{.002, 0}, Point{.004, 0})
	if middle.DistanceMeters > 230 {
		t.Fatal("via-way rule applied without its from-way history")
	}
	d.Relations[0].Tags["restriction"] = "only_straight_on"
	r = build(t, d, Car)
	res = query(t, r, Point{0, 0}, Point{.002, .002})
	if res.DistanceMeters < 700 {
		t.Fatal("only-via restriction allowed an early exit")
	}
	// A forbidden via way must not dissolve an only_* turn requirement.
	d.Ways = append(d.Ways, road(50, 2, 6))
	d.Ways[1].Tags["motorcar"] = "no"
	r = build(t, d, Car)
	if _, err := r.Route(t.Context(), Request{From: Point{0, 0}, To: Point{.002, .002}, MaxSnapMeters: 10}); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("inaccessible only-via: %v", err)
	}
}
func TestNoUTurnDoesNotBanStraightContinuation(t *testing.T) {
	d := fixture()
	d.Ways[0] = road(10, 1, 2, 3)
	d.Ways = d.Ways[:1]
	d.Relations = []Relation{restriction("no_u_turn", 10, 10, Member{"node", 2, "via"})}
	r := build(t, d, Car)
	res := query(t, r, Point{-.001, 0}, Point{.001, 0})
	if res.DistanceMeters > 230 {
		t.Fatal("straight continuation rejected")
	}
}
func TestProfilesAccessOneWaySpeedAndExceptions(t *testing.T) {
	d := Data{Nodes: []Node{{1, Point{0, 0}, nil}, {2, Point{.001, 0}, nil}}, Ways: []Way{road(10, 1, 2)}}
	d.Ways[0].Tags["oneway"] = "-1"
	d.Ways[0].Tags["oneway:bicycle"] = "no"
	d.Ways[0].Tags["maxspeed"] = "10 mph"
	car := build(t, d, Car)
	if _, err := car.Route(t.Context(), Request{From: Point{0, 0}, To: Point{.001, 0}}); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("oneway bypass: %v", err)
	}
	res := query(t, car, Point{.001, 0}, Point{0, 0})
	if math.Abs(res.DurationSeconds-res.DistanceMeters/(10*1.609344/3.6)) > 1e-8 {
		t.Fatal("mph speed ignored")
	}
	query(t, build(t, d, Bicycle), Point{0, 0}, Point{.001, 0})
	query(t, build(t, d, Foot), Point{0, 0}, Point{.001, 0})
	d.Ways[0].Tags["access"] = "private"
	d.Ways[0].Tags["bicycle"] = "yes"
	if build(t, d, Car).Stats().DirectedEdges != 0 {
		t.Fatal("private way allowed")
	}
	query(t, build(t, d, Bicycle), Point{0, 0}, Point{.001, 0})
	d = fixture()
	d.Relations = []Relation{restriction("no_straight_on", 10, 20, Member{"node", 2, "via"})}
	d.Relations[0].Tags["except"] = "bicycle"
	if query(t, build(t, d, Bicycle), Point{-.001, 0}, Point{.001, 0}).DistanceMeters > 230 {
		t.Fatal("bicycle exception ignored")
	}
	d.Nodes[1].Tags = map[string]string{"barrier": "bollard"}
	if build(t, d, Car).Stats().DirectedEdges >= 8 {
		t.Fatal("barrier ignored")
	}
}
func TestConditionalAndMalformedDataRejected(t *testing.T) {
	d := fixture()
	d.Relations = []Relation{restriction("no_straight_on", 10, 20, Member{"node", 2, "via"})}
	d.Relations[0].Tags["restriction:conditional"] = "no_straight_on @ (Mo-Fr)"
	if _, err := Build(t.Context(), d, Car, Options{}); err == nil {
		t.Fatal("conditional restriction discarded")
	}
	d = fixture()
	d.Ways[0].Nodes = append(d.Ways[0].Nodes, 999)
	if _, err := Build(t.Context(), d, Car, Options{}); err == nil {
		t.Fatal("missing node accepted")
	}
	d = fixture()
	d.Nodes = append(d.Nodes, d.Nodes[0])
	if _, err := Build(t.Context(), d, Car, Options{}); err == nil {
		t.Fatal("duplicate node accepted")
	}
}
func TestSphericalSnapMatchesBruteForce(t *testing.T) {
	d := fixture()
	d.Nodes = append(d.Nodes, Node{10, Point{179.99, 70}, nil}, Node{11, Point{-179.99, 70}, nil}, Node{12, Point{0, 89.99}, nil}, Node{13, Point{90, 89.99}, nil})
	d.Ways = append(d.Ways, road(90, 10, 11), road(91, 12, 13))
	r := build(t, d, Car)
	points := []Point{{180, 70}, {45, 89.991}, {.0004, .0003}, {-.0007, .0001}}
	for _, p := range points {
		got, err := r.Nearest(t.Context(), p, 5000)
		if err != nil {
			t.Fatal(err)
		}
		best := math.Inf(1)
		for _, s := range r.segments {
			_, _, distance := project(vector(p), r.nodes[s.a].xyz, r.nodes[s.b].xyz)
			best = math.Min(best, distance)
		}
		if math.Abs(best-got.DistanceMeters) > 1e-7 {
			t.Fatalf("%v got=%v want=%v", p, got.DistanceMeters, best)
		}
	}
}
func grid(side int) Data {
	d := Data{}
	for y := 0; y < side; y++ {
		for x := 0; x < side; x++ {
			id := int64(y*side + x + 1)
			d.Nodes = append(d.Nodes, Node{id, Point{11 + float64(x)*.001, 48 + float64(y)*.001}, nil})
			if x > 0 {
				d.Ways = append(d.Ways, road(int64(len(d.Ways)+1), id-1, id))
			}
			if y > 0 {
				d.Ways = append(d.Ways, road(int64(len(d.Ways)+1), id-int64(side), id))
			}
		}
	}
	return d
}
func TestAStarMatchesDijkstraWithoutAnswerCache(t *testing.T) {
	r := build(t, grid(12), Car)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 50; i++ {
		a, b := rng.Intn(len(r.nodes)), rng.Intn(len(r.nodes))
		req := Request{From: r.nodes[a].point, To: r.nodes[b].point}
		got, e1 := r.route(t.Context(), req, true)
		want, e2 := r.route(t.Context(), req, false)
		if e1 != nil || e2 != nil || math.Abs(got.DurationSeconds-want.DurationSeconds) > 1e-8 {
			t.Fatalf("query=%+v got=%+v/%v want=%+v/%v", req, got, e1, want, e2)
		}
	}
}
func TestRouteCancellationLimitsAndConcurrency(t *testing.T) {
	r := build(t, grid(15), Car)
	req := Request{From: r.nodes[0].point, To: r.nodes[len(r.nodes)-1].point}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := r.Route(ctx, req); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	small, err := Build(t.Context(), grid(15), Car, Options{MaxStates: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = small.Route(t.Context(), req); !errors.Is(err, ErrSearchLimit) {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := r.Route(t.Context(), req); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

const tinyOSM = `<osm version="0.6"><way id="10"><nd ref="1"/><nd ref="2"/><tag k="highway" v="residential"/></way><node id="1" lat="0" lon="0"/><node id="2" lat="0" lon="0.001"/></osm>`

func TestOSMXMLAndTinySQLTable(t *testing.T) {
	data, err := ReadOSM(t.Context(), strings.NewReader(tinyOSM))
	if err != nil {
		t.Fatal(err)
	}
	direct := query(t, build(t, data, Car), Point{0, 0}, Point{.001, 0})
	db := tinysql.NewDB()
	if _, err := importer.ImportOSM(t.Context(), db, "default", "osm", strings.NewReader(tinyOSM), &importer.ImportOptions{CreateTable: true}); err != nil {
		t.Fatal(err)
	}
	r, err := FromDB(t.Context(), db, "default", "osm", Car, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if got := query(t, r, Point{0, 0}, Point{.001, 0}); !reflect.DeepEqual(got, direct) {
		t.Fatalf("DB differs: %+v / %+v", got, direct)
	}
}
func TestHTTPAPI(t *testing.T) {
	h := Handler(map[Profile]*Router{Car: build(t, fixture(), Car)})
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{{"GET", "/route?from=-0.001,0&to=0.001,0", "", 200}, {"POST", "/route", `{"from":{"lon":-0.001,"lat":0},"to":{"lon":0.001,"lat":0},"profile":"car"}`, 200}, {"GET", "/route?from=NaN,0&to=0,0", "", 400}, {"GET", "/route?from=0,0&to=0.001,0&profile=boat", "", 400}, {"GET", "/nearest?point=0,0", "", 200}, {"GET", "/profiles", "", 200}, {"GET", "/healthz", "", 204}, {"POST", "/route", `{"unknown":1}`, 400}, {"POST", "/route", `{} {}`, 400}, {"POST", "/route", `{}`, 400}, {"POST", "/route", `{"from":null,"to":{"lon":0,"lat":0}}`, 400}} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if w.Code != tc.status {
			t.Fatalf("%s: status=%d body=%s", tc.path, w.Code, w.Body.String())
		}
		if strings.HasPrefix(tc.path, "/route") && w.Code == 200 {
			var res Result
			if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil || res.Geometry.Type != "LineString" {
				t.Fatal("invalid route JSON")
			}
		}
	}
}
func BenchmarkRouteNoResultCache(b *testing.B) {
	r := build(b, grid(60), Car)
	for _, target := range []struct {
		name  string
		index int
	}{{"diagonal", len(r.nodes) - 1}, {"axis", 59}} {
		req := Request{From: r.nodes[0].point, To: r.nodes[target.index].point}
		for _, astar := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/astar=%t", target.name, astar), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := r.route(context.Background(), req, astar); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkSnapNoResultCache(b *testing.B) {
	r := build(b, grid(100), Car)
	p := Point{11.05, 48.05}
	b.Run("full_scan", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			best := math.Inf(1)
			v := vector(p)
			for _, s := range r.segments {
				_, _, d := project(v, r.nodes[s.a].xyz, r.nodes[s.b].xyz)
				best = math.Min(best, d)
			}
			if best < 0 {
				b.Fatal(best)
			}
		}
	})
	b.Run("bvh", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := r.Nearest(context.Background(), p, 100); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestUnusedNodesDoNotInflateSearch(t *testing.T) {
	d := fixture()
	want := build(t, d, Car)
	for i := 0; i < 1000; i++ {
		d.Nodes = append(d.Nodes, Node{ID: int64(10000 + i), Point: Point{1, 1}})
	}
	got := build(t, d, Car)
	if len(got.nodes) != len(want.nodes) {
		t.Fatalf("unused nodes retained: %d", len(got.nodes))
	}
	a, b := Point{-.001, 0}, Point{.001, 0}
	if !reflect.DeepEqual(query(t, got, a, b), query(t, want, a, b)) {
		t.Fatal("compaction changed route")
	}
}

func TestXMLMissingCoordinates(t *testing.T) {
	if _, err := ReadOSM(t.Context(), strings.NewReader(`<osm><node id="1" lon="0"/></osm>`)); err == nil {
		t.Fatal("accepted missing latitude")
	}
}
func TestRestrictedAStarMatchesDijkstra(t *testing.T) {
	for _, value := range []string{"no_straight_on", "only_right_turn"} {
		d := fixture()
		d.Relations = []Relation{{ID: 99, Tags: map[string]string{"type": "restriction", "restriction": value}, Members: []Member{{"way", 10, "from"}, {"node", 2, "via"}, {"way", 20, "to"}}}}
		r := build(t, d, Car)
		for _, from := range r.nodes {
			for _, to := range r.nodes {
				req := Request{From: from.point, To: to.point}
				a, ae := r.route(t.Context(), req, true)
				b, be := r.route(t.Context(), req, false)
				if (ae == nil) != (be == nil) || ae == nil && math.Abs(a.DurationSeconds-b.DurationSeconds) > 1e-8 {
					t.Fatalf("A* differs: %+v %v / %+v %v", a, ae, b, be)
				}
			}
		}
	}
}

func TestRestrictionAutomatonOverlaps(t *testing.T) {
	a := automaton{nodes: []ruleNode{{}}, limit: 100}
	if err := a.add([]int{1, 2, 3}, false); err != nil {
		t.Fatal(err)
	}
	if err := a.add([]int{2, 4}, true); err != nil {
		t.Fatal(err)
	}
	a.finish()
	state, ok := a.step(0, 1)
	if !ok {
		t.Fatal("first edge")
	}
	state, ok = a.step(state, 2)
	if !ok {
		t.Fatal("second edge")
	}
	if _, ok = a.step(state, 3); ok {
		t.Fatal("overlapping no rule ignored")
	}
	if _, ok = a.step(state, 5); ok {
		t.Fatal("overlapping only rule ignored")
	}
	if _, ok = a.step(state, 4); !ok {
		t.Fatal("legal exit rejected")
	}
}
