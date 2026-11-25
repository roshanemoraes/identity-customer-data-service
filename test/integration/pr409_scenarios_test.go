/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package integration

// Scenario tests for PR 409 (parallel unification). Each one simulates a race or failure found
// in review and FAILS when the gap reproduces, so the TestGrid shows, per database, which gaps
// are present. Every scenario also prints a "PR409-RESULT" line with its counts.
//
// Run on one database locally:
//
//	CDS_TEST_DB=sqlite go test -count=1 -v -run 'Test_PR409' ./test/integration
//
// Run on every database (Actions > TestGrid > Run workflow, test = Test_PR409):
//
//	gh workflow run testgrid.yml -f test=Test_PR409 --ref <branch>
//
// S6 and S7 need the inbuilt database (they use SQLite's single writer lock) and skip elsewhere.

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	irModel "github.com/wso2/identity-customer-data-service/internal/identity_resolution/model"
	irService "github.com/wso2/identity-customer-data-service/internal/identity_resolution/service"
	irStore "github.com/wso2/identity-customer-data-service/internal/identity_resolution/store"
	irWorker "github.com/wso2/identity-customer-data-service/internal/identity_resolution/worker"
	profileModel "github.com/wso2/identity-customer-data-service/internal/profile/model"
	profileService "github.com/wso2/identity-customer-data-service/internal/profile/service"
	profileStore "github.com/wso2/identity-customer-data-service/internal/profile/store"
	profileSchema "github.com/wso2/identity-customer-data-service/internal/profile_schema/model"
	schemaService "github.com/wso2/identity-customer-data-service/internal/profile_schema/service"
	"github.com/wso2/identity-customer-data-service/internal/system/config"
	"github.com/wso2/identity-customer-data-service/internal/system/constants"
	"github.com/wso2/identity-customer-data-service/internal/system/database"
	errors2 "github.com/wso2/identity-customer-data-service/internal/system/errors"
	"github.com/wso2/identity-customer-data-service/internal/system/workers"
	urService "github.com/wso2/identity-customer-data-service/internal/unification_rules/service"
	"github.com/wso2/identity-customer-data-service/test/setup"
)

func pr409Reportf(t *testing.T, format string, args ...interface{}) {
	t.Helper()
	line := fmt.Sprintf(format, args...)
	t.Log("PR409-RESULT " + t.Name() + ": " + line)
	fmt.Println("PR409-RESULT " + t.Name() + ": " + line)
}

// pr409Jitter returns a random delay below limit, to vary where two racing calls interleave.
func pr409Jitter(limit time.Duration) time.Duration {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(limit)))
	if err != nil {
		return 0
	}
	return time.Duration(n.Int64())
}

func pr409Fuzzy() {
	cfg := config.GetCDSRuntime().Config
	cfg.IdentityResolution.FuzzyMatchingEnabled = true
	config.OverrideCDSRuntime(cfg)
}

// pr409Schema declares string traits so PATCH passes schema validation.
func pr409Schema(t *testing.T, orgID string, names ...string) {
	t.Helper()
	attrs := make([]profileSchema.ProfileSchemaAttribute, 0, len(names))
	for _, n := range names {
		attrs = append(attrs, profileSchema.ProfileSchemaAttribute{OrgID: orgID, AttributeID: uuid.New().String(),
			AttributeName: "traits." + n, ValueType: constants.StringDataType, MergeStrategy: "overwrite",
			Mutability: constants.MutabilityReadWrite})
	}
	if _, err := schemaService.GetProfileSchemaService().AddProfileSchemaAttributesForScope(context.Background(),
		attrs, constants.Traits, orgID); err != nil {
		t.Fatalf("schema: %v", err)
	}
}

func pr409IndexAll(t *testing.T, orgID string) {
	t.Helper()
	rules, err := urService.GetUnificationRuleService().GetUnificationRules(context.Background(), orgID)
	if err != nil {
		t.Fatalf("load rules: %v", err)
	}
	for _, rule := range rules {
		irWorker.IndexNewAttribute(context.Background(), orgID, rule)
	}
}

// pr409MasterOf returns the master a profile belongs to: itself when it is a reference profile.
func pr409MasterOf(t *testing.T, orgID, profileID string) (string, bool) {
	t.Helper()
	p, err := profileStore.GetProfileWithOptions(context.Background(), profileID, orgID,
		profileStore.GetProfileOptions{IncludeApplicationData: false})
	if err != nil || p == nil || p.ProfileStatus == nil {
		return "", false
	}
	if p.ProfileStatus.ReferenceProfileID != "" {
		return p.ProfileStatus.ReferenceProfileID, true
	}
	return p.ProfileID, true
}

func pr409IsChild(t *testing.T, orgID, profileID string) bool {
	t.Helper()
	m, ok := pr409MasterOf(t, orgID, profileID)
	return ok && m != profileID
}

// ── Scenario 1: many profiles for one person unified in parallel ─────────────────────────────

// Test_PR409_S1_ConcurrentSameEmail queues 1 permanent + 7 temporary profiles with the same
// email at once, so the 5 workers race to merge them. Afterwards every profile must belong to
// one master, every child must point at a reference profile, and the master must hold every
// profile's own trait (no data lost).
func Test_PR409_S1_ConcurrentSameEmail(t *testing.T) {
	pr409Fuzzy()
	ctx := context.Background()
	const rounds, temps = 10, 7
	ok, bad := 0, 0
	var problems []string
	for round := range rounds {
		org := newOrg(fmt.Sprintf("pr409-s1-%d", round))
		ref := setup.TestOrg(t, org)
		insertRule(t, org, "email", "traits.email", 1, constants.AttributeTypeEmail,
			constants.UnificationMethodDeterministic)
		email := fmt.Sprintf("alice%d@pr409.io", round)
		ids := make([]string, 0, 1+temps)
		ids = append(ids, insertBareProfile(t, org, "u-alice", map[string]interface{}{
			"email": email, "tag_perm": "perm"}).ProfileID)
		for i := range temps {
			ids = append(ids, insertBareProfile(t, org, "", map[string]interface{}{
				"email": email, fmt.Sprintf("tag_%d", i): fmt.Sprintf("v%d", i)}).ProfileID)
		}
		pr409IndexAll(t, ref.ID)
		for _, id := range ids {
			workers.EnqueueProfileForProcessing(profileModel.Profile{ProfileID: id, OrgID: ref.ID})
		}

		// Wait until every profile belongs to one master, or 30 s.
		var masters map[string]bool
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			masters = map[string]bool{}
			for _, id := range ids {
				m, _ := pr409MasterOf(t, ref.ID, id)
				masters[m] = true
			}
			if len(masters) == 1 {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		time.Sleep(1 * time.Second) // let re-queued jobs settle

		var roundProblems []string
		masters = map[string]bool{}
		for _, id := range ids {
			m, _ := pr409MasterOf(t, ref.ID, id)
			masters[m] = true
			if m != id && pr409IsChild(t, ref.ID, m) {
				roundProblems = append(roundProblems,
					fmt.Sprintf("%s points at %s which is itself a child", id[:8], m[:8]))
			}
		}
		if len(masters) != 1 {
			roundProblems = append(roundProblems, fmt.Sprintf("%d masters instead of 1", len(masters)))
		} else {
			var masterID string
			for m := range masters {
				masterID = m
			}
			master, err := profileStore.GetProfile(ctx, masterID, ref.ID)
			if err != nil || master == nil {
				roundProblems = append(roundProblems, "master unreadable")
			} else {
				want := make([]string, 0, 1+temps)
				want = append(want, "tag_perm")
				for i := range temps {
					want = append(want, fmt.Sprintf("tag_%d", i))
				}
				for _, k := range want {
					if _, ok := master.Traits[k]; !ok {
						roundProblems = append(roundProblems, "master lost trait "+k)
					}
				}
				if master.UserID != "u-alice" {
					roundProblems = append(roundProblems,
						"master is not the permanent profile (userId="+master.UserID+")")
				}
			}
		}
		if len(roundProblems) == 0 {
			ok++
		} else {
			bad++
			problems = append(problems, fmt.Sprintf("round %d: %s", round, strings.Join(roundProblems, "; ")))
		}
	}
	pr409Reportf(t, "%d/%d rounds fully unified with no lost data; %d rounds with problems", ok, rounds, bad)
	for _, p := range problems {
		pr409Reportf(t, "  %s", p)
	}
	if bad > 0 {
		t.Errorf("%d of %d rounds did not end as one master holding every profile's data", bad, rounds)
	}
}

// ── Scenario 2: a PATCH to the master while it is being merged ───────────────────────────────

// Test_PR409_S2_PatchDuringMerge merges temporary P3 (carrying "p3_data") into permanent P2 while
// an app PATCHes P2's color from blue to red at the same moment. Afterwards P2 must have both
// color=red and p3_data. A missing one is a lost update.
func Test_PR409_S2_PatchDuringMerge(t *testing.T) {
	ctx := context.Background()
	const rounds = 40
	var lostPatch, lostMerge, clean, errs int
	for round := range rounds {
		org := newOrg(fmt.Sprintf("pr409-s2-%d", round))
		ref := setup.TestOrg(t, org)
		pr409Schema(t, ref.ID, "email", "color", "p3_data")
		p2 := insertBareProfile(t, org, "u-bob", map[string]interface{}{"email": "bob@pr409.io", "color": "blue"})
		p3 := insertBareProfile(t, org, "", map[string]interface{}{"email": "bob@pr409.io", "p3_data": "from-p3"})

		start := make(chan struct{})
		var wg sync.WaitGroup
		var mergeErr, patchErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, mergeErr = workers.MergeMatchedProfiles(ctx, ref.ID, p2, p3, constants.MergeReasonAutoMerge)
		}()
		go func() {
			defer wg.Done()
			<-start
			time.Sleep(pr409Jitter(3 * time.Millisecond))
			_, patchErr = profileService.GetProfilesService().PatchProfile(ctx, p2.ProfileID, ref.ID,
				map[string]interface{}{"traits": map[string]interface{}{"color": "red"}})
		}()
		close(start)
		wg.Wait()
		if mergeErr != nil || patchErr != nil {
			if errs == 0 {
				pr409Reportf(t, "first error: merge=%v patch=%v", mergeErr, patchErr)
			}
			errs++
			continue
		}
		stored, err := profileStore.GetProfile(ctx, p2.ProfileID, ref.ID)
		if err != nil || stored == nil {
			errs++
			continue
		}
		switch {
		case stored.Traits["color"] != "red":
			lostPatch++
		case stored.Traits["p3_data"] != "from-p3":
			lostMerge++
		default:
			clean++
		}
	}
	pr409Reportf(t, "%d rounds: %d clean, %d lost the PATCH (color back to blue), %d lost P3's merged data, %d errors",
		rounds, clean, lostPatch, lostMerge, errs)
	if lostPatch+lostMerge > 0 {
		t.Errorf("S2: a PATCH and a merge of the same profile lost an update in %d of %d rounds",
			lostPatch+lostMerge, rounds)
	}
	if errs > 0 {
		t.Errorf("%d of %d rounds failed with an error", errs, rounds)
	}
}

// ── Scenario 3: a review task raised for a profile that was merged a moment earlier ──────────

// Test_PR409_S3_ReviewTaskForMergedProfile evaluates P3 (same name as P1, different email, so a
// review task P3↔P1 is raised) while P1 is merged into permanent P2 at the same moment. A
// pending task that names a profile that is now a child is a stale task.
func Test_PR409_S3_ReviewTaskForMergedProfile(t *testing.T) {
	pr409Fuzzy()
	ctx := context.Background()
	const rounds = 200
	var stale, raisedClean, noTask, errs int
	for round := range rounds {
		org := newOrg(fmt.Sprintf("pr409-s3-%d", round))
		ref := setup.TestOrg(t, org)
		insertRule(t, org, "email", "traits.email", 1, constants.AttributeTypeEmail,
			constants.UnificationMethodDeterministic)
		insertRule(t, org, "name", "traits.name", 2, constants.AttributeTypeName, constants.UnificationMethodFuzzy)
		p1 := insertBareProfile(t, org, "", map[string]interface{}{"name": "Marcus Delacroix", "email": "m1@pr409.io"})
		p2 := insertBareProfile(t, org, "u-marcus", map[string]interface{}{"name": "Zed Quill", "email": "m1@pr409.io"})
		p3 := insertBareProfile(t, org, "", map[string]interface{}{"name": "Marcus Delacroix", "email": "m3@other.io"})
		pr409IndexAll(t, ref.ID)

		start := make(chan struct{})
		var wg sync.WaitGroup
		var mergeErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			irWorker.ResolveProfileAsync(ctx, ref.ID, profileModel.Profile{ProfileID: p3.ProfileID, OrgID: ref.ID})
		}()
		go func() {
			defer wg.Done()
			<-start
			time.Sleep(pr409Jitter(4 * time.Millisecond))
			_, mergeErr = workers.MergeMatchedProfiles(ctx, ref.ID, p2, p1, constants.MergeReasonAutoMerge)
		}()
		close(start)
		wg.Wait()
		if mergeErr != nil {
			errs++
			continue
		}
		pending, _, err := irStore.GetPendingReviewTasks(ctx, ref.ID, 50)
		if err != nil {
			errs++
			continue
		}
		if len(pending) == 0 {
			noTask++
			continue
		}
		roundStale := false
		for _, task := range pending {
			if pr409IsChild(t, ref.ID, task.IncomingProfileID) || pr409IsChild(t, ref.ID, task.CandidateProfileID) {
				roundStale = true
			}
		}
		if roundStale {
			stale++
		} else {
			raisedClean++
		}
	}
	pr409Reportf(t, "%d rounds: %d left a PENDING task naming a merged (child) profile, %d raised a valid task, "+
		"%d raised no task, %d errors", rounds, stale, raisedClean, noTask, errs)
	if stale > 0 {
		t.Errorf("S3: %d of %d rounds left a pending review task naming a profile that is now a child",
			stale, rounds)
	}
	if errs > 0 {
		t.Errorf("%d of %d rounds failed with an error", errs, rounds)
	}
}

// ── Scenario 4: rejecting an out-of-date review task ─────────────────────────────────────────

// Test_PR409_S4_RejectStaleTask merges P1 into permanent P2, and a task P1↔P3 is still pending
// (raised before the merge, as by an earlier version or the race of scenario 3). The admin
// REJECTS it. Approving now returns 409; check what rejecting does and where the rejection lands.
func Test_PR409_S4_RejectStaleTask(t *testing.T) {
	ctx := context.Background()
	org := newOrg("pr409-s4")
	ref := setup.TestOrg(t, org)
	p1 := insertBareProfile(t, org, "", map[string]interface{}{"name": "Gil"})
	p3 := insertBareProfile(t, org, "", map[string]interface{}{"name": "Ivy"})
	p2 := insertBareProfile(t, org, "u-gil", map[string]interface{}{"name": "Hal"})
	if _, err := workers.MergeMatchedProfiles(ctx, ref.ID, p2, p1, constants.MergeReasonAutoMerge); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if err := irStore.InsertReviewTask(ctx, ref.ID, irModel.ReviewTask{OrgID: ref.ID,
		IncomingProfileID: p1.ProfileID, CandidateProfileID: p3.ProfileID, MatchScore: 0.8,
		Status: constants.ReviewStatusPending, ScoreBreakdown: map[string]float64{"traits.name": 1}}); err != nil {
		t.Fatalf("task: %v", err)
	}
	pending, _, _ := irStore.GetPendingReviewTasks(ctx, ref.ID, 10)
	if len(pending) != 1 {
		t.Fatalf("want 1 pending task, got %d", len(pending))
	}
	rejectErr := irService.GetIdentityResolutionService().ResolveReviewTask(ctx, ref.ID, pending[0].ID, false,
		"admin", "")
	task, _ := irStore.GetReviewTaskByID(ctx, pending[0].ID)
	if task == nil {
		t.Fatal("task vanished")
	}
	onChild, _ := irStore.GetRejectionsForProfile(ctx, ref.ID, p1.ProfileID)
	onMaster, _ := irStore.GetRejectionsForProfile(ctx, ref.ID, p2.ProfileID)
	_, childHas := onChild[p3.ProfileID]
	_, masterHas := onMaster[p3.ProfileID]
	pr409Reportf(t, "reject returned err=%v, task status=%s; rejection stored on child P1=%v, on master P2=%v",
		rejectErr, task.Status, childHas, masterHas)
	if !masterHas {
		t.Errorf("S4: rejecting a task whose profile was merged recorded the rejection on the child, " +
			"not on the master it belongs to, so the master and the candidate can be proposed again")
	}
}

// ── Scenario 5: two admins approve the same task at the same moment ──────────────────────────

func Test_PR409_S5_ConcurrentApprovals(t *testing.T) {
	ctx := context.Background()
	const rounds = 30
	outcomes := map[string]int{}
	for round := range rounds {
		org := newOrg(fmt.Sprintf("pr409-s5-%d", round))
		ref := setup.TestOrg(t, org)
		p1 := insertBareProfile(t, org, "", map[string]interface{}{"name": "Gil", "email": "a@pr409.io"})
		p3 := insertBareProfile(t, org, "", map[string]interface{}{"name": "Gil", "email": "b@pr409.io"})
		if err := irStore.InsertReviewTask(ctx, ref.ID, irModel.ReviewTask{OrgID: ref.ID,
			IncomingProfileID: p1.ProfileID, CandidateProfileID: p3.ProfileID, MatchScore: 0.8,
			Status: constants.ReviewStatusPending, ScoreBreakdown: map[string]float64{"traits.name": 1}}); err != nil {
			t.Fatalf("task: %v", err)
		}
		pending, _, _ := irStore.GetPendingReviewTasks(ctx, ref.ID, 10)
		taskID := pending[0].ID
		start := make(chan struct{})
		var wg sync.WaitGroup
		results := make([]string, 2)
		for i := range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				err := irService.GetIdentityResolutionService().ResolveReviewTask(ctx, ref.ID, taskID, true,
					fmt.Sprintf("admin%d", i+1), "")
				var clientErr *errors2.ClientError
				switch {
				case err == nil:
					results[i] = "ok"
				case errors.As(err, &clientErr):
					results[i] = fmt.Sprintf("%d(%s)", clientErr.StatusCode, clientErr.ErrorMessage.Code)
				default:
					results[i] = "err"
				}
			}()
		}
		close(start)
		wg.Wait()
		sort.Strings(results)
		task, _ := irStore.GetReviewTaskByID(ctx, taskID)
		m1, _ := pr409MasterOf(t, ref.ID, p1.ProfileID)
		m3, _ := pr409MasterOf(t, ref.ID, p3.ProfileID)
		merged := m1 == m3
		outcomes[fmt.Sprintf("calls=%s merged=%v status=%s", strings.Join(results, "+"), merged, task.Status)]++
	}
	pr409Reportf(t, "%d rounds of two simultaneous approvals:", rounds)
	keys := make([]string, 0, len(outcomes))
	for k := range outcomes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		pr409Reportf(t, "  %2d × %s", outcomes[k], k)
		if !strings.HasSuffix(k, "merged=true status="+constants.ReviewStatusApproved) {
			t.Errorf("S5: %d of %d rounds of two simultaneous approvals ended as %q, want the profiles merged "+
				"and the task %s", outcomes[k], rounds, k, constants.ReviewStatusApproved)
		}
	}
}

// ── Scenario 6: a burst of profiles on SQLite, with the API in use ───────────────────────────

func pr409RestartWorkers(t *testing.T, n int) {
	t.Helper()
	if err := workers.StopProfileWorker(context.Background()); err != nil {
		t.Logf("stop worker: %v", err)
	}
	cfg := config.GetCDSRuntime().Config
	cfg.IdentityResolution.UnificationWorkers = n
	cfg.IdentityResolution.FuzzyMatchingEnabled = true
	config.OverrideCDSRuntime(cfg)
	if err := workers.StartProfileWorker(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
}

func pr409Percentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return d[int(float64(len(d)-1)*p)]
}

func Test_PR409_S6_SQLiteBurst(t *testing.T) {
	if suiteDBType != database.TypeSQLite {
		t.Skip("SQLite only")
	}
	defer pr409RestartWorkers(t, 5)
	for _, n := range []int{1, 5} {
		pr409RestartWorkers(t, n)
		ctx := context.Background()
		org := newOrg(fmt.Sprintf("pr409-s6-w%d", n))
		ref := setup.TestOrg(t, org)
		pr409Schema(t, ref.ID, "email", "counter", "seen")
		insertRule(t, org, "email", "traits.email", 1, constants.AttributeTypeEmail,
			constants.UnificationMethodDeterministic)
		const pairs = 900
		type pair struct{ perm, temp string }
		all := make([]pair, 0, pairs)
		for i := range pairs {
			email := fmt.Sprintf("burst%d@pr409.io", i)
			a := insertBareProfile(t, org, fmt.Sprintf("u-%d", i), map[string]interface{}{"email": email})
			b := insertBareProfile(t, org, "", map[string]interface{}{"email": email, "seen": fmt.Sprint(i)})
			all = append(all, pair{a.ProfileID, b.ProfileID})
		}
		bystander := insertBareProfile(t, org, "u-bystander", map[string]interface{}{"email": "nobody@pr409.io"})
		pr409IndexAll(t, ref.ID)

		var readLat, writeLat []time.Duration
		var readErrs, writeErrs atomic.Int64
		var mu sync.Mutex
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { // an app reading profiles
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				p := all[(i*7919)%len(all)]
				s := time.Now()
				_, err := profileStore.GetProfile(ctx, p.temp, ref.ID)
				mu.Lock()
				readLat = append(readLat, time.Since(s))
				mu.Unlock()
				if err != nil {
					readErrs.Add(1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
		go func() { // an app updating an unrelated profile
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				i++
				s := time.Now()
				_, err := profileService.GetProfilesService().PatchProfile(ctx, bystander.ProfileID, ref.ID,
					map[string]interface{}{"traits": map[string]interface{}{"counter": fmt.Sprint(i)}})
				mu.Lock()
				writeLat = append(writeLat, time.Since(s))
				mu.Unlock()
				if err != nil {
					if writeErrs.Add(1) == 1 {
						pr409Reportf(t, "first patch error: %v", err)
					}
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()

		begin := time.Now()
		for _, p := range all {
			workers.EnqueueProfileForProcessing(profileModel.Profile{ProfileID: p.temp, OrgID: ref.ID})
		}
		unmerged := pairs
		deadline := time.Now().Add(120 * time.Second)
		for time.Now().Before(deadline) {
			unmerged = 0
			for _, p := range all {
				m, _ := pr409MasterOf(t, ref.ID, p.temp)
				if m != p.perm {
					unmerged++
				}
			}
			if unmerged == 0 {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		elapsed := time.Since(begin)
		close(stop)
		wg.Wait()
		mu.Lock()
		pr409Reportf(t, "workers=%d: %d/%d pairs merged in %s; API read p50=%s p95=%s max=%s errors=%d; "+
			"API patch p50=%s p95=%s max=%s errors=%d", n, pairs-unmerged, pairs, elapsed.Round(time.Millisecond),
			pr409Percentile(readLat, .5).Round(time.Microsecond), pr409Percentile(readLat, .95).Round(time.Microsecond),
			pr409Percentile(readLat, 1).Round(time.Millisecond), readErrs.Load(),
			pr409Percentile(writeLat, .5).Round(time.Microsecond),
			pr409Percentile(writeLat, .95).Round(time.Microsecond),
			pr409Percentile(writeLat, 1).Round(time.Millisecond), writeErrs.Load())
		mu.Unlock()
		if unmerged > 0 || readErrs.Load() > 0 || writeErrs.Load() > 0 {
			t.Errorf("workers=%d: %d pairs unmerged, %d read errors, %d patch errors during the burst",
				n, unmerged, readErrs.Load(), writeErrs.Load())
		}
	}
}

// ── Scenario 7: the database is briefly unavailable for writes ───────────────────────────────

// Test_PR409_S7_TransientLockLosesJobs holds SQLite's write lock from another connection for 30 s
// (as a long write, a backup, or another process would) while 20 pairs are queued. Merges that
// wait more than the 5 s busy timeout fail. After the lock is released, count the pairs that are
// never merged because their failed job was not retried.
func Test_PR409_S7_TransientLockLosesJobs(t *testing.T) {
	if suiteDBType != database.TypeSQLite {
		t.Skip("SQLite only")
	}
	pr409Fuzzy()
	ctx := context.Background()
	var file string
	rows, err := suiteDB.QueryContext(ctx, "PRAGMA database_list")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var seq int
		var name, f string
		_ = rows.Scan(&seq, &name, &f)
		if name == "main" {
			file = f
		}
	}
	_ = rows.Close()

	org := newOrg("pr409-s7")
	ref := setup.TestOrg(t, org)
	insertRule(t, org, "email", "traits.email", 1, constants.AttributeTypeEmail,
		constants.UnificationMethodDeterministic)
	const pairs = 20
	type pair struct{ perm, temp string }
	all := make([]pair, 0, pairs)
	for i := range pairs {
		email := fmt.Sprintf("lock%d@pr409.io", i)
		a := insertBareProfile(t, org, fmt.Sprintf("u-%d", i), map[string]interface{}{"email": email})
		b := insertBareProfile(t, org, "", map[string]interface{}{"email": email})
		all = append(all, pair{a.ProfileID, b.ProfileID})
	}
	pr409IndexAll(t, ref.ID)

	other, err := sql.Open(database.DriverSQLite, file+"?"+database.DefaultSQLiteOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	conn, err := other.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("take write lock: %v", err)
	}
	for _, p := range all {
		workers.EnqueueProfileForProcessing(profileModel.Profile{ProfileID: p.temp, OrgID: ref.ID})
	}
	time.Sleep(pr409LockHold)
	if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("release write lock: %v", err)
	}
	_ = conn.Close()

	countUnmerged := func() int {
		n := 0
		for _, p := range all {
			m, _ := pr409MasterOf(t, ref.ID, p.temp)
			if m != p.perm {
				n++
			}
		}
		return n
	}
	time.Sleep(15 * time.Second) // ample time for any retry
	stuck := countUnmerged()
	// Queue them again by hand: they merge, so nothing but the missing retry kept them apart.
	for _, p := range all {
		workers.EnqueueProfileForProcessing(profileModel.Profile{ProfileID: p.temp, OrgID: ref.ID})
	}
	time.Sleep(10 * time.Second)
	after := countUnmerged()
	pr409Reportf(t, "write lock held %s while %d pairs were queued: %d pairs still unmerged 15 s after release "+
		"(no retry); after queuing them again by hand, %d unmerged", pr409LockHold, pairs, stuck, after)
	if stuck > 0 {
		t.Errorf("S7: %d of %d queued profiles were never unified after a temporary write lock, because a "+
			"merge that fails is not retried", stuck, pairs)
	}
	if after > 0 {
		t.Errorf("%d pairs stayed unmerged even after they were queued again", after)
	}
}

const pr409LockHold = 30 * time.Second

// Test_PR409_S5b_SingleApprovalControl is the control for S5: one approval on its own.
func Test_PR409_S5b_SingleApprovalControl(t *testing.T) {
	ctx := context.Background()
	org := newOrg("pr409-s5b")
	ref := setup.TestOrg(t, org)
	p1 := insertBareProfile(t, org, "", map[string]interface{}{"name": "Gil", "email": "a@pr409.io"})
	p3 := insertBareProfile(t, org, "", map[string]interface{}{"name": "Gil", "email": "b@pr409.io"})
	if err := irStore.InsertReviewTask(ctx, ref.ID, irModel.ReviewTask{OrgID: ref.ID,
		IncomingProfileID: p1.ProfileID, CandidateProfileID: p3.ProfileID, MatchScore: 0.8,
		Status: constants.ReviewStatusPending, ScoreBreakdown: map[string]float64{"traits.name": 1}}); err != nil {
		t.Fatalf("task: %v", err)
	}
	pending, _, _ := irStore.GetPendingReviewTasks(ctx, ref.ID, 10)
	err := irService.GetIdentityResolutionService().ResolveReviewTask(ctx, ref.ID, pending[0].ID, true, "admin", "")
	task, _ := irStore.GetReviewTaskByID(ctx, pending[0].ID)
	m1, _ := pr409MasterOf(t, ref.ID, p1.ProfileID)
	m3, _ := pr409MasterOf(t, ref.ID, p3.ProfileID)
	pr409Reportf(t, "one approval: err=%v merged=%v status=%s", err, m1 == m3, task.Status)
	if err != nil || m1 != m3 || task.Status != constants.ReviewStatusApproved {
		t.Errorf("one approval: err=%v merged=%v status=%s, want no error, merged and %s",
			err, m1 == m3, task.Status, constants.ReviewStatusApproved)
	}
}

// Test_PR409_S1b_ConcurrentEvaluations runs the evaluation of 1 permanent + 7 temporary profiles
// with the same email at the same moment, as 8 CDS instances (or 8 workers) would, without the
// queue. It reports whether they end up under one master with no lost data.
func Test_PR409_S1b_ConcurrentEvaluations(t *testing.T) {
	pr409Fuzzy()
	ctx := context.Background()
	const rounds, temps = 20, 7
	ok := 0
	problems := map[string]int{}
	for round := range rounds {
		org := newOrg(fmt.Sprintf("pr409-s1b-%d", round))
		ref := setup.TestOrg(t, org)
		insertRule(t, org, "email", "traits.email", 1, constants.AttributeTypeEmail,
			constants.UnificationMethodDeterministic)
		email := fmt.Sprintf("carol%d@pr409.io", round)
		ids := make([]string, 0, 1+temps)
		ids = append(ids, insertBareProfile(t, org, "u-carol", map[string]interface{}{
			"email": email, "tag_perm": "perm"}).ProfileID)
		for i := range temps {
			ids = append(ids, insertBareProfile(t, org, "", map[string]interface{}{
				"email": email, fmt.Sprintf("tag_%d", i): fmt.Sprintf("v%d", i)}).ProfileID)
		}
		pr409IndexAll(t, ref.ID)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, id := range ids {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { _ = recover() }()
				<-start
				irWorker.ResolveProfileAsync(ctx, ref.ID, profileModel.Profile{ProfileID: id, OrgID: ref.ID})
			}()
		}
		close(start)
		wg.Wait()
		// Wait up to 20 s for the profiles to settle under one master, since a stale merge
		// queues its profiles again and they finish through the queue.
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			settled := map[string]bool{}
			for _, id := range ids {
				m, _ := pr409MasterOf(t, ref.ID, id)
				settled[m] = true
			}
			if len(settled) == 1 {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		time.Sleep(1 * time.Second) // let re-queued jobs settle
		masters := map[string]bool{}
		var roundProblems []string
		for _, id := range ids {
			m, found := pr409MasterOf(t, ref.ID, id)
			if !found {
				roundProblems = append(roundProblems, "profile unreadable")
				continue
			}
			masters[m] = true
			if m != id && pr409IsChild(t, ref.ID, m) {
				roundProblems = append(roundProblems, "child points at a child")
			}
		}
		if len(masters) != 1 {
			roundProblems = append(roundProblems, fmt.Sprintf("%d masters", len(masters)))
		} else {
			for m := range masters {
				master, _ := profileStore.GetProfile(ctx, m, ref.ID)
				if master == nil {
					roundProblems = append(roundProblems, "master unreadable")
					break
				}
				lost := 0
				if _, has := master.Traits["tag_perm"]; !has {
					lost++
				}
				for i := range temps {
					if _, has := master.Traits[fmt.Sprintf("tag_%d", i)]; !has {
						lost++
					}
				}
				if lost > 0 {
					roundProblems = append(roundProblems, "master lost traits")
				}
			}
		}
		if len(roundProblems) == 0 {
			ok++
		}
		for _, p := range roundProblems {
			problems[p]++
		}
	}
	pr409Reportf(t, "%d/%d rounds ended as one master with all data; problems seen: %v", ok, rounds, problems)
	if ok < rounds {
		t.Errorf("S1b: %d of %d rounds of simultaneous evaluations did not end as one master holding every "+
			"profile's data, 20 s after they ran: %v", rounds-ok, rounds, problems)
	}
}
