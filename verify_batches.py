"""Stdlib-only smoke test for batch event submission (run without FastAPI installed)."""

import os
import tempfile
import threading
import traceback

tmp = tempfile.mkdtemp()
os.environ["DATABASE_PATH"] = os.path.join(tmp, "verify.db")

from app import db, repo, seals  # noqa: E402

db.initialise()

failures = []


def check(name, cond, extra=""):
    print(f"{'PASS' if cond else 'FAIL'}: {name} {extra}")
    if not cond:
        failures.append(name)


def ev(event_id, sequence, delta):
    return repo.BatchEvent(event_id=event_id, sequence=sequence, delta=delta)


# 1. in-order batch on a fresh stream: all created, stream auto-created
s, results = repo.submit_event_batch(
    "b1", [ev("e0", 0, 100), ev("e1", 1, -30), ev("e2", 2, 50)]
)
check(
    "batch creates in order",
    [r.status for r in results] == ["created", "created", "created"]
    and s.balance == 120
    and s.next_sequence == 3
    and s.pending_sequences == [],
    str(s),
)

# 2. out-of-order batch: one catch-up pass applies the contiguous run
s, results = repo.submit_event_batch(
    "b2", [ev("e2", 2, 30), ev("e0", 0, 10), ev("e1", 1, 20)]
)
check(
    "out-of-order batch caught up once",
    [r.event_id for r in results] == ["e2", "e0", "e1"]
    and s.balance == 60
    and s.next_sequence == 3
    and s.pending_sequences == [],
    str(s),
)

# 3. batch with a gap leaves the tail pending; a later batch closes it
s, _ = repo.submit_event_batch("b3", [ev("e0", 0, 10), ev("e2", 2, 30)])
check(
    "gap leaves tail pending",
    s.balance == 10 and s.next_sequence == 1 and s.pending_sequences == [2],
    str(s),
)
s, _ = repo.submit_event_batch("b3", [ev("e1", 1, 20)])
check(
    "later batch closes gap",
    s.balance == 60 and s.next_sequence == 3 and s.pending_sequences == [],
    str(s),
)

# 4. all-replay batch: every item replayed, nothing changes
s, results = repo.submit_event_batch("b1", [ev("e1", 1, -30), ev("e0", 0, 100)])
check(
    "all replayed",
    all(r.status == "replayed" for r in results)
    and s.balance == 120
    and s.next_sequence == 3,
    str(s),
)

# 5. mixed batch: replayed + created, reported in input order
s, results = repo.submit_event_batch("b1", [ev("e0", 0, 100), ev("e3", 3, 5)])
check(
    "mixed replayed/created",
    [r.status for r in results] == ["replayed", "created"]
    and s.balance == 125
    and s.next_sequence == 4,
    str(s),
)

# 6. replay conflict: whole batch rolls back, earlier new items do not survive
try:
    repo.submit_event_batch("b1", [ev("n1", 4, 1), ev("e0", 0, 999)])
    check("replay conflict -> 409", False)
except repo.ConflictError as e:
    check("replay conflict -> 409", e.code == "replay_conflict")
s = repo.get_stream_state("b1")
check(
    "replay conflict left nothing",
    s.balance == 125 and s.next_sequence == 4 and s.pending_sequences == [],
    str(s),
)

# 7. sequence conflict: whole batch rolls back
try:
    repo.submit_event_batch("b1", [ev("n2", 4, 1), ev("n3", 0, 1)])
    # seq 0 occupied by e0
    check("sequence conflict -> 409", False)
except repo.ConflictError as e:
    check("sequence conflict -> 409", e.code == "sequence_conflict")
s = repo.get_stream_state("b1")
check(
    "sequence conflict left nothing",
    s.balance == 125 and s.next_sequence == 4 and s.pending_sequences == [],
    str(s),
)

# 8. in-batch duplicates are rejected and write nothing
try:
    repo.submit_event_batch("b8", [ev("e0", 0, 1), ev("e0", 1, 2)])
    check("duplicate event_id -> 409", False)
except repo.ConflictError as e:
    check("duplicate event_id -> 409", e.code == "duplicate_event_id")
check("duplicate event_id wrote nothing", repo.get_stream_state("b8") is None)
try:
    repo.submit_event_batch("b8b", [ev("e0", 0, 1), ev("e1", 0, 2)])
    check("duplicate sequence -> 409", False)
except repo.ConflictError as e:
    check("duplicate sequence -> 409", e.code == "duplicate_sequence")
check("duplicate sequence wrote nothing", repo.get_stream_state("b8b") is None)

# 9. sealed stream: full replay allowed, any new event rejects the batch
repo.submit_event_batch("b9", [ev("e0", 0, 10), ev("e1", 1, 20)])
seals.create_seal("b9", "seal-1", 2)
s, results = repo.submit_event_batch("b9", [ev("e0", 0, 10), ev("e1", 1, 20)])
check(
    "sealed full replay",
    all(r.status == "replayed" for r in results) and s.balance == 30,
    str(s),
)
try:
    repo.submit_event_batch("b9", [ev("e0", 0, 10), ev("e2", 2, 30)])
    check("sealed new event -> 409", False)
except repo.ConflictError as e:
    check("sealed new event -> 409", e.code == "stream_sealed")
s = repo.get_stream_state("b9")
check(
    "sealed batch left nothing",
    s.balance == 30 and s.next_sequence == 2 and s.pending_sequences == [],
    str(s),
)

# 10. empty batch is a programming error
try:
    repo.submit_event_batch("b10", [])
    check("empty batch rejected", False)
except ValueError:
    check("empty batch rejected", True)

# 11. concurrency: 20 identical batches create the events exactly once
outcomes = []
errors = []


def post_same():
    try:
        _, res = repo.submit_event_batch("b11", [ev(f"e{i}", i, 1) for i in range(5)])
        outcomes.append(all(r.status == "replayed" for r in res))
    except Exception:
        errors.append(traceback.format_exc())


threads = [threading.Thread(target=post_same) for _ in range(20)]
for t in threads:
    t.start()
for t in threads:
    t.join()
check("concurrent batches no errors", not errors, str(errors[:1]))
check(
    "exactly one batch created",
    outcomes.count(False) == 1 and outcomes.count(True) == 19,
    str(outcomes),
)
s = repo.get_stream_state("b11")
check(
    "concurrent batches applied once",
    s.balance == 5 and s.next_sequence == 5 and s.pending_sequences == [],
    str(s),
)

# 12. concurrency: disjoint batches apply every sequence exactly once
errors = []


def post_disjoint(base):
    try:
        repo.submit_event_batch(
            "b12", [ev(f"e{base}", base, 1), ev(f"e{base + 1}", base + 1, 1)]
        )
    except Exception:
        errors.append(traceback.format_exc())


threads = [threading.Thread(target=post_disjoint, args=(i * 2,)) for i in range(10)]
for t in threads:
    t.start()
for t in threads:
    t.join()
check("disjoint batches no errors", not errors, str(errors[:1]))
s = repo.get_stream_state("b12")
check(
    "disjoint batches applied once",
    s.balance == 20 and s.next_sequence == 20 and s.pending_sequences == [],
    str(s),
)

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL BATCH CHECKS PASSED")
