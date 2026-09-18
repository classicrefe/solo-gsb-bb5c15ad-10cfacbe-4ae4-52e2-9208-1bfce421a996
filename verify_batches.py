"""Stdlib-only smoke test for atomic event batches (run without FastAPI installed)."""

import os
import tempfile
import threading
import traceback

tmp = tempfile.mkdtemp()
os.environ["DATABASE_PATH"] = os.path.join(tmp, "verify_batches.db")

from app import db, repo, seals  # noqa: E402

db.initialise()

failures = []


def check(name, cond, extra=""):
    print(f"{'PASS' if cond else 'FAIL'}: {name} {extra}")
    if not cond:
        failures.append(name)


# 1. in-order batch creates the stream and applies everything
s, statuses = repo.submit_event_batch("b1", [("e0", 0, 100), ("e1", 1, -30)])
check("batch created", statuses == ["created", "created"], str(statuses))
check("batch applied", s.balance == 70 and s.next_sequence == 2 and s.pending_sequences == [], str(s))

# 2. out-of-order batch: one catch-up closes the gap
s, statuses = repo.submit_event_batch("b2", [("e2", 2, 50), ("e0", 0, 100), ("e1", 1, 200)])
check("out-of-order batch applied", statuses == ["created"] * 3
      and s.balance == 350 and s.next_sequence == 3 and s.pending_sequences == [], str(s))

# 3. a batch may leave events pending behind a remaining gap
s, _ = repo.submit_event_batch("b3", [("e1", 1, 20), ("e3", 3, 40), ("e0", 0, 10)])
check("batch partial pending", s.balance == 30 and s.next_sequence == 2 and s.pending_sequences == [3], str(s))

# 4. mixed replay + create, statuses align with the input order
s, statuses = repo.submit_event_batch("b1", [("e0", 0, 100), ("e2", 2, 5)])
check("mixed statuses", statuses == ["replayed", "created"], str(statuses))
check("mixed state", s.balance == 75 and s.next_sequence == 3 and s.pending_sequences == [], str(s))

# 5. pure replay batch
s, statuses = repo.submit_event_batch("b1", [("e1", 1, -30), ("e0", 0, 100)])
check("pure replay", statuses == ["replayed", "replayed"] and s.balance == 75 and s.next_sequence == 3, str(statuses))

# 6. replay conflict rolls back the whole batch (incl. earlier new events)
try:
    repo.submit_event_batch("b1", [("e9", 9, 1), ("e0", 0, 999)])
    check("replay conflict -> 409", False)
except repo.ConflictError as e:
    check("replay conflict -> 409", e.code == "replay_conflict")
s = repo.get_stream_state("b1")
check("replay conflict left nothing", s.balance == 75 and s.next_sequence == 3 and s.pending_sequences == [], str(s))

# 7. sequence conflict rolls back the whole batch
try:
    repo.submit_event_batch("b1", [("e3", 3, 1), ("eX", 0, 1)])
    check("sequence conflict -> 409", False)
except repo.ConflictError as e:
    check("sequence conflict -> 409", e.code == "sequence_conflict")
s = repo.get_stream_state("b1")
check("sequence conflict left nothing", s.balance == 75 and s.next_sequence == 3 and s.pending_sequences == [], str(s))

# 8. a conflict also rolls back the would-be catch-up
repo.submit_event("b4", "e1", 1, 100)  # buffered behind the missing sequence 0
try:
    repo.submit_event_batch("b4", [("e0", 0, 50), ("e1", 1, 999)])
    check("catch-up conflict -> 409", False)
except repo.ConflictError as e:
    check("catch-up conflict -> 409", e.code == "replay_conflict")
s = repo.get_stream_state("b4")
check("catch-up rolled back", s.balance == 0 and s.next_sequence == 0 and s.pending_sequences == [1], str(s))

# 9. sealed stream: pure replay allowed, any new event rejects the batch
repo.submit_event_batch("b5", [("e0", 0, 10), ("e1", 1, 20)])
seals.create_seal("b5", "seal-1", 2)
s, statuses = repo.submit_event_batch("b5", [("e1", 1, 20), ("e0", 0, 10)])
check("sealed pure replay", statuses == ["replayed", "replayed"] and s.balance == 30, str(statuses))
try:
    repo.submit_event_batch("b5", [("e0", 0, 10), ("e2", 2, 30)])
    check("sealed new event -> 409", False)
except repo.ConflictError as e:
    check("sealed new event -> 409", e.code == "stream_sealed")
s = repo.get_stream_state("b5")
check("sealed unchanged", s.balance == 30 and s.next_sequence == 2 and s.pending_sequences == [], str(s))

# 10. concurrency: 10 threads x 2 events, each sequence applied exactly once
errors = []
def post_batch(base):
    try:
        repo.submit_event_batch("b6", [(f"e{base}", base, 1), (f"e{base + 1}", base + 1, 1)])
    except Exception:
        errors.append(traceback.format_exc())

threads = [threading.Thread(target=post_batch, args=(i,)) for i in range(0, 20, 2)]
for t in threads:
    t.start()
for t in threads:
    t.join()
check("concurrent batches no errors", not errors, str(errors[:1]))
s = repo.get_stream_state("b6")
check("concurrent batches applied once", s.balance == 20 and s.next_sequence == 20 and s.pending_sequences == [], str(s))

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL BATCH CHECKS PASSED")
