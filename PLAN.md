# Block Stage Streaming Mode Plan

## Goal
Change BlockStage from processing one whole block per slot to processing multiple streamed messages per slot, with minimal diff.

## Current Architecture
- `block_receiver: Receiver<HarmonicBlock>` receives one message, processes it as a whole block
- `block_should_schedule()` claims the slot atomically (sets BLOCK_CLAIMED_BIT)
- After block execution: `block_execution_finished()` clears bit → vanilla can proceed
- On failure: `block_failed()` reverts → vanilla can claim
- Vote mutual exclusion: `wait_for_votes_to_finish()` before block execution, `begin_vote_processing()`/`end_vote_processing()` around vote batches

## Slot Timeline (New)

```
|--- delegation period (7/8 slot) ---|--- vote restore period (1/8 slot) ---|
|  stream messages, process each     |  stop recv new msgs, finish current  |
|  votes limited (4M CU)             |  restore vote limit, more votes      |
|  block_claim held                  |  block_claim still held              |
|                                    |  NO non-vote appending (banking+     |
|                                    |  bundle) UNLESS not leader next slot |
```

## Changes

### 1. `block_stage/mod.rs` — `process_loop` (main change)

**Current**: recv one message → claim → process whole block → done.

**New**:
- Recv first message → claim slot (as today via `maybe_consume::<false>`)
- Loop: recv more messages for same slot, process each one
- If any message fails during processing → stop processing for remainder of slot
- After delegation period ends → stop receiving new messages (but finish current in-flight processing)
- Restore vote limit after delegation period
- Do NOT transition to vanilla (don't call `block_execution_finished`) UNLESS we're not leader in next slot — this prevents non-vote appending by banking+bundle during the vote restore period

Pseudocode:
```rust
// Outer loop (per slot)
match block_receiver.recv_timeout(...) {
    Ok(block) => {
        // ... existing slot/leader checks ...
        // Claim slot (existing logic)
        if let BufferedPacketsDecision::Consume(working_bank) = DecisionMaker::maybe_consume::<false>(decision) {
            scheduler_synchronization::wait_for_votes_to_finish();

            let mut failed = false;

            // Process first message
            failed = !self.process_single_message(&block, &working_bank, &root_bank, ...);

            // Stream loop: keep receiving messages for this slot
            while !failed {
                // Check if we're past delegation period
                if past_delegation_period(&working_bank) {
                    break; // Stop receiving new messages
                }

                match block_receiver.recv_timeout(Duration::from_millis(1)) {
                    Ok(next_block) => {
                        if next_block.intended_slot() != current_slot {
                            break; // Wrong slot, done with streaming
                        }
                        // Wait for votes before each message
                        scheduler_synchronization::wait_for_votes_to_finish();
                        failed = !self.process_single_message(&next_block, &working_bank, &root_bank, ...);
                    }
                    Err(RecvTimeoutError::Timeout) => continue, // Keep waiting for more messages
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }

            // Restore vote limit (existing call, same position conceptually)
            working_bank.restore_vote_limit();

            if failed {
                scheduler_synchronization::block_failed(current_slot);
            } else {
                // Only transition to vanilla if NOT leader in next slot
                // This prevents non-vote appending during vote restore period
                let is_leader_next_slot = keypair.pubkey() == working_bank
                    .leader_schedule_cache()... // or check via bank_forks
                if !is_leader_next_slot {
                    scheduler_synchronization::block_execution_finished(current_slot);
                }
                // If leader next slot: keep block_claim held, preventing vanilla/bundle
                // The claim will naturally be superseded when next slot's claim happens
            }
        }
    }
}
```

### 2. `scheduler_synchronization.rs` — Turn claim into consume

**Current**: `block_should_schedule` does a CAS to set `BLOCK_CLAIMED_BIT`. This is effectively a "claim". There's no separate "consume" step.

**Proposed**: No change needed to the atomic state machine. The claim IS the consume — once block claims, it holds the slot for the duration. The key insight: we don't need a separate claim→consume transition because:
- Block claims once at first message (existing behavior)
- Block holds the claim while streaming multiple messages
- Block either fails (revert) or finishes (clear bit)

The "turn a block claim into a block consume" requirement is already satisfied by the existing atomic — claiming IS consuming for the block stage. The fallback mode (vanilla + bundle) remains unchanged: if block doesn't claim during delegation, vanilla claims during fallback.

**No changes needed here** — this keeps the diff minimal.

### 3. Vote processing — Bank hash mismatch prevention

**Current logic** (must be preserved):
- `wait_for_votes_to_finish()` before each block message execution
- `begin_vote_processing()`/`end_vote_processing()` around each vote batch
- Peterson's algorithm via `VOTE_PROCESSING` + `SCHEDULER_STATE` atomics

**Change**: Call `wait_for_votes_to_finish()` before EACH streamed message (not just the first). This is critical — each message may touch vote accounts, so we need mutual exclusion per-message.

The `is_block_executing()` check in vote worker already works correctly: as long as BLOCK_CLAIMED_BIT is set for the current slot, votes will see the block is executing and defer. This is unchanged.

### 4. `process_single_message` — Extract helper from existing code

Extract the translate + crank + process logic from the current monolithic block into a helper method that processes one `HarmonicBlock` message. This keeps the diff small — it's just a refactor of existing code, no logic changes to the processing itself.

### 5. Delegation period check

Need a helper to check if we're past the delegation period. This can reuse the same tick math from `DecisionMaker::maybe_consume`:
```rust
fn past_delegation_period(bank: &Bank) -> bool {
    let current_tick_height = bank.tick_height();
    let max_tick_height = bank.max_tick_height();
    let bank_ticks_per_slot = bank.ticks_per_slot();
    let start_tick = max_tick_height - bank_ticks_per_slot;
    let ticks_into_slot = current_tick_height.saturating_sub(start_tick);
    let delegation_period_length = bank_ticks_per_slot * 7 / 8;
    ticks_into_slot >= delegation_period_length
}
```

### 6. "Not leader in next slot" check

Need to determine if we're leader in `current_slot + 1`. Options:
- Pass `LeaderScheduleCache` into BlockStage (cleanest, but adds a parameter)
- Use `bank_forks` to check — but leader_schedule_cache isn't directly on bank_forks
- Check via `ClusterInfo` + poh_recorder — but we don't have poh_recorder in block stage

**Recommendation**: Pass `Arc<LeaderScheduleCache>` into `BlockStage::new()` and down to `process_loop`. This is a one-line addition to the constructor and thread spawn. Then:
```rust
let next_slot_leader = leader_schedule_cache.slot_leader_at(current_slot + 1, Some(&working_bank));
let is_leader_next = next_slot_leader == Some(keypair.pubkey());
```

## Files Changed
1. **`core/src/block_stage/mod.rs`** — Main changes: streaming loop, extract helper, delegation period check, next-slot leader check
2. **`core/src/tpu.rs`** — Pass `leader_schedule_cache` to `BlockStage::new()`
3. **`core/src/validator.rs`** — (only if leader_schedule_cache needs to be threaded through)

## Files NOT Changed (minimizing diff)
- `scheduler_synchronization.rs` — No changes needed, existing state machine works
- `block_consumer.rs` — No changes, still processes one batch at a time
- `decision_maker.rs` — No changes
- `vote_worker.rs` — No changes, existing mutual exclusion works
- `devin_scheduler.rs` — No changes

## Key Invariants Preserved
1. **Vote mutual exclusion**: `wait_for_votes_to_finish()` called before each message, `begin/end_vote_processing` unchanged
2. **Bank hash mismatch prevention**: Block claim held for entire streaming duration, votes deferred while `is_block_executing()` returns true
3. **Fallback mode**: If block never claims (no messages during delegation), vanilla claims during fallback — unchanged
4. **Crank transactions**: Only prepended to first message (or each message — need to clarify)

## Open Questions
1. **Crank transactions**: Should cranks be prepended to every streamed message or just the first? (Likely just the first, since they're one-time-per-slot operations like tip distribution init)
2. **Vote limit restore timing**: Should we restore immediately when delegation period ends, or after the last in-flight message finishes? (Proposed: after delegation period ends and current message finishes processing, since restore_vote_limit is called once)
3. **Holding block claim when leader next slot**: The claim for slot N will be superseded by the claim for slot N+1 — is there any edge case where we need to explicitly clear it? (Likely no, since `block_should_schedule` for N+1 overwrites via the `Less` branch in the CAS)
