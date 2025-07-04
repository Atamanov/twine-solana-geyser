# ZK Proof of Ethereum Consensus: A High-Level Design

This document outlines a design for proving Ethereum's consensus mechanism within a zero-knowledge virtual machine (zkVM). The goal is to create a trustless ZK light client capable of verifying beacon chain state and data.

## Key Concepts & Notation

| Symbol | Description |
| :--- | :--- |
| **b₀** | The last **trusted** beacon chain block header stored by the client. This is the initial trust anchor. |
| **E₀** | The epoch that contains block **b₀**. |
| **S₀** | The state root of block **b₀** (`b₀.state_root`). This is a public input to the ZK circuit. |
| **Vroot₀** | The Merkle root of the **active validator set** for the in epoch **E₀**. This is derived from the state at **b₀**. |
| **seed(E)**| The canonical epoch seed (`get_seed` from the spec) derived from the RANDAO mix of epoch `E-1`. Used for shuffling. |
| **shuffle(seed, i)** | The permutation `compute_shuffled_index(i, seed)` from the Ethereum specification. |
| **σ** | The aggregated BLS signature for a given block header. |

---

## 1. Bootstrap Process

The ZK light client must be initialized with a trusted state to begin verification.

1.  **Initial Anchor:** The client is pre-loaded with a recent and trusted beacon block header, **b₀**.
2.  **Validator Set Root:** From the state of **b₀** (referenced by its state root **S₀**), we derive the Merkle root of the current full validator set, **Vroot₀**. This computation is performed once during initialization.
3.  **Epoch Look-ahead:** Due to the 1-epoch look-ahead in committee assignment, the validator set rooted at **Vroot₀** is responsible for signing all blocks in both epoch **E₀** and **E₀ + 1**. This allows the client to verify headers for up to two epochs using the initial `Vroot₀`.

---

## 2. Per-Block Verification Proof

For any given block **Bₛ** (at slot `s` where `s ≤ E₀ + 1`), the client generates a ZK proof to verify its validity and any associated data.

### ZK Circuit Inputs

| Type | Input | Description |
| :--- | :--- | :--- |
| **Public** | `S₀` (State Root of `b₀`) | The trusted state root from our anchor block. |
| **Public** | Header Chain `b₀ → … → Bₛ` | A list of headers linking the anchor to the target block. |
| **Public** | Aggregated Signature `σ` | The aggregated signature for the header of **Bₛ**. |
| **Public** | Aggregated Public Key `pkₛ`  | The aggregated public key used to verify the signature. |
| **Public** | Data Proof (optional) | A Merkle proof of transaction inclusion or a state value against `Bₛ.state_root`. |
| **Private**| Validator Public Keys & Proofs | The public keys of the validators who participated in the sync committee and their Merkle proofs against **Vroot₀**. |
| **Private**| Signer Bitfield | A bitfield indicating which of the committee members contributed to the aggregated signature **σ**. |

### ZK Circuit Checks

The circuit performs the following critical validations:

1.  **Header Chain Integrity:** Verifies that the `parent_root` of each header correctly links to the hash of the previous header, from **Bₛ** all the way back to **b₀**.
2.  **Validator Membership:** For every public key in the private inputs, verifies its Merkle proof against the validator set root **Vroot₀**. This proves the signers are part of the legitimate committee.
3.  **Committee Shuffling Correctness:**
    *   Re-computes the epoch seed: `seed = seed(floor(s / SLOTS_PER_EPOCH))`. This requires access to the RANDAO mix from a header within that epoch.
    *   For each validator, it simulates the `compute_shuffled_index` permutation to confirm they were assigned to the committee for slot **s**.
    *   Checks if the bitfield provided is in alignment with the set
4.  **BLS Signature Verification:** Verifies the aggregated signature `σ` against the header of **Bₛ** using the public keys of the participating signers. The circuit asserts that the total stake of the signers is **> 2/3** of the total committee stake.
5.  **Data Validity:** If a data proof was provided, the circuit verifies its validity against the state root or transaction root of **Bₛ**.

---

## 3. Epoch Transition Proof

When the client needs to verify a block beyond epoch `E₀ + 1`, the underlying validator set (and thus the sync committee) may have changed. A special epoch transition proof is required to update the trust anchor.

Let **b₁** be the last block header of epoch `E₀ + 1`.

### ZK Circuit Inputs

The inputs are largely the same as the per-block proof for **b₁**, with one addition:

*   **Public Input:** Full validator registry data from the state of **b₁**.
*   **Private Input:** Merkle proofs for the validator data against `b₁.state_root`.

### ZK Circuit Checks

The circuit performs all the checks from the standard block proof for **b₁**, plus:

1.  **Process Epoch Updates:** It simulates the `process_epoch` function from the Ethereum specification. This includes:
    *   Processing validator activations, exits, and penalties.
    *   Updating effective balances.
2.  **Compute New Validator Root:** Based on the updated validator set, it computes the **new sync committee root**, `Vroot₁`.

### Atomic Update

Upon successful verification of this proof, the client **atomically updates its trust anchor** from `(b₀, Vroot₀)` to `(b₁, Vroot₁)`. The verification process can then continue from this new anchor for the next two epochs. This proof is only generated when necessary to avoid its computational overhead.
