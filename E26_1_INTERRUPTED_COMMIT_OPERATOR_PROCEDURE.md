# E26.1 Interrupted Commit Operator Procedure
1. Stop all Executor writers.
2. Preserve byte-identical copies of the main ledger, temp ledger, lock artifact, and directory metadata where available.
3. Calculate SHA-256 for every artifact.
4. Never delete or promote `.tmp` blindly.
5. Validate main and temp independently with the read-only verifier.
6. Do not modify ATS Position State.
7. Do not synthesize lifecycle.
8. Record the selected recovery authority and all supporting evidence.
9. Any recovery modification requires a separate authorized offline phase.

E26.1 performs no automatic repair.
