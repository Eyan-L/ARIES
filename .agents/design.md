# Design principles

Obtain agreement before changing an invariant; proceed with aligned work.

- ARIES serves trusted researchers in controlled environments. Favor experimental
  correctness, reproducibility, and simplicity. Add public-service, multi-tenant,
  or consumer-product safeguards only when requested. Trust operators/configuration,
  not agent commands; preserve credential confidentiality, verifier isolation,
  and ownership.
- Preserve measurement meaning: unsupported is not zero. Confirmed teardown must
  not fail observation of remaining runtimes; retain genuine measurement failures.
- Runner has four replaceable roles: Benchmark, AgentHarness, ToolSandbox, and
  ToolBridge. Model infrastructure adds no role. Follow [ownership](architecture.md)
  and [component contracts](components.md); logical components need not be services.
- Use supported native behavior. Extra wrappers, state, retries, or supervision
  need an inspected native path and a concrete unmet requirement for an actual
  consumer. Preserve operation semantics, cancellation, errors, and ownership.
- Prefer deletion and direct calls over speculative abstractions. Avoid registries
  and generic orchestration frameworks. When an authorized contract change removes
  a guarantee, delete the wrappers, state, checks, and tests enforcing only it.
- Withhold verifiers until harness stop and bridge revocation are confirmed.
  Evaluate independently of harness success using the benchmark's original
  methodology; record harness and evaluation outcomes separately.
- Cleanup reverses ownership, including partial starts and cancellation, and
  confirms owned-runtime absence. Borrowed-command cancellation closes transport
  without guaranteeing command termination.

Rationale and current gaps: [human design guide](../docs/design.md).
