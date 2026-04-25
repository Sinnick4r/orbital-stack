# ADR-012: Orbit regime classifier - rule adjustments from empirical discovery

## Status

Accepted - 2026-04-24

## Context

ADR-010 defined a deterministic orbit regime classifier for v0.5.0
using the following rules, expressed in prose:

```
mean_motion > 11.25                             -> LEO
2.0 < mean_motion <= 11.25  and ecc < 0.25      -> MEO
0.99 <= mean_motion <= 1.01 and ecc < 0.01      -> GEO
eccentricity >= 0.25                            -> HEO
else                                            -> unknown
```

These thresholds were defensible on theory but were never tested
against real Celestrak data. Running the classifier against the full
active GP catalog on 2026-04-24 produced:

- LEO: 94.7%
- GEO: 3.8%
- MEO: 0.7%
- HEO: 0.3%
- **unknown: 0.6%** (87 objects)

The `unknown` bucket is small but not random. Inspection of its
contents revealed three systematic misclassifications:

1. **Medium Earth Orbit navigation satellites** (GPS, GLONASS,
   Galileo, Beidou-2 M). Mean motion around 1.70-1.95 revolutions per
   day. Fall below the `mean_motion > 2.0` MEO threshold, so they
   land in `unknown` despite being textbook MEO.

2. **Inclined Geosynchronous Orbit satellites** (Beidou-2 IGSO-4/5,
   Milstar-2, DSP satellites). Mean motion around 1.00-1.01, correct
   for geosynchronous, but eccentricity around 0.01-0.02 - slightly
   above the `ecc < 0.01` GEO threshold. These are orbits that
   complete one revolution per sidereal day but with non-zero
   inclination or eccentricity, producing a figure-eight ground
   track rather than a fixed point.

3. **Aged GEO satellites** (LES-5, Syracuse 3B, old DSP series).
   Mean motion drifted slightly outside 0.99-1.01 due to station-
   keeping fuel exhaustion or deliberate end-of-life drift into
   graveyard orbits. Some of these are genuinely in transition, but
   others are still near-geosynchronous and the rule was too tight.

The unknown bucket at 0.6% is not catastrophic - the classifier still
labels 99.4% of the catalog correctly. But the systematic miss on GPS
and Galileo is particularly bad for the project's thesis: navigation
constellations are exactly the kind of high-salience objects the
dashboard will want to display accurately.

## Decision

The rules are revised for v0.5.0 shipment as follows:

```
mean_motion > 11.25                             -> LEO
1.5 < mean_motion <= 11.25  and ecc < 0.25      -> MEO   (was > 2.0)
0.97 <= mean_motion <= 1.03 and ecc < 0.05      -> GEO   (was 0.99-1.01, ecc < 0.01)
eccentricity >= 0.25                            -> HEO
else                                            -> unknown
```

### Specific threshold changes

- **MEO lower bound: 2.0 -> 1.5 revolutions per day.** Captures GPS
  (~2.0), GLONASS (~2.13), Galileo (~1.70), and Beidou MEO (~1.86)
  without extending so low that it overlaps the GEO band. A
  geostationary object has mean motion exactly 1.0027; the new MEO
  floor at 1.5 leaves a ~50% margin to GEO.

- **GEO mean motion: 0.99-1.01 -> 0.97-1.03.** Captures aged GEO
  satellites that drifted slightly off-station and IGSO satellites
  whose mean motion is essentially identical to GEO's by definition.
  The range 0.97-1.03 corresponds to orbital periods of roughly
  23.3 to 24.7 hours - still recognizably geosynchronous.

- **GEO eccentricity: < 0.01 -> < 0.05.** Accommodates IGSO orbits,
  which are geosynchronous but slightly eccentric. Does not relax
  enough to capture Molniya (eccentricity > 0.6) or other genuine
  HEOs.

### What the rules still do not capture

The revised rules still label as `unknown`:

- Orbits between MEO and GEO that are neither (very rare, mostly
  transfer orbits mid-maneuver or uncontrolled tumbling).
- Orbits between GEO and highly elliptical, i.e. geosynchronous
  transfer orbits near apogee kickoff.

A `unknown` rate of ~0.1% after this change is acceptable and
expected. The point of the classifier is not perfect coverage; it is
to label the 99%+ of objects whose classification is unambiguous, and
to mark the rest explicitly as uncertain so downstream analysis does
not pretend to know.

### Sun-synchronous is not a new regime in v0.5.0

Sun-synchronous orbits are a subset of LEO distinguished by their
inclination (~98 degrees). Distinguishing them requires inclination
data, which both GP and SATCAT provide, but the v0.5.0 classifier
operates only on `mean_motion` and `eccentricity`. Adding sun-
synchronous detection is a post-v0.5.0 extension; the reserved
`orbit_regime_confidence` column from ADR-009 exists precisely to
signal when a classifier moved beyond deterministic rules.

### IGSO as a distinct regime: rejected for v0.5.0

Creating a fifth regime value `IGSO` to separate inclined
geosynchronous from true equatorial GEO was considered. Rejected for
v0.5.0 because:

- Adding a value to a closed literal set is additive under ADR-008
  (a new value is allowed in additive changes); it can be introduced
  post-v0.5.0 without a major bump.
- v0.5.0 is narrow on purpose per ADR-010. Adding a regime value also
  requires adding rules for when to pick it over GEO, which is
  non-obvious without inclination data.
- The distinction matters for ground-station design and some
  geopolitical compliance questions, but not for the v0.5.0
  dashboard's compliance-asymmetry thesis.

### Implementation

`src/orbital/transform/orbit_regime.py` exposes one pure function:

```python
def classify_orbit_regime(
    mean_motion: float | None,
    eccentricity: float | None,
) -> Literal["LEO", "MEO", "GEO", "HEO", "unknown"]:
    ...
```

The thresholds are module-level `Final` constants with names that
match the ADR (`_MEO_LOWER_MEAN_MOTION = 1.5`,
`_GEO_MEAN_MOTION_RANGE = (0.97, 1.03)`, etc.). Anyone reading the
module sees the numbers that correspond to this decision.

A unit test covers each threshold's boundary: an object at exactly
the threshold and an object one `1e-4` away, on both sides. A
property test with `hypothesis` asserts that the output is always
one of the five Literal values for any float input in the declared
domain.

A regression test loads a small hand-picked sample of known-class
satellites (ISS for LEO, a GPS satellite for MEO, a Hot Bird for
GEO, a Molniya-orbit satellite for HEO) and asserts the classifier
labels each correctly. The sample lives at
`tests/fixtures/orbit_regime_samples.yaml` and is small enough
(approximately 15 entries) to curate by hand.

## Consequences

**Positive**

- GPS, GLONASS, Galileo, and Beidou MEO all classify correctly as
  MEO. These are high-salience, frequently-queried objects and
  getting them wrong would be immediately noticed.
- Inclined geosynchronous satellites classify as GEO, which is
  semantically defensible: from a "what kind of orbit is this"
  perspective, IGSO is a GEO variant.
- The `unknown` bucket shrinks to genuinely ambiguous cases, which is
  a better signal than a large bucket of misclassifications.

**Negative**

- GEO is a broader category under the new rules. Consumers who need
  to distinguish true equatorial GEO from IGSO cannot do so from
  `orbit_regime_canonical` alone. They must fall back to inclination,
  which is in the raw Celestrak data but not in the canonical at
  v0.5.0.
- The thresholds now include one unusual value (`1.5` instead of
  `2.0`). The lookup remains readable but is slightly less
  memorable than round numbers. Documented in the module docstring
  and this ADR; acceptable tradeoff.

**Neutral**

- The revised rules do not break compatibility with ADR-010 in a
  schema sense - `orbit_regime_canonical` still accepts the same
  literal set. ADR-010's prose specification of the rules is
  superseded by this ADR but its scope decisions remain valid.
- If empirical evidence post-v0.5.0 shows the 1.5 MEO floor creates
  a different misclassification cluster (for example, highly
  eccentric LEO objects that happen to have low mean motion at
  apogee - shouldn't happen for conventional TLEs but possible for
  unusual orbits), a further adjustment would be a non-breaking
  rule change.

## References

- ADR-008 - Canonical schema evolution: governs any future addition
  of regime values (e.g., IGSO, sun-synchronous) as additive
  changes.
- ADR-009 - Canonical schema shape: defines
  `orbit_regime_canonical` as a closed literal set. This ADR does
  not change the set; it only changes the rules that populate it.
- ADR-010 - v0.5.0 scope: defines the original classifier rules.
  This ADR supersedes the rule specification in ADR-010 but leaves
  the scope decisions intact.
- ADR-011 - Celestrak two-endpoint split: unrelated, but both
  ADRs originate from the same `notebooks/exploration/
  celestrak_discovery.ipynb` findings.
- `notebooks/exploration/celestrak_discovery.ipynb` - empirical
  observation of the 87 misclassified objects that motivated this
  change.
