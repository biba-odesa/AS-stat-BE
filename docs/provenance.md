# Origin and licensing review

Upstream: https://github.com/manuelkasper/AS-Stats, by Manuel Kasper for Monzoon Networks AG. Its LICENSE.txt is BSD-2-Clause, copyright 2008–2014 Manuel Kasper. If actual upstream source were distributed, its copyright, conditions and disclaimer would need to be retained for source and reproduced in binary distribution materials.

The owner identifies the previous Python implementation as an adaptation of the AS-Stats idea. The ASN/link accounting idea and knownlinks format originate there. AS-stat-BE evolved through that local adaptation, but this does not establish copied C++ source. The owner specifically reports that upstream code was not transferred.

The review read the upstream license and original Perl daemon's credits, examined the previous Python collector/aggregator/supervisor and compared decoder/accounting structure with C++. Implementations differ. A supporting comparison of trimmed lines >=60 characters found no identical lines between C++ sources/headers and either predecessor; this check alone cannot prove absence of all derivative expression. No specific copied upstream code was identified. Protocol field numbers, common binary parsing patterns and accounting ideas are not evidence of literal code transfer.

No upstream/Python source, web assets or dependency source is bundled, and no original notice is attached to new code as if a transfer had been found. If future review identifies actually borrowed material, preserve its applicable notices. libcurl is an external system dependency; VictoriaMetrics is separately installed under its own license.

The owner selected BSD-2-Clause for the new code, copyright 2026 biba-odesa; see LICENSE. The project has no official affiliation or endorsement by the original author.
